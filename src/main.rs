use axum::routing::post;
use axum::{extract::State, Json, Router};
use clap::{Parser, Subcommand};
use mini_db::cfg::watch_config;
use mini_db::init_tracing;
use mini_db::sql::execution::ResultSet;
use mini_db::types::Value;
use mini_db::Database;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Serialize)]
struct SqlResponse {
    success: bool,
    labels: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    error: Option<String>,
}

#[derive(Parser)]
#[command(name = "mini-db")]
#[command(about = "A mini SQL database with Bitcask storage engine")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP server (default if no subcommand is given)
    Server,
    /// Execute a single SQL statement and print the result
    Exec {
        /// SQL statement to execute
        sql: String,
    },
    /// Start an interactive SQL REPL
    Cli,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command.unwrap_or(Commands::Server) {
        Commands::Server => {
            if let Err(e) = run_server().await {
                eprintln!("Server error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Exec { sql } => {
            if let Err(e) = run_exec(&sql).await {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Cli => {
            if let Err(e) = run_cli().await {
                eprintln!("CLI error: {e}");
                std::process::exit(1);
            }
        }
    }
}

async fn run_server() -> mini_db::db_error::Result<()> {
    init_tracing();
    watch_config(broadcast::channel(10).1).await;

    let engine = mini_db::init_db()?;
    let db = Arc::new(Database::new(engine));

    let addr = SocketAddr::from(([127, 0, 0, 1], 6666));
    let listener = TcpListener::bind(addr).await?;

    let app = Router::new()
        .route("/", post(execute_sql))
        .with_state(db);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_exec(sql: &str) -> mini_db::db_error::Result<()> {
    let engine = mini_db::init_db()?;
    let db = Database::new(engine);
    let result = db.execute(sql).await?;
    println!("{}", format_result(&result));
    Ok(())
}

async fn run_cli() -> mini_db::db_error::Result<()> {
    let engine = mini_db::init_db()?;
    let db = Database::new(engine);
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    println!("mini-db interactive SQL shell");
    println!("Type 'exit' or 'quit' to leave.\n");

    loop {
        write!(stdout, "mini-db> ")?;
        stdout.flush()?;

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                // EOF
                println!();
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Input error: {e}");
                continue;
            }
        }

        let input = line.trim();
        if input.is_empty() {
            continue;
        }
        if input.eq_ignore_ascii_case("exit")
            || input.eq_ignore_ascii_case("quit")
            || input.eq_ignore_ascii_case(".q")
        {
            break;
        }

        match db.execute(input).await {
            Ok(result) => println!("{}", format_result(&result)),
            Err(e) => eprintln!("Error: {e}"),
        }
    }

    println!("Bye!");
    Ok(())
}

fn format_result(result: &ResultSet) -> String {
    if result.rows.is_empty() && result.labels.is_empty() {
        return "OK".to_string();
    }

    let headers: Vec<String> = result.labels.iter().map(|l| l.as_header()).collect();
    let num_cols = headers.len();
    if num_cols == 0 {
        return format!("({} rows)", result.rows.len());
    }

    // Convert all cells to strings and compute column widths
    let mut rows_str: Vec<Vec<String>> = Vec::with_capacity(result.rows.len());
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();

    for row in &result.rows {
        let cells: Vec<String> = row.iter().map(|v| value_to_string(v)).collect();
        for (i, cell) in cells.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        rows_str.push(cells);
    }

    // Helper to build a separator line like +---+---+---+
    let sep = || {
        let parts: Vec<String> = widths
            .iter()
            .map(|w| "-".repeat(w + 2))
            .collect();
        format!("+{}+\n", parts.join("+"))
    };

    // Helper to build a data row
    let fmt_row = |cells: &[String]| {
        let parts: Vec<String> = cells
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let w = widths.get(i).copied().unwrap_or(0);
                format!(" {:>width$} ", c, width = w)
            })
            .collect();
        format!("|{}|\n", parts.join("|"))
    };

    let mut out = String::new();
    out.push_str(&sep());
    out.push_str(&fmt_row(&headers));
    out.push_str(&sep());
    for row in &rows_str {
        out.push_str(&fmt_row(row));
    }
    out.push_str(&sep());
    out.push_str(&format!("({} rows)\n", result.rows.len()));
    out
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
    }
}

async fn execute_sql(
    State(db): State<Arc<Database>>,
    Json(req): Json<SqlRequest>,
) -> Json<SqlResponse> {
    match db.execute(&req.sql).await {
        Ok(result_set) => {
            let labels: Vec<String> = result_set.labels.iter().map(|l| l.as_header()).collect();
            let rows: Vec<Vec<serde_json::Value>> = result_set
                .rows
                .iter()
                .map(|row| row.iter().map(|v| value_to_json(v)).collect())
                .collect();
            Json(SqlResponse {
                success: true,
                labels,
                rows,
                error: None,
            })
        }
        Err(e) => Json(SqlResponse {
            success: false,
            labels: vec![],
            rows: vec![],
            error: Some(e.to_string()),
        }),
    }
}

fn value_to_json(v: &mini_db::types::Value) -> serde_json::Value {
    use mini_db::types::Value;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => {
            if let Some(n) = serde_json::Number::from_f64(*f) {
                serde_json::Value::Number(n)
            } else {
                serde_json::Value::Null
            }
        }
        Value::String(s) => serde_json::Value::String(s.clone()),
    }
}
