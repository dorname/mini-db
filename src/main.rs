use axum::routing::post;
use axum::{extract::State, Json, Router};
use mini_db::cfg::watch_config;
use mini_db::init_tracing;
use mini_db::Database;
use serde::{Deserialize, Serialize};
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

#[tokio::main]
async fn main() -> mini_db::db_error::Result<()> {
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
                .map(|row| {
                    row.iter()
                        .map(|v| value_to_json(v))
                        .collect()
                })
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
