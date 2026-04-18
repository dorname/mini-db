pub mod cfg;
pub mod db_error;
pub mod storage;
pub mod utils;
pub use storage::BitCask;

pub mod sql;
pub mod types;

use crate::db_error::Result;
use crate::sql::execution::{execute, ResultSet};
use crate::sql::parser::Parser;
use crate::sql::planner::planner::plan;
use crate::storage::mvcc::MVCC;
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();
}

pub fn init_db() -> db_error::Result<BitCask> {
    BitCask::init_db()
}

/// 数据库会话，封装 MVCC 引擎与 SQL 执行
pub struct Database {
    mvcc: Arc<Mutex<MVCC<BitCask>>>,
}

impl Database {
    pub fn new(engine: BitCask) -> Self {
        Self {
            mvcc: Arc::new(Mutex::new(MVCC::new(engine))),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<ResultSet> {
        let statement = Parser::pasre(sql)?;
        let mvcc = self.mvcc.lock().await;
        let plan = plan(&mvcc, &statement)?;
        let result = execute(&mvcc, &plan)?;
        Ok(result)
    }
}
