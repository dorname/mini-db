pub mod db_error;
pub mod storage;
pub mod utils;
pub mod cfg;

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();
}