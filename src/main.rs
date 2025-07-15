use std::net::{SocketAddr};
use axum::routing::get;
use axum::serve::Listener;
use axum::Router;
use mini_db::cfg::{watch_config};
use mini_db::init_tracing;
use tokio::sync::broadcast;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> mini_db::db_error::Result<()> {
    // 初始化日志
    init_tracing();
    // 启动配置监听
    watch_config(broadcast::channel(10).1).await;
    // 启动数据库
    let db = mini_db::init_db()?;
    let addr = SocketAddr::from(([127,0,0,1],6666));
    let listener = TcpListener::bind(addr).await?;
    let app = Router::new().route("/", get(|| async { "Hello, World!" }));
    axum::serve(listener, app).await?;
    Ok(())
}