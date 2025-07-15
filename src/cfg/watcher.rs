use crate::cfg::{load_config};
use notify::{Event, RecursiveMode, Result, Watcher};
use std::path::Path;
use tokio::{sync::{mpsc,broadcast},task};
use tracing::{info, error};
use super::config::get_config_path;

/// 监听配置文件变化，更新全局的配置实例
pub async fn watch_config(mut shutdown: broadcast::Receiver<()>) {
    // 启动后台异步任务，不阻塞主线程
    tokio::spawn(async move {
        let (tx, mut rx) = mpsc::unbounded_channel::<Result<Event>>();
        let path = get_config_path();

        // 在阻塞线程中运行 watcher
        let _watcher_handle = task::spawn_blocking({
            let tx = tx.clone();
            move || -> crate::db_error::Result<()> {
                let mut watcher = notify::recommended_watcher(move |res| {
                    let _ = tx.send(res);
                })?;
                watcher.watch(&path, RecursiveMode::NonRecursive)?;

                // 阻塞保持线程运行
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(60));
                }
            }
        });

        // 主监听循环
        loop {
            tokio::select! {
                _ = shutdown.recv() => {}
                opt = rx.recv() => {
                    match opt {
                        Some(Ok(ev)) if ev.kind.is_modify() => {
                            info!("配置文件发生变化: {:?}", ev);
                            match load_config() {
                                Ok(new_config) => {
                                    info!("配置已更新");
                                    let mut config = super::CONFIG.lock().unwrap();
                                    config.single_file_limit = new_config.single_file_limit;
                                    config.sync_strategy = new_config.sync_strategy;
                                    config.fsync_inteval_ms = new_config.fsync_inteval_ms;
                                    config.compaction_threshold = new_config.compaction_threshold;
                                    config.file_cache_capacity = new_config.file_cache_capacity;
                                }
                                Err(e) => error!("重新加载配置失败: {}", e),
                            }
                        }
                        Some(Ok(ev)) => info!("文件事件: {:?}", ev),
                        Some(Err(e)) => error!("监听错误: {:?}", e),
                        None => break,
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_tracing;
    #[tokio::test]
    async fn test_watch_config(){
        init_tracing();
        watch_config(broadcast::channel(10).1).await;
    }
}
