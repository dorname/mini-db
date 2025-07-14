use crate::cfg::{load_config, watcher, Config};
use notify::{Event, RecursiveMode, Result, Watcher};
use std::{path::{self, Path},sync::Arc};
use tokio::{sync::{mpsc,broadcast},task};

/// 监听配置文件变化，更新全局的配置实例
pub async fn watch_config(config: Arc<Config>,
mut shutdown: broadcast::Receiver<()>) -> crate::db_error::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Result<Event>>();
    let path = Path::new("src/config.toml");
    // 1、在阻塞线程里运行文件系统 watcher
    task::spawn_blocking(move || -> crate::db_error::Result<()>{
        let mut watcher = notify::recommended_watcher(move |res|{
            // 将事件推送到Tokio通道；忽略send 失败(接收端关闭)
            let _=tx.send(res);
        })?;
        watcher.watch(path, RecursiveMode::NonRecursive)?;

        // 阻塞驻留： 直到进程推出或通道关闭
        std::thread::park();
        Ok(())
    });
    // 主异步循环：消费事件 + 更新配置
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => {
                break;
            }
            opt = rx.recv() => {
                match opt {
                    Some(Ok(ev)) if ev.kind.is_modify() => {
                        println!("配置文件发生变化: {:?}", ev);
                        // 重新加载配置
                        match load_config() {
                            Ok(new_config) => {
                                // 更新全局配置
                                println!("配置已更新");
                            }
                            Err(e) => {
                                eprintln!("重新加载配置失败: {}", e);
                            }
                        }
                    }
                    Some(Ok(ev)) => {
                        println!("文件事件: {:?}", ev);
                    }
                    Some(Err(e)) => {
                        eprintln!("监听错误: {:?}", e);
                    }
                    None => {
                        // 通道关闭，退出循环
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}