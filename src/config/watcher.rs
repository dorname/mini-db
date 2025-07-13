// use notify::{Event, RecursiveMode, Result, Watcher};
// use std::{path::Path, sync::mpsc};
// use crate::db_error::Result;
#[cfg(test)]
mod tests{

    use notify::{Event, RecursiveMode, Result, Watcher, EventKind};
    use notify::event::{AccessKind, AccessMode};
    use std::{path::Path, sync::mpsc};

    #[test]
    fn test_notify()->Result<()>{
        let (tx, rx) = mpsc::channel::<Result<Event>>();

        // Use recommended_watcher() to automatically select the best implementation
        // for your platform. The `EventHandler` passed to this constructor can be a
        // closure, a `std::sync::mpsc::Sender`, a `crossbeam_channel::Sender`, or
        // another type the trait is implemented for.
        let mut watcher = notify::recommended_watcher(tx)?;
    
        // 方法1：直接监听config.toml文件
        let target = Path::new("src/config.toml");
        watcher.watch(target, RecursiveMode::NonRecursive)?;
        
        // 方法2：或者监听父目录并递归监听（如果需要监听多个配置文件）
        // watcher.watch(target.parent().unwrap(), RecursiveMode::Recursive)?;
    
        // 存储文件的前一个内容
        let mut previous_content = String::new();
        
        // 4. 阻塞等待并处理事件
        for res in rx {
            match res {
                Ok(event) => {
                    println!("event: {:?}", event);
                    
                    // 检查是否是写操作
                    if let EventKind::Access(AccessKind::Close(AccessMode::Write)) = event.kind {
                        // 读取文件当前内容
                        match std::fs::read_to_string(target) {
                            Ok(current_content) => {
                                if current_content != previous_content {
                                    println!("=== 文件内容发生变化 ===");
                                    if !previous_content.is_empty() {
                                        println!("=== 修改前的内容 ===");
                                        println!("{}", previous_content);
                                        println!("=== 修改后的内容 ===");
                                        println!("{}", current_content);
                                        
                                        // 简单的差异显示
                                        let lines_before: Vec<&str> = previous_content.lines().collect();
                                        let lines_after: Vec<&str> = current_content.lines().collect();
                                        
                                        println!("=== 内容差异 ===");
                                        for (i, (before, after)) in lines_before.iter().zip(lines_after.iter()).enumerate() {
                                            if before != after {
                                                println!("第{}行: '{}' -> '{}'", i + 1, before, after);
                                            }
                                        }
                                        
                                        // 处理新增或删除的行
                                        if lines_after.len() > lines_before.len() {
                                            for i in lines_before.len()..lines_after.len() {
                                                println!("新增第{}行: '{}'", i + 1, lines_after[i]);
                                            }
                                        } else if lines_before.len() > lines_after.len() {
                                            for i in lines_after.len()..lines_before.len() {
                                                println!("删除第{}行: '{}'", i + 1, lines_before[i]);
                                            }
                                        }
                                    } else {
                                        println!("=== 文件内容（首次读取）===");
                                        println!("{}", current_content);
                                    }
                                    previous_content = current_content;
                                }
                            }
                            Err(e) => eprintln!("读取文件失败: {e:?}"),
                        }
                    }
                }
                Err(e)    => eprintln!("watch error: {e:?}"),
            }
        }
        Ok(())
    }
}