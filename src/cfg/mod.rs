mod config;
mod watcher;

use crate::db_error::Result;
use std::path::Path;
use std::path::PathBuf;
pub use config::{Config, ConfigWrapper};

pub fn load_config()->Result<Config>{
    let path = Path::new("./src/config.toml");
    // 1、读取配置文件
    let content = std::fs::read_to_string(path)?;
    // 2、解析配置文件
    let wrapper:ConfigWrapper = toml::from_str(&content)?;
    // 3、返回实际的配置
    Ok(wrapper.config)
 }
// TODO 需要增加一个机制
// pub fn watch_config(config:&mut Config)->Result<()>{
//     watcher::watch_config(config)?;
//     Ok(())
// }