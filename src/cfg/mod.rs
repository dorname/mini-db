mod config;
mod watcher;

pub use config::Config;
pub use watcher::watch_config;

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref CONFIG: Mutex<Config> = Mutex::new(
        load_config().unwrap_or_else(|e| {
            panic!("Failed to load config and fallback also failed: {}", e)
        })
    );
}

/// 测试专用：覆盖全局配置
#[cfg(test)]
pub fn override_config_for_test(config: Config) {
    let mut guard = CONFIG.lock().unwrap();
    *guard = config;
}

/// 测试辅助：构造一个带指定存储路径的默认配置
#[cfg(test)]
pub fn test_config_with_path(path: std::path::PathBuf) -> Config {
    use crate::cfg::config::SyncStrategy;
    Config {
        storage_path: path,
        single_file_limit: 1,
        sync_strategy: SyncStrategy::Never,
        fsync_inteval_ms: 1000,
        compaction_threshold: 0.6,
        file_cache_capacity: 32,
    }
}

pub fn load_config() -> crate::db_error::Result<Config> {
    Config::load_config()
}

pub fn get_db_base() -> String {
    let config = CONFIG.lock().unwrap();
    let mut path = config.storage_path.to_str().unwrap().to_string();
    if !path.ends_with('/') {
        path.push('/');
    }
    path
}

pub fn get_max_size() -> u64 {
    let config = CONFIG.lock().unwrap();
    config.single_file_limit * 1024 * 1024
}
