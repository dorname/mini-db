mod config;
mod watcher;

pub use config::Config;
pub use watcher::watch_config;

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref CONFIG: Mutex<Config> = Mutex::new(load_config().unwrap());
}

pub fn load_config() -> crate::db_error::Result<Config> {
    Config::load_config()
}

pub fn get_db_base() -> String {
    let config = CONFIG.lock().unwrap();
    config.storage_path.to_str().unwrap().to_string()
}

pub fn get_max_size() -> u64 {
    let config = CONFIG.lock().unwrap();
    config.single_file_limit * 1024 * 1024
}
