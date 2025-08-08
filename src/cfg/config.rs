use std::env;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::db_error::Result;

pub fn get_config_path() -> PathBuf {
    let exe_path = env::current_exe().unwrap();
    let exe_dir = exe_path.parent().unwrap();
    let project_root = exe_dir.parent().unwrap().parent().unwrap();
    project_root.join("/project/rust_base_learning/mini-db/config.toml")
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ConfigWrapper {
    pub config: Config,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Config {
    // 存储路径
    pub storage_path: PathBuf,

    // 单文件存储上限 单位：GiB
    pub single_file_limit: u64,

    //同步策略
    pub sync_strategy: SyncStrategy,

    //同步间隔
    pub fsync_inteval_ms: u64,

    // 当无效键占用文件比例比较高时 自动压缩阈值
    pub compaction_threshold: f64,

    //LRU 旧文件句柄的缓存容量
    pub file_cache_capacity: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize)]
pub enum SyncStrategy {
    // 每次写入数据立马同步到磁盘
    Always,
    /// 由后台线程按 fsync_inteval_ms 间隔去同步
    Every,
    // 仅依赖OS 缓冲磁盘
    #[default]
    Never,
}

pub struct ConfigBuilder {
    pub inner: Config,
}

#[allow(dead_code)]
impl ConfigBuilder {
    fn storage_path(mut self, path: PathBuf) -> Self {
        self.inner.storage_path = path;
        self
    }
    fn single_file_limit(mut self, limit: u64) -> Self {
        self.inner.single_file_limit = limit;
        self
    }
    fn sync_strategy(mut self, strategy: SyncStrategy) -> Self {
        self.inner.sync_strategy = strategy;
        self
    }
    fn fsync_inteval_ms(mut self, ms: u64) -> Self {
        self.inner.fsync_inteval_ms = ms;
        self
    }
    fn file_cache_capacity(mut self, cached_size: usize) -> Self {
        self.inner.file_cache_capacity = cached_size;
        self
    }
    fn compaction_threshold(mut self, compact: f64) -> Self {
        self.inner.compaction_threshold = compact;
        self
    }

    fn valiate(&self) -> Result<()> {
        // todo!("配置模块属性验证在这里添加");
        Ok(())
    }

    pub fn build(self) -> Result<Config> {
        self.valiate()?;
        Ok(self.inner)
    }
}

impl Config {
    pub fn builder<P: Into<PathBuf>>(storage_path: P) -> ConfigBuilder {
        ConfigBuilder {
            inner: Config {
                storage_path: storage_path.into(),
                ..Default::default()
            },
        }
    }

    pub fn load_config() -> Result<Config> {
        let path = get_config_path();
        // 1、读取配置文件
        let content = std::fs::read_to_string(path)?;
        // 2、解析配置文件
        let wrapper: ConfigWrapper = toml::from_str(&content)?;
        // 3、返回实际的配置
        Ok(wrapper.config)
    }
}

#[cfg(test)]
mod test {
    use crate::cfg::config::{Config, SyncStrategy};
    use crate::db_error::Result;
    use std::path::PathBuf;

    /// 单元测试：
    /// 测试配置模块的构建方法
    #[test]
    fn build_test() -> Result<()> {
        let config = Config::builder("./db")
            .compaction_threshold(0.6)
            .file_cache_capacity(32)
            .build()?;
        println!("{:?}", config);
        assert_eq!(config.storage_path, PathBuf::from("./db"));
        assert_eq!(config.sync_strategy, SyncStrategy::Never);
        assert_eq!(config.file_cache_capacity, 32);
        assert_eq!(config.compaction_threshold, 0.6);
        Ok(())
    }

    /// 单元测试：
    /// 测试配置模块的加载方法
    #[test]
    fn load_test() -> Result<()> {
        let config = Config::load_config()?;
        println!("{:?}", config);
        Ok(())
    }
}
