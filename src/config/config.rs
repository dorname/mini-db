use std::path::PathBuf;

use serde::de;

use crate::{db_error::Result, storage};

#[derive(Debug,Default)]
pub struct Config{

    // 存储路径
    pub storage_path: PathBuf,

    // 单文件存储上限 单位：GiB
    pub single_file_limit: u64,

    //同步策略
    pub sync_strategy: SyncStrategy,

    //同步间隔
    pub fsync_inteval_ms:u64,

    // 当无效键占用文件比例比较高时 自动压缩阈值
    pub compaction_threshold:f64,

    //LRU 旧文件句柄的缓存容量
    pub file_cache_capacity:usize
}

#[derive(Debug,Clone,Default)]
pub enum  SyncStrategy {
    // 每次写入数据立马同步到磁盘
    Always,
    /// 由后台线程按 fsync_inteval_ms 间隔去同步
    Every,
    // 仅依赖OS 缓冲磁盘
    #[default]
    Never,
}

pub struct ConfigBuilder {
    pub inner:Config
}

impl ConfigBuilder {
    fn storage_path(mut self,path:PathBuf)->Self{
        self.inner.storage_path = path;
        self
    }
    fn single_file_limit(mut self,limit:u64)->Self{
        self.inner.single_file_limit = limit;
        self
    }
    fn sync_strategy(mut self,strategy:SyncStrategy)->Self{
        self.inner.sync_strategy = strategy;
        self
    }
    fn fsync_inteval_ms(mut self,ms:u64)->Self{
        self.inner.fsync_inteval_ms = ms;
        self
    }
    fn file_cache_capacity(mut self,cached_size:usize)->Self{
        self.inner.file_cache_capacity = cached_size;
        self
    }
    fn compaction_threshold(mut self,compact:f64)->Self{
        self.inner.compaction_threshold = compact;
        self
    }

    fn valiate(&self)->Result<()>{
        // todo!("配置模块属性验证在这里添加");
        Ok(())
    }

    fn build(self)-> Result<Config>{
        self.valiate()?;
        Ok(self.inner)
    }
}

impl Config {
    fn builder<P: Into<PathBuf>>(storage_path:P)->ConfigBuilder{
        ConfigBuilder{
            inner:Config{
                storage_path:storage_path.into(),
                ..Default::default()
            }
        }
    }
}

#[cfg(test)]
mod test{
    use crate::config::{self, config::Config};
    use crate::db_error::Result;
    #[test]
    fn build_test()->Result<()>{
        let config = Config::builder("./db")
        .compaction_threshold(0.6)
        .file_cache_capacity(32)
        .build()?;
        println!("{:?}",config);
        Ok(())
    }
}