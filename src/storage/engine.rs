use crate::db_error::Result;
/// Engine trait
/// 定义存储引擎的通用行为
pub trait Engine: Send {
    // 为特定键值Key,设置一个值Value,替代原本已有的值
    fn set(&mut self, key: &[u8], value:&[u8]) -> Result<()>;

    // 为特定键值Key,获取一个值Value+
    fn get(&self, key: &[u8]) -> Result<Option<String>>;

    // 删除一个键值Key
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    // 把缓冲区的数据存储到磁盘上
    fn flush(&mut self) -> Result<()>;

    // 检查键是否存在
    fn exists(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    // 批量设置键值对
    fn batch_set(&mut self, pairs: Vec<(&[u8], &[u8])>) -> Result<()> {
        for (key, value) in pairs {
            self.set(key, &value)?;
        }
        Ok(())
    }

    // 批量获取键值对
    fn batch_get(&mut self, keys: Vec<&[u8]>) -> Result<Vec<Option<String>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key)?);
        }
        Ok(results)
    }

    // 扫描指定范围的键值对
    // 由于Btree本身是基于key排序的，所以需要指定开始和结束的键值范围
    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<Vec<(Vec<u8>, String)>>;

    // 清空所有数据
    fn clear(&mut self) -> Result<()>;

    fn status(&mut self) -> Result<EngineStatus>;
}

/// ScanIterator是一个用于遍历存储引擎中键值对的迭代器接口。
/// 它的设计强调了灵活性和错误处理能力，适合在需要双向遍历和处理潜在错误的场景中使用。
/// 通过继承DoubleEndedIterator，它为存储引擎的实现提供了一个强大的工具，用于高效地扫描和处理数据。
pub trait ScanIter: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

/// 为继承DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>的所有类型实现ScanIter特征
impl<I: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>>> ScanIter for I {}

/// 定义引擎状态
/// Engine Status
#[derive(Debug, Clone)]
pub struct EngineStatus {
    /// 引擎名称
    pub name: String,
    /// 所有键值的逻辑大小，就是所有键值的长度之和
    pub logical_size: u64,
    /// 所有的键值数量
    pub total_count: u64,
    /// 所有数据的磁盘/内存总占用空间
    pub total_size: u64,
    /// 存活数据的磁盘/内存存储空间
    pub live_size: u64,
    /// 垃圾数据占用的磁盘/内存空间
    pub garbage_size: u64,
}

impl EngineStatus {
    // 计算垃圾数据占用的磁盘空间百分比
    pub fn garbage_rate(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        self.garbage_size as f64 / self.total_size as f64 * 100.0
    }
}
