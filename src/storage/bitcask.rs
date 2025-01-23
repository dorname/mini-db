use sha3::{Digest, Sha3_256};
use std::path::PathBuf;
/// 实现一个BitCask结构
/// struct - BitCask
/// 成员：
/// 日志文件集合 - Log
/// 全局的映射表 - KeyDir
pub struct BitCask {
    logs: Vec<Log>,
    keydir: KeyDir,
}
/// KeyDir
/// 维护key和（fileId、value_sz、value_pos、tstamp）的映射关系
type KeyDir = std::collections::BTreeMap<Vec<u8>, (snowflake::ProcessUniqueId, u64, u64, Vec<u8>)>;

/// 实现一个日志文件结构体
/// file_id 文件的索引
/// file_path 文件路径
/// file 文件实体
struct Log {
    file_id: snowflake::ProcessUniqueId,
    file_path: PathBuf,
    file: std::fs::File,
}

/// 实现一个日志文件条目结构体
/// crc 完整性验证字段 256位的hash编码 => Vec<u8> 长度 32
/// tstamp 时间戳 32位的时间戳 => Vec<u8> 长度 4
/// ksz key的长度 根据键值定
/// value_sz value的长度 根据value值定
/// key 键 Vec<u8>
/// value 值 Vec<u8>
/// 拼接方式：
/// ------|------|------|---------|------|------|
///  crc  |tstamp|ksz   |value_sz |key   |value |
/// ------|------|------|---------|------|------|
#[derive(Debug)]
struct LogEntry {
    crc: Vec<u8>,
    tstamp: Vec<u8>,
    ksz: u64,
    value_sz: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl LogEntry {
    /// 初始化日志条目
    /// ```
    /// let tstamp = crate::utils::get_timestamp_to_vec();
    /// let key = "key".as_bytes().to_vec();
    /// let value = "value".as_bytes().to_vec();
    /// let log = LogEntry::new(tstamp, key, value);
    /// ```
    fn new(tstamp: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> Self {
        let ksz = key.len() as u64;
        let value_sz = value.len() as u64;
        Self {
            crc: vec![],
            tstamp: tstamp,
            ksz: ksz,
            value_sz: value_sz,
            key: key,
            value: value,
        }
    }

    /// 构建完整性校验字段
    /// ```
    /// let tstamp = crate::utils::get_timestamp_to_vec();
    /// let key = "key".as_bytes().to_vec();
    /// let value = "value".as_bytes().to_vec();
    /// let log = LogEntry::new(tstamp, key, value);
    /// log.build_src();
    /// ```
    fn build_crc(&mut self) {
        let crc_from = [self.tstamp.clone(), self.key.clone(), self.value.clone()].concat();
        let mut hasher = Sha3_256::new();
        hasher.update(crc_from);
        self.crc = hasher.finalize().to_vec();
    }

    fn get_data(&self) -> Vec<u8> {
        [
            self.crc.clone(),
            self.tstamp.clone(),
            self.ksz.to_be_bytes().to_vec(),
            self.value_sz.to_be_bytes().to_vec(),
            self.key.clone(),
            self.value.clone(),
        ]
        .concat()
        .to_vec()
    }

    fn get_value_pos(&self) -> u64 {
        (self.crc.len() + self.tstamp.len() + 2) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha3::{Digest, Sha3_256};

    #[test]
    fn sha3_test() {
        // 1) 创建一个 Sha3_256 hasher
        let mut h = Sha3_256::new();
        h.update(b"abc");
        let result = h.finalize();
        println!("{:?}", result.to_vec().len());
        println!("{:?}", hex::encode(result.to_vec()));
    }

    #[test]
    fn test_log_entry() {
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        println!("{:?}", log);
    }
}
