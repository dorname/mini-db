use fs4::fs_std::FileExt;
// use fs4::FileExt;
use crate::db_error::Result;
use sha3::{Digest, Sha3_256};
use snowflake::ProcessUniqueId;
use std::fs::{read_dir, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::vec;

const DB_BASE: &str = "/workspaces/rust_base_learning/mini-db/db/";
const MAX_SIZE: u64 = 1024 * 1024 * 1024; // 1GB 的字节数
/// 实现一个BitCask结构
/// struct - BitCask
/// 成员：
/// 日志文件集合 - Log
/// 全局的映射表 - KeyDir
#[derive(Debug)]
pub struct BitCask {
    log: Option<Log>,
    keydir: KeyDir,
}
impl BitCask {
    /// 1、扫描数据库所有的存储文件
    /// 2、构建全局KeyDir——索引
    /// 3、打开活跃的存储文件
    fn init_db() -> Result<Self> {
        let path = Path::new(DB_BASE);
        let mut log_file_id = ProcessUniqueId::new().to_string() + "_active";
        let mut db = Self {
            log: None,
            keydir: KeyDir::new(),
        };
        if (path.is_dir()) {
            //1 、遍历目录下的所有文件
            for log_entry in read_dir(path).unwrap() {
                let file_path = log_entry.unwrap().path();
                if (file_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap()
                    .ends_with("active"))
                {
                    log_file_id = file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap()
                        .to_string();
                }
                db.build_key_dir(file_path);
            }
        }
        db.log = Some(Log::new(log_file_id).unwrap());
        Ok(db)
    }
    /// 构建索引
    fn build_key_dir(&mut self, file_path: PathBuf) -> Result<String> {
        let file_id = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .to_string();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .truncate(false)
            .open(file_path)?;
        file.try_lock_exclusive()?;
        let file_len = file.metadata()?.len();
        // key、value的长度读取缓冲区
        let mut len_buf = [0u8; 4];
        let mut reader = BufReader::new(&mut file);
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        // 从头开始扫描文件
        while (pos < file_len) {
            let result =
                || -> std::result::Result<(Vec<u8>, String, Option<u32>), std::io::Error> {
                    let crc_pos = pos as u32;
                    let ksz_pos = (crc_pos + 12) as u64;
                    reader.seek(SeekFrom::Start(ksz_pos))?;
                    reader.read_exact(&mut len_buf)?;
                    let ksz = u32::from_be_bytes(len_buf);
                    let key_pos = 20 + ksz as u64;
                    let mut key = vec![0u8; ksz as usize];
                    reader.seek(SeekFrom::Start(key_pos))?;
                    reader.read_exact(&mut key)?;
                    let value_sz_pos = (crc_pos + 16) as u64;
                    reader.seek(SeekFrom::Start(value_sz_pos))?;
                    reader.read_exact(&mut len_buf)?;
                    let value_sz = i32::from_be_bytes(len_buf);
                    if (value_sz > 0) {
                        pos = pos + 20 + ksz as u64 + value_sz as u64;
                        Ok((key, file_id.clone(), Some(crc_pos)))
                    } else {
                        pos = pos + 20 + ksz as u64;
                        Ok((key, file_id.clone(), None))
                    }
                }();
            match result {
                Ok((key, file_id, Some(crc_pos))) => {
                    self.keydir.insert(key, (file_id, crc_pos));
                }
                Ok((key, file_id, None)) => {
                    self.keydir.remove(&key);
                }
                Err(_) => todo!(),
            }
        }
        Ok("构建完成".to_string())
    }

    /// 更新存储文件
    /// 1、创建一个新的日志存储文件
    /// 2、更新为活跃文件
    fn refresh_active() {}

    /// 判断活跃文件是否超过了限制大小
    fn check_size_limit(&self, file: std::fs::File) -> bool {
        file.metadata().unwrap().len() >= MAX_SIZE
    }

    /// 读取条目数据
    /// 根据keyDir取获取
    fn get(&self, key: Vec<u8>) -> Result<()> {
        // 1、根据key,从keydir中读相关的存储信息
        // KeyDir：key ——— (fileId、crc_pos）
        let (file_id, crc_pos) = self.keydir.get(&key).unwrap();
        Ok(())
    }
}
/// KeyDir
/// 维护key和（fileId、crc_pos）的映射关系
/// 因为基于当前设计crc、tstamp、ksz、value_sz均为定长数组
type KeyDir = std::collections::BTreeMap<Vec<u8>, ValTuple>;
type ValTuple = (String, u32);
/// 实现一个日志文件结构体
/// file_id 文件的索引
/// file_path 文件路径
/// file 文件实体
/// current_offset
#[derive(Debug)]
struct Log {
    file_id: String,
    file_path: PathBuf,
    file: std::fs::File,
    current_offset: u32,
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
    ksz: u32,
    value_sz: u32,
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
        let ksz = key.len() as u32;
        let value_sz = value.len() as u32;
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
        let mut hasher = Sha3_256::new();
        let input = [
            self.tstamp.clone(),
            self.ksz.to_be_bytes().to_vec(),
            self.value_sz.to_be_bytes().to_vec(),
            self.key.clone(),
            self.value.clone(),
        ]
        .concat()
        .to_vec();
        hasher.update(input);
        self.crc = hasher.finalize()[15..23].to_vec();
    }

    /// 获取条目的存储格式
    /// 数据拼接方式：
    /// ------|------|------|---------|------|------|
    ///  crc  |tstamp|ksz   |value_sz |key   |value |
    /// ------|------|------|---------|------|------|
    /// ------|------|------|---------|------|------|
    ///  8    | 4    | 4    | 4       | ...  |...   |
    /// ------|------|------|---------|------|------|
    fn get_entry_str(&self) -> String {
        let parts = [
            self.crc.clone(),
            self.tstamp.clone(),
            self.ksz.to_be_bytes().to_vec(),
            self.value_sz.to_be_bytes().to_vec(),
            self.key.clone(),
            self.value.clone(),
        ]
        .concat()
        .to_vec();
        hex::encode(parts)
    }

    fn get_entry(&self) -> Vec<u8> {
        [
            self.crc.clone(),
            self.tstamp.clone(),
            self.ksz.to_be_bytes().to_vec(),
            self.value_sz.to_be_bytes().to_vec(),
            self.key.clone(),
            self.value.clone(),
        ]
        .concat()
    }
    /// 根据数组恢复成结构体
    fn from_bytes(bytes: Vec<u8>) -> Self {
        let ksz = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        let value_sz = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let key = &bytes[20..(20 + ksz) as usize];
        let value = &bytes[(20 + ksz) as usize..(20 + ksz + value_sz) as usize];
        Self {
            crc: bytes[0..8].to_vec(),
            tstamp: bytes[8..12].to_vec(),
            ksz: ksz,
            value_sz: value_sz,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }
    /// 初始化值的位置
    fn init_value_pos(&self, belong_log: Log) -> u32 {
        let current_offset = belong_log.current_offset;
        current_offset + (self.crc.len() + self.tstamp.len() + 2) as u32
    }
    /// 初始化校验字段的位置
    fn init_crc_pos(&self, belong_log: Log) -> u32 {
        belong_log.current_offset as u32
    }
}

impl Log {
    /// 创建一个新的日志存储文件
    /// 或者打开一个活跃存储文件
    fn new(file_id: String) -> Result<Self> {
        let path = PathBuf::from(DB_BASE.to_string() + file_id.as_str());
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock_exclusive()?;
        Ok(Self {
            file_path: path,
            file: file,
            file_id: file_id,
            current_offset: 0,
        })
    }

    /// 存储数据完整性验证
    fn check_crc(&self, current_crc: Vec<u8>, check_parts: Vec<u8>) -> bool {
        let mut hasher = Sha3_256::new();
        hasher.update(check_parts);
        let crc = hasher.finalize()[15..23].to_vec();
        current_crc.eq(&crc)
    }

    /// 将条目写入文件
    fn write_entry(&mut self, log_entry: LogEntry) -> Result<u64> {
        // 1、计算存储条目的总大小
        let total_size = log_entry.get_entry().len();
        // 2、设置文件插入指针
        let pos = self.file.seek(SeekFrom::End(0))?;
        // 3、创建一个文件的缓冲区
        let mut writer = BufWriter::with_capacity(total_size, &self.file);
        // 4、写数据入缓冲区
        writer.write_all(&log_entry.get_entry())?;
        // 5、刷新缓冲区
        writer.flush()?;
        Ok(pos)
    }

    /// value位置、value大小、crc位置读取值
    fn read_entry(&mut self, crc_pos: u32) -> Result<Option<LogEntry>> {
        // 1、计算ksz
        let mut ksz_v = [0u8; 4];
        let ksz_pos = (crc_pos + 12) as usize;
        self.file.seek(SeekFrom::Start(ksz_pos as u64))?;
        self.file.read_exact(&mut ksz_v)?;
        let ksz = u32::from_be_bytes(ksz_v);
        // 2、计算value_sz
        let mut value_sz_v = [0u8; 4];
        let value_pos = (crc_pos + 16) as usize;
        self.file.seek(SeekFrom::Start(value_pos as u64))?;
        self.file.read_exact(&mut value_sz_v)?;
        let value_sz = u32::from_be_bytes(value_sz_v);
        // 3、计算条目总长度，并构建结构体
        let entry_len = 20 + ksz + value_sz;
        let mut entry = vec![0u8; entry_len as usize];
        self.file.seek(SeekFrom::Start(crc_pos as u64))?;
        self.file.read_exact(&mut entry)?;
        let log_entry = LogEntry::from_bytes(entry);
        // 4、检验完整性
        if (self.check_crc(log_entry.crc.clone(), log_entry.get_entry()[8..].to_vec())) {
            return Ok(Some(log_entry));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::read_dir;

    use super::*;
    use serde::de::value;
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

    #[test]
    fn test_get_entry() {
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();
        let ksz = key.len() as u32;
        let value_sz = value.len() as u32;
        println!(
            "key:{:?}——value:{:?}",
            ksz.to_be_bytes().to_vec(),
            value_sz.to_be_bytes().to_vec()
        );
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        println!("{:?}", log.crc);
        println!("{:?}", log.get_entry());
    }

    #[test]
    fn test_new_log() {
        let file_id = ProcessUniqueId::new().to_string() + "_active";
        let temp_path = DB_BASE.to_string() + file_id.as_str();
        println!("{:?}", temp_path);
        let mut log_db = Log::new(file_id).unwrap();
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key_1".as_bytes().to_vec();
        let value = "value_1".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        log_db.write_entry(log);
    }

    #[test]
    fn test_read_log() {
        let file_id = "puid-0-0".to_string();
        let temp_path = DB_BASE.to_string() + file_id.as_str();
        let mut log_db = Log::new(file_id).unwrap();
        let pos = 23 as u64;
        let mut buf = vec![0u8; 5];
        log_db.file.seek(SeekFrom::Start(pos)).unwrap();
        log_db.file.read_exact(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf));
    }

    #[test]
    fn test_read_entry() {
        let file_id = "puid-0-0".to_string();
        let mut log_db = Log::new(file_id).unwrap();
        // println!("{:?}", log_db.read_entry(0u32));
        println!("{:?}", log_db.read_entry(28u32));
    }

    #[test]
    fn test_write_entry() {
        let file_id = "puid-0-0".to_string();
        let mut log_db = Log::new(file_id).unwrap();
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key_1".as_bytes().to_vec();
        let value = "value_1".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        log_db.write_entry(log);
    }

    #[test]
    fn test_files_iter() {
        let path = Path::new(DB_BASE);
        // 1、遍历文件目录
        if path.is_dir() {
            for file_entry in read_dir(path).unwrap() {
                let file_path = file_entry.unwrap().path();
                if file_path.is_file() {
                    let file_name = file_path.file_name().and_then(|n| n.to_str()).unwrap();
                    println!("{:?}", file_name.ends_with("active"));
                }
            }
        }
    }

    #[test]
    fn test_init_db() {
        let db = BitCask::init_db().unwrap();
        // 当key值相同时会出现索引覆盖
        println!("{:?}", db);
    }
}
