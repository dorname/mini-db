use crate::cfg::{get_db_base, get_max_size};
use crate::db_error::Result;
use crate::storage::engine::{Engine, EngineStatus};
use std::collections::btree_map::Range;

use fs4::fs_std::FileExt;
use sha3::{Digest, Sha3_256};
use std::fs::{self, read_dir};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::vec;
use tracing::info;
use tsid::create_tsid;

/// 实现一个BitCask结构
/// struct - BitCask
/// 成员：
/// 日志文件集合 - Log
/// 全局的映射表 - KeyDir
#[derive(Debug)]
pub struct BitCask {
    log: Option<Log>,
    keydir: KeyDir,
    db_base: String,
}

impl BitCask {
    /// 1、扫描数据库所有的存储文件
    /// 2、构建全局KeyDir——索引
    /// 3、打开活跃的存储文件
    pub fn init_db() -> Result<Self> {
        Self::init_db_with_base(get_db_base())
    }

    fn init_db_with_base(db_base: String) -> Result<Self> {
        let path = Path::new(db_base.as_str());
        let mut log_file_id = create_tsid().number().to_string() + "_active";
        let mut db = Self {
            log: None,
            keydir: KeyDir::new(),
            db_base: db_base.clone(),
        };
        if path.is_dir() {
            //1 、遍历目录下的所有文件收集所有文件路径
            let mut paths = read_dir(path)?
                .into_iter()
                .map(|e| e.unwrap().path())
                .collect::<Vec<_>>();
            //2、根据时间大小排序 创建时间越晚文件名的数值越大
            //活跃文件的文件名永远是最大的
            if !paths.is_empty() {
                paths.sort_by(|file_a, file_b| {
                    let mut file_a_name = file_a.file_name().and_then(|n| n.to_str()).unwrap();
                    file_a_name = file_a_name.strip_suffix("_active").unwrap_or(file_a_name);
                    let mut file_b_name = file_b.file_name().and_then(|n| n.to_str()).unwrap();
                    file_b_name = file_b_name.strip_suffix("_active").unwrap_or(file_b_name);
                    file_a_name
                        .to_string()
                        .parse::<u64>()
                        .unwrap()
                        .cmp(&file_b_name.to_string().parse::<u64>().unwrap())
                });
                // 3、遍历文件集合，构建索引
                paths.iter().for_each(|file_path| {
                    if file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap()
                        .ends_with("active")
                    {
                        log_file_id = file_path
                            .file_name()
                            .and_then(|n| n.to_str())
                            .unwrap()
                            .to_string();
                    }
                    // 由于for_each闭包不能使用?操作符，这里改用for循环
                    if let Err(e) = db.build_key_dir(file_path) {
                        // 可以根据需要选择panic、返回Err或打印错误，这里选择panic
                        panic!("构建KeyDir失败: {:?}", e);
                    }
                });
            }
        }
        db.log = Some(Log::new_with_base(log_file_id, db_base)?);
        Ok(db)
    }

    pub fn init_db_at(path: &Path) -> Result<Self> {
        let mut base = path.to_string_lossy().to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        Self::init_db_with_base(base)
    }
    /// 构建索引
    fn build_key_dir(&mut self, file_path: &PathBuf) -> Result<String> {
        let file_id = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap()
            .to_string();
        let mut file = fs::OpenOptions::new()
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
        while pos < file_len {
            let result =
                || -> std::result::Result<(Vec<u8>, String, Option<u32>), std::io::Error> {
                    let crc_pos = pos as u32;
                    let ksz_pos = (crc_pos + 12) as u64;
                    reader.seek(SeekFrom::Start(ksz_pos))?;
                    reader.read_exact(&mut len_buf)?;
                    let ksz = u32::from_be_bytes(len_buf);
                    let key_pos = 20 + crc_pos as u64;
                    let mut key = vec![0u8; ksz as usize];
                    reader.seek(SeekFrom::Start(key_pos))?;
                    reader.read_exact(&mut key)?;
                    let value_sz_pos = (crc_pos + 16) as u64;
                    reader.seek(SeekFrom::Start(value_sz_pos))?;
                    reader.read_exact(&mut len_buf)?;
                    let value_sz = i32::from_be_bytes(len_buf);
                    if value_sz > 0 {
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
                Ok((key, _, None)) => {
                    self.keydir.remove(&key);
                }
                Err(_) => {
                    println!("ERROR");
                }
            }
        }
        Ok("构建完成".to_string())
    }

    /// 更新存储文件
    fn refresh_active(&mut self) {
        let db_base = self.db_base.clone();
        //1、将当前活跃文件设置为非活跃文件
        let file_id = self.log.as_mut().unwrap().file_id.clone();
        fs::rename(
            Path::new(&(db_base.clone() + file_id.as_str())),
            Path::new(
                &(db_base.clone()
                    + file_id
                    .as_str()
                    .strip_suffix("_active")
                    .unwrap_or(file_id.as_str())),
            ),
        )
            .expect("重命名失败");
        //2、创建新的活跃文件
        let new_file_id = create_tsid().number().to_string() + "_active";
        let new_log = Log::new_with_base(new_file_id, self.db_base.clone()).unwrap();
        self.log = Some(new_log);
    }

    /// 判断活跃文件是否超过了限制大小
    fn check_size_limit(file: &fs::File) -> bool {
        file.metadata().unwrap().len() >= get_max_size()
    }

    // 判断Key是在活跃文件还是在非活跃文件
    // 如果为活跃文件直接通过self.log 去读取数据
    // 如果不是，则需要初始化一个old非活跃文件实体old_log 去读取数据
    fn get_log_by_key(&self, key: Vec<u8>) -> Result<Option<Log>> {
        let file_id = self
            .keydir
            .get(&key)
            .map(|(file_id, _)| file_id.clone())
            .unwrap_or("".to_owned());
        if file_id.eq("") {
            return Ok(None);
        }
        let active = &self.log;
        match active {
            None => Ok(None),
            _ => {
                let active_file_id = active.as_ref().unwrap().file_id.clone();
                if file_id.eq(&active_file_id) {
                    return Ok(Some(active.as_ref().unwrap().to_owned())); // 返回 Log 的克隆
                }
                let log = Log::new_with_base(file_id.to_string(), self.db_base.clone())?; // 创建新的 Log 实例
                Ok(Some(log)) // 返回新的 Log 实例
            }
        }
    }

    /// compact方法
    /// 压缩活跃日志文件：
    /// 1、创建新的活跃日志文件，将所有活跃的键写入新的日志文件
    /// 2、删除旧的活跃日志文件
    fn compact(&mut self) -> Result<()> {
        let db_base = self.db_base.clone();
        // 3、根据旧文件名删除旧的活跃日志文件
        let old_file_id = self.log.as_ref().unwrap().file_id.clone();
        // 1、创建新的活跃日志文件
        let new_log = Log::new_with_base(create_tsid().number().to_string() + "_active", self.db_base.clone())?;
        self.log = Some(new_log);
        fn write(log: &mut Log, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
            let tstamp = crate::utils::get_timestamp_to_vec();
            let mut log_entry = LogEntry::new(tstamp, key, value);
            log_entry.build_crc(); // 构建crc校验字段
            log.write_entry(log_entry)
        }
        // 2、将所有活跃的键写入新的日志文件
        let keydir = self.keydir.clone();
        for (key, _) in keydir.iter() {
            let value = self.get(key)?;
            let crc_pos = write(self.log.as_mut().unwrap(), key.clone(), value.unwrap())?;
            self.keydir.insert(
                key.clone(),
                (self.log.as_ref().unwrap().file_id.clone(), crc_pos as u32),
            );
        }
        self.flush()?;
        fs::remove_file(Path::new(&(db_base.clone() + old_file_id.as_str())))?;
        Ok(())
    }
}
impl Drop for BitCask {
    fn drop(&mut self) {
        self.flush().expect("缓冲数据无法刷入磁盘");
        ()
    }
}

impl Engine for BitCask {
    type ScanIter<'a> = ScanIterator<'a>;
    /// 写入条目数据
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // 0、获取当前key所在的文件
        let mut belong_log = self.get_log_by_key(key.to_vec())?;
        let current_file_id = self.log.clone().unwrap().file_id;
        fn write(log: &mut Log, key: &[u8], value: &[u8]) -> Result<u64> {
            let tstamp = crate::utils::get_timestamp_to_vec();
            let mut log_entry = LogEntry::new(tstamp, key.to_vec(), value.to_vec());
            log_entry.build_crc(); // 构建crc校验字段
            log.write_entry(log_entry)
        }
        if let Some(log) = &mut belong_log {
            // 键值已经存在,写入数据
            let crc_pos = write(log, key, value)?;
            info!("写入文件位置:{:?}", crc_pos);
            // 4、更新索引
            self.keydir
                .insert(key.to_vec(), (log.file_id.clone(), crc_pos as u32));
        } else {
            {
                let log = self.log.as_ref().unwrap();
                let file = log.file.lock()?;
                let need_compact =
                    log.file_id == current_file_id && BitCask::check_size_limit(&file);
                info!("需要压缩:{:?}", need_compact);
                if need_compact {
                    drop(file);
                    self.compact()?;
                }
            }
            {
                let log = self.log.as_ref().unwrap();
                let file = log.file.lock()?;
                let need_refresh =
                    log.file_id == current_file_id && BitCask::check_size_limit(&file);
                info!("需要写入到新的活跃文件:{:?}", need_refresh);
                if need_refresh {
                    drop(file);
                    self.refresh_active();
                }
            }

            let log = self.log.as_mut().unwrap();
            let crc_pos = write(log, key, value)?;
            info!("写入文件位置:{:?}", crc_pos);
            // 4、更新索引
            self.keydir
                .insert(key.to_vec(), (log.file_id.clone(), crc_pos as u32));
        }
        Ok(())
    }
    /// 读取条目数据
    /// 根据keyDir取获取
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // 1、根据key,从keydir中读相关的存储信息
        // KeyDir：key ——— (fileId、crc_pos）
        if let Some((_, crc_pos)) = self.keydir.get(key) {
            let log = self.get_log_by_key(key.to_vec())?;
            match log {
                Some(mut log) => {
                    println!("读取文件位置:{:?}", *crc_pos);
                    let entry = log.read_entry(*crc_pos)?;
                    match entry {
                        Some(e) => Ok(Some(e.value)),
                        None => Ok(None),
                    }
                }
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.set(&key, &[])?;
        self.flush()?;
        self.keydir.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        //1、将缓冲区的数据刷入磁盘，非测试环境下

        if let Some(log) = &mut self.log {
            log.file.lock()?.flush()?;
        }
        Ok(())
    }
    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIter<'_> {
        Self::ScanIter {
            inner: self.keydir.range(range),
            log: &mut self.log,
        }
    }

    fn clear(&mut self) -> Result<()> {
        //1、获取所有的key
        let keys = self.keydir.keys().cloned().collect::<Vec<_>>();
        //2、删除所有的key
        for key in keys {
            self.delete(&key)?;
        }
        Ok(())
    }

    fn status(&mut self) -> Result<EngineStatus> {
        let db_base = self.db_base.clone();
        //1、获取所有key
        let keys = self.keydir.keys().cloned().collect::<Vec<_>>();
        // 获取所有存活的key
        let live_keys = keys
            .iter()
            .filter(|&key| {
                println!("获取key:{:?}", String::from_utf8_lossy(key));
                match self.keydir.contains_key(key) {
                    true => true,
                    false => false, // 处理 Err 或 None 的情况
                }
            })
            .collect::<Vec<_>>();
        //2、计算所有键值的数量
        let total_count = self.keydir.len();
        let logical_size: u64 = self
            .keydir
            .iter()
            .map(|(key, value)| {
                let mut log = match self.get_log_by_key(key.clone()) {
                    Ok(Some(log)) => log,
                    _ => return 0, // 处理错误情况
                };
                match log.read_entry(value.1) {
                    Ok(Some(entry)) => entry.get_entry().len(),
                    _ => 0, // 处理错误情况
                }
            })
            .sum::<usize>() as u64;
        //3、计算所有数据的磁盘总占用空间
        let path = Path::new(db_base.as_str());
        let mut total_disk_size = 0;
        let mut live_disk_size = 0;
        if path.is_dir() {
            // 获取所有目录条目，并处理可能的错误
            let entries: Vec<_> = match read_dir(path) {
                Ok(dir) => dir.filter_map(|entry| entry.ok()).collect(),
                Err(e) => {
                    eprintln!("无法读取目录: {}", e);
                    vec![] // 返回空数组，继续执行
                }
            };
            // 遍历所有目录条目，计算总大小
            total_disk_size = entries
                .iter()
                .filter_map(|entry| entry.path().metadata().ok())
                .map(|metadata| metadata.len())
                .sum();
            if total_disk_size > 0 {
                //4、计算活跃数据的磁盘存储空间
                live_disk_size = live_keys
                    .iter()
                    .map(|&key| {
                        let log = self.get_log_by_key(key.clone()).unwrap();
                        match log {
                            Some(mut log) => {
                                let value = self.keydir.get(&key.clone()).unwrap();
                                log.read_entry(value.1).unwrap().unwrap().get_entry().len()
                            }
                            None => 0,
                        }
                    })
                    .sum::<usize>() as u64;
            }
        }
        let garbage_disk_size = total_disk_size - live_disk_size;

        Ok(EngineStatus {
            name: "bitcask".to_string(),
            logical_size,
            total_count: total_count as u64,
            total_size: total_disk_size,
            live_size: live_disk_size,
            garbage_size: garbage_disk_size,
        })
    }
}

/// 迭代器结构体
/// ScanIterator
pub struct ScanIterator<'a> {
    /// 迭代器
    inner: Range<'a, Vec<u8>, ValTuple>,
    /// 所属日志
    log: &'a mut Option<Log>,
}
impl<'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &ValTuple)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        let log = self.log.as_mut().unwrap();
        let val = match log.read_entry(value.1)? {
            Some(entry) => entry.value,
            None => vec![],
        };
        Ok((key.clone(), val))
    }
}
/// 实现由前向后迭代功能
impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}
/// 实现由后向前迭代功能
impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
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
#[derive(Debug, Clone)]
struct Log {
    file_id: String,
    #[allow(dead_code)]
    file_path: PathBuf,
    file: Arc<Mutex<fs::File>>, // 使用 Arc 和 Mutex 包装 File
    #[allow(dead_code)]
    current_offset: u32,
}

/// 实现一个日志文件条目结构体
/// - crc 完整性验证字段 256位的hash编码 => Vec<u8> 长度 32
/// - tstamp 时间戳 32位的时间戳 => Vec<u8> 长度 4
/// - ksz key的长度 根据键值定
/// - value_sz value的长度 根据value值定
/// - key 键 Vec<u8>
/// - value 值 Vec<u8>
/// 拼接方式：
/// ```text
/// ------|------|------|---------|------|------|
///  crc  |tstamp|ksz   |value_sz |key   |value |
/// ------|------|------|---------|------|------|
/// ```
#[derive(Debug)]
pub(super) struct LogEntry {
    crc: Vec<u8>,
    tstamp: Vec<u8>,
    ksz: u32,
    value_sz: i32,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[allow(dead_code)]
impl LogEntry {
    /// 初始化日志条目
    /// ```ignore
    /// let tstamp = mini_db::utils::get_timestamp_to_vec();
    /// let key = "key".as_bytes().to_vec();
    /// let value = "value".as_bytes().to_vec();
    /// let log = mini_db::storage::LogEntry::new(tstamp, key, value);
    /// ```
    pub fn new(tstamp: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> Self {
        let ksz = key.len() as u32;
        let value_sz = match value.len() {
            0 => -1,
            _ => value.len() as i32,
        };
        Self {
            crc: vec![],
            tstamp,
            ksz,
            value_sz,
            key,
            value,
        }
    }

    /// 构建完整性校验字段
    /// ```ignore
    /// let tstamp = mini_db::utils::get_timestamp_to_vec();
    /// let key = "key".as_bytes().to_vec();
    /// let value = "value".as_bytes().to_vec();
    /// let log = mini_db::storage::LogEntry::new(tstamp, key, value);
    /// //log.build_src();
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
        let value_sz = i32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let key = &bytes[20..(20 + ksz) as usize];
        let value = match value_sz {
            x if x > 0 => &bytes[(20 + ksz) as usize..(20 + ksz + value_sz as u32) as usize],
            _ => &[0u8; 0],
        };
        Self {
            crc: bytes[0..8].to_vec(),
            tstamp: bytes[8..12].to_vec(),
            ksz,
            value_sz,
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
        belong_log.current_offset
    }
}

impl Log {
    /// 创建一个新的日志存储文件
    /// 或者打开一个活跃存储文件
    fn new(file_id: String) -> Result<Self> {
        Self::new_with_base(file_id, get_db_base())
    }

    fn new_with_base(file_id: String, db_base: String) -> Result<Self> {
        let path = PathBuf::from(db_base + file_id.as_str());
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.try_lock_exclusive()?;
        Ok(Self {
            file_path: path,
            file: Arc::new(Mutex::new(file)),
            file_id,
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
        let total_size: usize = log_entry.get_entry().len();
        let pos;
        // 2、设置文件插入指针
        let mut file = self.file.lock()?;
        pos = file.seek(SeekFrom::End(0))?;
        // 3、创建一个文件的缓冲区
        {
            let mut writer = BufWriter::with_capacity(total_size, &mut *file);
            // 4、写数据入缓冲区
            writer.write_all(&log_entry.get_entry())?;
            // 5、刷新缓冲区
            writer.flush()?;
        } // 这里结束了对 writer 的借用
        // 6、回到文件中数据的起始位置
        file.rewind()?;

        // 7、返回文件中数据的起始位置
        Ok(pos)
    }

    /// value位置、value大小、crc位置读取值
    fn read_entry(&mut self, crc_pos: u32) -> Result<Option<LogEntry>> {
        let mut len_buf = [0u8; 4];
        // 1、计算ksz
        let ksz_pos = (crc_pos + 12) as u64;
        let mut file = self.file.lock()?;

        if let Err(e) = file.seek(SeekFrom::Start(ksz_pos)) {
            eprintln!("定位文件指针时出错: {}", e);
            return Err(e.into());
        }

        if let Err(e) = file.read_exact(&mut len_buf) {
            eprintln!("读取 ksz 时出错: {}", e);
            return Err(e.into());
        }

        let ksz = u32::from_be_bytes(len_buf);
        // 2、计算value_sz
        let value_sz_pos = (crc_pos + 16) as u64;
        file.seek(SeekFrom::Start(value_sz_pos))?;
        file.read_exact(&mut len_buf)?;
        let value_sz = i32::from_be_bytes(len_buf);
        if value_sz < 0 {
            return Ok(None);
        }

        // 3、计算条目总长度，并构建结构体
        let entry_len: usize = (20u32 + ksz + value_sz as u32) as usize;

        let mut entry = vec![0u8; entry_len];
        if let Err(e) = file.seek(SeekFrom::Start(crc_pos as u64)) {
            eprintln!("定位文件指针时出错: {}", e);
            return Err(e.into());
        }

        if let Err(e) = file.read_exact(&mut entry) {
            eprintln!("读取条目时出错: {}", e);
            return Err(e.into());
        }
        let log_entry = LogEntry::from_bytes(entry);
        // 4、检验完整性
        if self.check_crc(log_entry.crc.clone(), log_entry.get_entry()[8..].to_vec()) {
            return Ok(Some(log_entry));
        }
        file.rewind()?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::{override_config_for_test, test_config_with_path};
    use tempfile::TempDir;

    fn setup(temp_dir: &TempDir) {
        let config = test_config_with_path(temp_dir.path().to_path_buf());
        override_config_for_test(config);
    }

    #[test]
    fn test_log_entry() {
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        assert_eq!(log.ksz, 3);
        assert_eq!(log.value_sz, 5);
        assert_eq!(log.key, [107, 101, 121]);
        assert_eq!(log.value, [118, 97, 108, 117, 101]);
    }

    #[test]
    fn test_get_entry() {
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key".as_bytes().to_vec();
        let value = "value".as_bytes().to_vec();
        let ksz = key.len() as u32;
        let value_sz = value.len() as u32;
        assert_eq!(ksz.to_be_bytes().to_vec(), [0, 0, 0, 3]);
        assert_eq!(value_sz.to_be_bytes().to_vec(), [0, 0, 0, 5]);
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        let entry = log.get_entry();
        assert_eq!(entry.len(), 8 + 4 + 4 + 4 + 3 + 5);
    }

    #[test]
    fn test_wr_log() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let file_id = create_tsid().number().to_string() + "_active";
        let mut log_db = Log::new(file_id).unwrap();
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "test_1".as_bytes().to_vec();
        let value = "test-3333".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        let _ = log_db.write_entry(log);
        let log_entry = log_db.read_entry(0u32).unwrap().unwrap();
        assert_eq!("test-3333", String::from_utf8_lossy(&log_entry.value));
    }

    #[test]
    fn test_crud() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        let key = b"foo";
        let value = b"bar";

        // Create
        db.set(key, value).unwrap();
        assert_eq!(db.get(key).unwrap().unwrap(), value);

        // Update
        let value2 = b"baz";
        db.set(key, value2).unwrap();
        assert_eq!(db.get(key).unwrap().unwrap(), value2);

        // Delete
        db.delete(key).unwrap();
        assert!(db.get(key).unwrap().is_none());
    }

    #[test]
    fn test_multiple_keys() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        for i in 0..100u32 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);
            db.set(key.as_bytes(), value.as_bytes()).unwrap();
        }

        for i in 0..100u32 {
            let key = format!("key_{:03}", i);
            let expected = format!("value_{:03}", i);
            let actual = db.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(actual, expected.as_bytes());
        }
    }

    #[test]
    fn test_scan_range() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"a", b"1").unwrap();
        db.set(b"b", b"2").unwrap();
        db.set(b"c", b"3").unwrap();
        db.set(b"d", b"4").unwrap();

        let mut results: Vec<(String, String)> = db
            .scan(b"b".to_vec()..b"d".to_vec())
            .map(|r| {
                let (k, v) = r.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect();
        // BTreeMap 有序，范围 [b, d) 包含 b、c
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ("b".to_string(), "2".to_string()));
        assert_eq!(results[1], ("c".to_string(), "3".to_string()));

        // 反向扫描
        results = db
            .scan(b"b".to_vec()..b"d".to_vec())
            .rev()
            .map(|r| {
                let (k, v) = r.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], ("c".to_string(), "3".to_string()));
        assert_eq!(results[1], ("b".to_string(), "2".to_string()));
    }

    #[test]
    fn test_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        {
            let mut db = BitCask::init_db_at(dir.path()).unwrap();
            db.set(b"persist_key", b"persist_val").unwrap();
            db.flush().unwrap();
        }
        // reopen
        let db = BitCask::init_db_at(dir.path()).unwrap();
        assert_eq!(db.get(b"persist_key").unwrap().unwrap(), b"persist_val");
    }

    #[test]
    fn test_delete_is_tombstone() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        {
            let mut db = BitCask::init_db_at(dir.path()).unwrap();
            db.set(b"del_me", b"val").unwrap();
            db.delete(b"del_me").unwrap();
        }
        let db = BitCask::init_db_at(dir.path()).unwrap();
        assert!(db.get(b"del_me").unwrap().is_none());
    }

    #[test]
    fn test_status_accuracy() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"k1", b"v1").unwrap();
        db.set(b"k2", b"v2").unwrap();
        db.set(b"k3", b"v3").unwrap();

        let status = db.status().unwrap();
        assert_eq!(status.name, "bitcask");
        assert_eq!(status.total_count, 3);
        assert!(status.total_size > 0);
        assert!(status.live_size > 0);
        assert_eq!(status.garbage_size, status.total_size - status.live_size);
    }

    #[test]
    fn test_clear() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"k1", b"v1").unwrap();
        db.set(b"k2", b"v2").unwrap();
        db.clear().unwrap();

        assert!(db.get(b"k1").unwrap().is_none());
        assert!(db.get(b"k2").unwrap().is_none());
    }

    #[test]
    fn test_active_file_rotation() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        // 写入第一个 key
        db.set(b"key1", b"value1").unwrap();
        let active_before = db.log.as_ref().unwrap().file_id.clone();

        // 手动触发 refresh_active
        db.refresh_active();
        let active_after = db.log.as_ref().unwrap().file_id.clone();

        assert_ne!(active_before, active_after);
        assert!(active_after.ends_with("_active"));

        // 旧文件应该已被重命名（去掉 _active 后缀）
        let old_file_name = active_before.strip_suffix("_active").unwrap_or(&active_before);
        let old_path = dir.path().join(old_file_name);
        assert!(old_path.exists());

        // 新 active 文件也应该存在
        let new_path = dir.path().join(&active_after);
        assert!(new_path.exists());
    }

    #[test]
    fn test_compact_updates_keydir() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"keep", b"keep_val").unwrap();
        db.set(b"remove", b"remove_val").unwrap();
        db.delete(b"remove").unwrap();

        // compact 将所有存活 key 写入新的 active 文件
        db.compact().unwrap();

        // 验证 compact 后数据仍然可读
        assert_eq!(db.get(b"keep").unwrap().unwrap(), b"keep_val");
        assert!(db.get(b"remove").unwrap().is_none());
    }

    #[test]
    fn test_overwrite_and_reopen() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        {
            let mut db = BitCask::init_db_at(dir.path()).unwrap();
            db.set(b"key", b"v1").unwrap();
            db.set(b"key", b"v2").unwrap();
            db.set(b"key", b"v3").unwrap();
        }
        let db = BitCask::init_db_at(dir.path()).unwrap();
        assert_eq!(db.get(b"key").unwrap().unwrap(), b"v3");
    }

    #[test]
    fn test_exists() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();
        db.set(b"yes", b"1").unwrap();
        assert!(db.exists(b"yes").unwrap());
        assert!(!db.exists(b"no").unwrap());
    }

    #[test]
    fn test_batch_set_and_batch_get() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.batch_set(vec![
            (b"a".as_slice(), b"1".as_slice()),
            (b"b".as_slice(), b"2".as_slice()),
            (b"c".as_slice(), b"3".as_slice()),
        ])
        .unwrap();

        let results = db.batch_get(vec![b"a", b"b", b"missing"]).unwrap();
        assert_eq!(results[0], Some(b"1".to_vec()));
        assert_eq!(results[1], Some(b"2".to_vec()));
        assert_eq!(results[2], None);
    }

    #[test]
    fn test_scan_prefix() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"prefix:a", b"1").unwrap();
        db.set(b"prefix:b", b"2").unwrap();
        db.set(b"other:c", b"3").unwrap();

        let results: Vec<_> = db
            .scan_prefix(b"prefix")
            .map(|r| {
                let (k, v) = r.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, _)| k == "prefix:a"));
        assert!(results.iter().any(|(k, _)| k == "prefix:b"));
    }

    #[test]
    fn test_delete_then_set_same_key() {
        let dir = TempDir::new().unwrap();
        setup(&dir);
        let mut db = BitCask::init_db_at(dir.path()).unwrap();

        db.set(b"k", b"v1").unwrap();
        db.delete(b"k").unwrap();
        assert!(db.get(b"k").unwrap().is_none());

        db.set(b"k", b"v2").unwrap();
        assert_eq!(db.get(b"k").unwrap().unwrap(), b"v2");
    }
}
