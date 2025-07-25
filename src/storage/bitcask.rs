use crate::cfg::{get_db_base, get_max_size};
use crate::db_error::Result;
use crate::storage::engine::{Engine, EngineStatus};

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
}

impl BitCask {
    /// 1、扫描数据库所有的存储文件
    /// 2、构建全局KeyDir——索引
    /// 3、打开活跃的存储文件
    pub fn init_db() -> Result<Self> {
        let db_base = get_db_base();
        let path = Path::new(db_base.as_str());
        let mut log_file_id = create_tsid().number().to_string() + "_active";
        let mut db = Self {
            log: None,
            keydir: KeyDir::new(),
        };
        if path.is_dir() {
            //1 、遍历目录下的所有文件收集所有文件路径
            let mut paths = read_dir(path)
                .unwrap()
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
        db.log = Some(Log::new(log_file_id).unwrap());
        Ok(db)
    }
    /// 构建索引
    fn build_key_dir(&mut self, file_path: &PathBuf) -> Result<String> {
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
        let db_base = get_db_base();
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
        let new_log = Log::new(new_file_id).unwrap();
        self.log = Some(new_log);
    }

    /// 判断活跃文件是否超过了限制大小
    fn check_size_limit(file: &std::fs::File) -> bool {
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
            None => {
                return Ok(None);
            }
            _ => {
                let active_file_id = active.as_ref().unwrap().file_id.clone();
                if file_id.eq(&active_file_id) {
                    return Ok(Some(active.as_ref().unwrap().to_owned())); // 返回 Log 的克隆
                }
                let log = Log::new(file_id.to_string())?; // 创建新的 Log 实例
                return Ok(Some(log)); // 返回新的 Log 实例
            }
        };
    }

    /// compact方法
    /// 压缩活跃日志文件：
    /// 1、创建新的活跃日志文件，将所有活跃的键写入新的日志文件
    /// 2、删除旧的活跃日志文件
    fn compact(&mut self) -> crate::db_error::Result<()> {
        let db_base = get_db_base();
        // 3、根据旧文件名删除旧的活跃日志文件
        let old_file_id = self.log.as_ref().unwrap().file_id.clone();
        // 1、创建新的活跃日志文件
        let new_log = Log::new(create_tsid().number().to_string() + "_active")?;
        self.log = Some(new_log);
        fn write(log: &mut Log, key: Vec<u8>, value: &str) -> Result<u64> {
            let tstamp = crate::utils::get_timestamp_to_vec();
            let mut log_entry = LogEntry::new(tstamp, key.clone(), value.as_bytes().to_vec());
            log_entry.build_crc(); // 构建crc校验字段
            log.write_entry(log_entry)
        }
        // 2、将所有活跃的键写入新的日志文件
        let keydir = self.keydir.clone();
        for (key, _) in keydir.iter() {
            let value = self.get(key)?;
            write(
                self.log.as_mut().unwrap(),
                key.clone(),
                &value.unwrap().as_str(),
            )?;
        }
        self.flush()?;
        fs::remove_file(Path::new(&(db_base.clone() + old_file_id.as_str())))?;
        Ok(())
    }
}

impl Engine for BitCask {
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
                let need_compact = log.file_id == current_file_id
                    && BitCask::check_size_limit(&log.file.lock().unwrap());
                info!("需要压缩:{:?}", need_compact);
                if need_compact {
                    self.compact()?;
                }
            }
            {
                let log = self.log.as_ref().unwrap();
                let need_refresh = log.file_id == current_file_id
                    && BitCask::check_size_limit(&log.file.lock().unwrap());
                info!("需要写入到新的活跃文件:{:?}", need_refresh);
                if need_refresh {
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
    fn get(&self, key: &[u8]) -> Result<Option<String>> {
        // 1、根据key,从keydir中读相关的存储信息
        // KeyDir：key ——— (fileId、crc_pos）
        if let Some((_, crc_pos)) = self.keydir.get(key) {
            let log = self.get_log_by_key(key.to_vec())?;
            match log {
                Some(mut log) => {
                    println!("读取文件位置:{:?}", *crc_pos);
                    let entry = log.read_entry(*crc_pos)?;
                    match entry {
                        Some(e) => {
                            return Ok(Some(String::from_utf8_lossy(&e.value).to_string()));
                        }
                        None => {
                            return Ok(None);
                        }
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        } else {
            return Ok(None);
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
            log.file.lock().unwrap().flush()?;
        }
        Ok(())
    }
    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<Vec<(Vec<u8>, String)>> {
        Ok(self
            .keydir
            .range(range)
            .map(|(key, _)| {
                let value = self.get(key).unwrap();
                (key.clone(), value.unwrap())
            })
            .collect::<Vec<(Vec<u8>, String)>>())
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
        let db_base = get_db_base();
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
            logical_size: logical_size as u64,
            total_count: total_count as u64,
            total_size: total_disk_size as u64,
            live_size: live_disk_size as u64,
            garbage_size: garbage_disk_size as u64,
        })
    }
}

/// 迭代器结构体
/// ScanIterator
pub struct ScanIterator<'a> {
    /// 迭代器
    inner: std::collections::btree_map::Iter<'a, Vec<u8>, ValTuple>,
    /// 所属日志
    log: &'a mut Log,
}
impl<'a> ScanIterator<'a> {
    fn map(&mut self, item: (&Vec<u8>, &ValTuple)) -> <Self as Iterator>::Item {
        let (key, value) = item;
        Ok((
            key.clone(),
            self.log.read_entry(value.1).unwrap().unwrap().value,
        ))
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
    file: Arc<Mutex<std::fs::File>>, // 使用 Arc 和 Mutex 包装 File
    #[allow(dead_code)]
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
    value_sz: i32,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[allow(dead_code)]
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
        let value_sz = match value.len() {
            0 => -1,
            _ => value.len() as i32,
        };
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
        let value_sz = i32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let key = &bytes[20..(20 + ksz) as usize];
        let value = match value_sz {
            x if x > 0 => &bytes[(20 + ksz) as usize..(20 + ksz + value_sz as u32) as usize],
            _ => &[0u8; 0],
        };
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
        let db_base = get_db_base();
        let path = PathBuf::from(db_base.clone() + file_id.as_str());
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
            file: Arc::new(Mutex::new(file)),
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
        let total_size: usize = log_entry.get_entry().len();
        let pos;
        // 2、设置文件插入指针
        let mut file = self.file.lock().unwrap();
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
        Ok(pos as u64)
    }

    /// value位置、value大小、crc位置读取值
    fn read_entry(&mut self, crc_pos: u32) -> Result<Option<LogEntry>> {
        // 1、计算ksz
        let mut ksz_v = [0u8; 4];
        let ksz_pos = (crc_pos + 12) as u64;
        let mut file = self.file.lock().unwrap();

        if let Err(e) = file.seek(SeekFrom::Start(ksz_pos)) {
            eprintln!("定位文件指针时出错: {}", e);
            return Err(e.into());
        }

        if let Err(e) = file.read_exact(&mut ksz_v) {
            eprintln!("读取 ksz 时出错: {}", e);
            return Err(e.into());
        }

        let ksz = u32::from_be_bytes(ksz_v);
        // 2、计算value_sz
        let mut value_sz_v = [0u8; 4];
        let value_pos = (crc_pos + 16) as u64;
        if let Err(e) = file.seek(SeekFrom::Start(value_pos)) {
            eprintln!("定位文件指针时出错: {}", e);
            return Err(e.into());
        }

        if let Err(e) = file.read_exact(&mut value_sz_v) {
            eprintln!("读取 value_sz 时出错: {}", e);
            return Err(e.into());
        }
        let value_sz = u32::from_be_bytes(value_sz_v);
        // 3、计算条目总长度，并构建结构体
        let entry_len: usize = (20u32 + ksz + value_sz) as usize;

        let mut entry = vec![0u8; entry_len as usize];
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
    use crate::{cfg::watch_config, init_tracing};
    use sha3::{Digest, Sha3_256};
    use std::{fs::read_dir, time::Duration};
    use tokio::{sync::broadcast, time};
    use tracing::info;

    #[test]
    fn sha3_test() {
        // 1) 创建一个 Sha3_256 hasher
        let mut h = Sha3_256::new();
        h.update("abc".as_bytes());
        let result = h.finalize();
        assert_eq!(result.to_vec().len(), 32);
        assert_eq!(
            hex::encode(result.to_vec()),
            "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532"
        );
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
        println!("{:?}", log.crc);
        println!("{:?}", log.get_entry());
    }

    #[test]
    fn test_wr_log() {
        // 创建一个活跃文件
        let file_id = create_tsid().number().to_string() + "_active";
        let db_base = get_db_base();
        let temp_path = db_base.clone() + file_id.as_str();
        println!("{:?}", temp_path);
        let mut log_db = Log::new(file_id).unwrap();
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "test_1".as_bytes().to_vec();
        let value = "test-3333".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        let _ = log_db.write_entry(log);
        // 读取数据
        let log_entry = log_db.read_entry(0u32).unwrap().unwrap();
        println!("{:?}", log_entry);
        assert_eq!("test-3333", String::from_utf8_lossy(&log_entry.value));
        // 删除测试文件
        std::fs::remove_file(temp_path).unwrap();
    }

    #[test]
    #[ignore]
    fn test_read_log() {
        let file_id = "696392295149515530_active".to_string();
        let log_db = Log::new(file_id).unwrap();
        let pos = 23 as u64;
        let mut buf = vec![0u8; 5];
        let mut file = log_db.file.lock().unwrap();
        file.seek(SeekFrom::Start(pos)).unwrap();
        file.read_exact(&mut buf).unwrap();
        println!("{:?}", String::from_utf8_lossy(&buf));
    }

    #[test]
    #[ignore]
    fn test_read_entry() {
        let file_id = "675727592794104102_active".to_string();
        let mut log_db = Log::new(file_id).unwrap();
        // println!("{:?}", log_db.read_entry(0u32));
        let log_entry = log_db.read_entry(57u32).unwrap().unwrap();
        println!("{:?}", log_entry);
        println!("{:?}", String::from_utf8_lossy(&log_entry.value));
    }

    #[test]
    #[ignore]
    fn test_write_entry() {
        let file_id = "675727592794104102_active".to_string();
        let mut log_db = Log::new(file_id).unwrap();
        let tstamp = crate::utils::get_timestamp_to_vec();
        let key = "key_1".as_bytes().to_vec();
        let value = "value_1".as_bytes().to_vec();
        let mut log = LogEntry::new(tstamp, key, value);
        log.build_crc();
        let _ = log_db.write_entry(log);
    }

    #[test]
    #[ignore]
    fn test_files_iter() {
        let db_base = get_db_base();
        let path = Path::new(db_base.as_str());
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
    #[ignore]
    fn test_init_db() {
        let db = BitCask::init_db().unwrap();
        // 当key值相同时会出现索引覆盖
        println!("{:?}", db);
    }

    #[test]
    fn test_get() {
        // 清理测试数据
        let mut db = BitCask::init_db().unwrap();

        // 写入测试数据
        let key = "key_4".as_bytes().to_vec();
        let value = "test".as_bytes().to_vec();
        // println!("开始写入数据");
        let _ = db.set(&key, &value);
        println!(
            "写入文件位置指针:{:?}",
            db.get_log_by_key(key.clone())
                .unwrap()
                .unwrap()
                .file
                .lock()
                .unwrap()
                .stream_position()
        );

        // 读取测试数据
        println!("开始读取数据");
        match db.get(&key) {
            Ok(Some(val)) => {
                println!("读取成功: {}", val);
                assert_eq!(val, String::from_utf8_lossy(&value));
            }
            Ok(None) => {
                panic!("未找到键: {:?}", String::from_utf8_lossy(&key));
            }
            Err(e) => {
                panic!("读取数据时出错: {}", e);
            }
        }
    }

    #[test]
    #[ignore]
    fn generate_id_test() {
        println!(
            "{:?}",
            create_tsid().number().to_string().parse::<u64>().unwrap()
        );
        println!("{:?}", create_tsid().number().to_string());
    }

    #[test]
    fn test_set() {
        let mut db = BitCask::init_db().unwrap();
        // db.set(10u32.to_be_bytes().to_vec(), "");
        let _ = db.set(&"key_1".as_bytes().to_vec(), &"value_1".as_bytes().to_vec());
        assert_eq!(
            db.get(&"key_1".as_bytes().to_vec()).unwrap().unwrap(),
            "value_1"
        );
    }

    #[test]
    #[ignore]
    fn test_refresh_active() {
        let mut db = BitCask::init_db().unwrap();
        db.refresh_active();
    }

    #[test]
    #[ignore]
    fn test_status() {
        let mut db = BitCask::init_db().unwrap();
        // db.set(&10u32.to_be_bytes().to_vec(), &"value_5".as_bytes().to_vec());
        // db.delete(&10u32.to_be_bytes().to_vec());
        // db.flush().unwrap();
        println!("{:?}", db.status().unwrap());
    }

    use crate::cfg::CONFIG;
    #[tokio::test]
    async fn test_init_db_async() {
        init_tracing();
        let db = BitCask::init_db().unwrap();
        watch_config(broadcast::channel(10).1).await;
        info!("{:?}", db);
        loop {
            info!("{:?}", CONFIG.lock().unwrap());
            time::sleep(Duration::from_secs(1)).await;
        }
    }

    #[test]
    #[ignore]
    fn test_compact() {
        let mut db = BitCask::init_db().unwrap();
        println!("{:?}", db.status().unwrap());
        // println!("{:?}", db.get("key_4".as_bytes().to_vec()).unwrap());
        db.compact().unwrap();
        println!("{:?}", db.status().unwrap());
    }
}
