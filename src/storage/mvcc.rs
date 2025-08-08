use crate::db_error::Result;
use crate::errdata;
use crate::storage::engine::Engine;
use crate::utils::{bin_coder, Key as KeyTrait, Value};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::MutexGuard;
/// 数据事务模块
/// 主要分为以下几个功能子模块
/// 1、事务管理
/// 2、版本管理
/// 3、并发控制
/// 4、数据一致性
/// 5、数据持久化
/// 6、数据恢复
/// 7、数据压缩
use std::{
    borrow::Cow,
    sync::{Arc, Mutex},
};

#[allow(dead_code)]
pub struct MVCC<E: Engine> {
    // 引擎 增加原子指针和互斥锁，目的是实现线程安全的引擎
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> MVCC<E> {
    pub fn new(engine: E) -> Self {
        Self {
            engine: Arc::new(Mutex::new(engine)),
        }
    }

    /// 开启一个读写事务
    pub fn begin(&self) -> Result<Transaction<E>> {
        Transaction::begin(self.engine.clone())
    }

    /// 开启最近事务版本的一个只读事务
    pub fn begin_readonly(&self) -> Result<Transaction<E>> {
        Transaction::begin_readonly(self.engine.clone(), None)
    }

    /// 开启指定版本的只读事务
    pub fn begin_readonly_version(&self, version: Version) -> Result<Transaction<E>> {
        Transaction::begin_readonly(Arc::clone(&self.engine), Some(version))
    }

    /// 事务状态恢复
    pub fn resume(&self, transaction_state: TransactionState) -> Result<Transaction<E>> {
        Transaction::resume(self.engine.clone(), transaction_state)
    }

    /// 获取无版本标记key的值
    pub fn get_unversioned(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine.lock()?.get(key)
    }

    /// 设置无版本标记的键值对
    pub fn set_unversioned(&self, key: &Vec<u8>, value: &[u8]) -> Result<()> {
        self.engine.lock()?.set(key, value)
    }
}

/// 一个事务的版本号是逻辑上的时间戳
/// 每个版本属于一个独立的读/写事务
/// 每次读/写事务开始时，需要更新版本号
pub type Version = u64;

impl Value for Version {}

/// 事务状态的枚举类
#[derive(Serialize, Deserialize, Debug)]
pub enum Key<'a> {
    /// 标记下一个可用的版本
    NextVersion,
    /// 标记所有活跃未提交事务
    Active(Version),
    /// 标记版本号对应的所有活跃事务集合的快照
    Snapshot(Version),
    /// 标记一个活跃事务（由其版本标识）写入的所有键，以便在需要回滚时使用。
    ActiveWrite(
        Version,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    /// key和其对应的版本号
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    /// 未带版本的非事务性键/值对，主要用于元数据。
    /// 这些键与带版本的键是完全独立的，
    /// 例如：未带版本的键 "foo" 与带版本的键 "foo@7" 完全无关。
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> KeyTrait<'a> for Key<'a> {}

#[derive(Debug, Deserialize, Serialize)]
pub enum KeyPrefix<'a> {
    NextVersion,
    Active,
    Snapshot,
    ActiveWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Unversioned,
}

impl<'a> KeyTrait<'a> for KeyPrefix<'a> {}

/// 事务结构体
pub struct Transaction<E: Engine> {
    // 存储引擎
    engine: Arc<Mutex<E>>,
    // 事务状态
    state: TransactionState,
}

/// 事务状态结构体
pub struct TransactionState {
    // 版本号
    version: Version,
    // 只读标记
    readonly: bool,
    // 未提交的活跃事务
    active: BTreeSet<Version>,
}

impl TransactionState {
    ///判断一个指定版本的记录（version）对当前事务是否可见。
    /// 1. 当前版本是否属于“活动事务”：
    ///   如果某条记录的版本属于 当前事务开始时就已经存在的其他活跃事务版本，那么它是不可见的。
    ///   避免读到未提交的值（即脏读）。
    /// 2. 当前是“只读事务”：
    ///   如果是只读事务，只能看到比自己事务版本更小的版本（即历史版本）。
    ///   注意：不能看到等于自己的版本（即排除刚好同时开始的事务写入的版本）。
    ///   这可以实现 快照隔离（snapshot isolation） 或 时间旅行查询（time-travel query）。
    /// 3. 普通读写事务（非只读）：
    ///   对于读写事务，允许读到小于或等于自己版本号的数据。
    ///   所以：读写事务可以“看到自己的写入”（也就是 version == self.version 是可见的）。
    fn is_visible(&self, version: Version) -> bool {
        // 判断该版本号是否是活跃事务
        if self.active.contains(&version) {
            false
        } else if self.readonly {
            version < self.version
        } else {
            version <= self.version
        }
    }
}

impl<E: Engine> Transaction<E> {
    /// 开启事务
    pub fn begin(engine: Arc<Mutex<E>>) -> Result<Transaction<E>> {
        // 获取存储引擎
        let mut session = engine.lock()?;
        // 从存储引擎获取下一个版本号
        // 如果获取失败，则初始化一个版本号为1，并将下一个版本号写入存储引擎
        // 如果获取成功，则直接将下一个版本号写入存储引擎
        let version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)?,
            None => 1u64,
        };
        let next_version = version + 1;
        // 将下一个版本号写入存储引擎
        session.set(&Key::NextVersion.encode()?, &next_version.encode()?)?;
        // 扫描所有活跃事务
        let active = Self::scan_active(&mut session)?;
        // 如果活跃事务集合不为空，则保存快照
        if !active.is_empty() {
            session.set(&Key::Snapshot(version).encode()?, &active.encode()?)?;
        }
        // 标记当前版本为活跃事务
        session.set(&Key::Active(version).encode()?, &vec![])?;
        // 删除锁
        drop(session);
        // 返回事务对象
        Ok(Self {
            engine,
            state: TransactionState {
                version,
                readonly: false,
                active,
            },
        })
    }

    ///开启只读事务
    /// 开始一个新的只读事务。如果指定了版本参数，事务将会看到该版本开始时的数据状态（会忽略该版本中的写入操作）。
    /// 换句话说，它看到的状态与该版本的读写事务开始时看到的状态相同
    pub fn begin_readonly(
        engine: Arc<Mutex<E>>,
        target: Option<Version>,
    ) -> Result<Transaction<E>> {
        // 1、开启一个只读事务
        let mut session = engine.lock()?;
        // 2、获取当前最新的版本号，但只读事务不消耗版本号
        let next_version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)?,
            None => 1u64,
        };

        // 3、只读事务使用一个观察版本号，确保它能看到所有已提交的数据
        // 但看不到未来版本的写入
        let readonly_version = if next_version > 1 {
            next_version  // 使用当前next_version作为观察点，能看到所有 < next_version 的已提交数据
        } else {
            1  // 初始情况
        };

        // 4、如果存在目标版本号，则返回该版本号的快照
        let active_snapshot = match target {
            Some(target) => {
                // 获取指定版本号的快照
                match session.get(&Key::Snapshot(target).encode()?)? {
                    Some(ref v) => BTreeSet::<Version>::decode(v)?,
                    None => return errdata!("snapshot not found"),
                }
            }
            None => {
                // 获取当前活跃事务集合
                Self::scan_active(&mut session)?
            }
        };
        // 5、删除锁
        drop(session);
        // 6、返回事务对象
        Ok(Self {
            engine,
            state: TransactionState {
                version: readonly_version,
                readonly: true,
                active: active_snapshot,
            },
        })
    }

    /// 数据写入操作（状态检测=>乐观并发事务）
    /// 这个函数在事务的特定版本号下为一个键写入新的值或标记删除。
    /// 它接收两个参数：要写入的键（key）和可选的值（value）。
    /// 当 value 为 None 时，表示删除该键（写入一个墓碑值 tombstone）。
    fn write(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        // 1、只读检测
        if self.state.readonly {
            return errdata!("readonly transaction");
        }
        // 2、获取session
        let mut session = self.engine.lock()?;
        // 3、写冲突检测
        // 构建扫描范围
        // 从当前下一个版本开始，到u64::MAX结束，扫描所有版本号
        // 小于当前版本号，则忽略
        let from = Key::Version(
            key.into(),
            self.state
                .active
                .first()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
            .encode()?;
        let to = Key::Version(key.into(), u64::MAX).encode()?;
        // 4、记录写操作并写入新版本
        if let Some((key, _)) = session.scan(from..=to).last().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_visible(version) {
                        return errdata!("write conflict");
                    }
                }
                key => return errdata!("require Key::Version got {key:?}"),
            }
        }
        // 5、
        // 表示这个 key 在当前事务版本（self.state.version）中有写入行为。
        // - 记录事务写操作：这里创建了一个特殊类型的记录 ActiveWrite(version, key)，用来跟踪当前事务（由 self.state.version 标识）修改了哪个键。
        // - 目的是支持事务回滚：如果事务需要回滚，系统需要知道该事务修改了哪些键，以便撤销这些修改。通过扫描所有 ActiveWrite(version, ...) 记录，系统可以找出所有需要删除的版本。
        // - 值为空向量 vec![]：因为只需要记录"这个键被这个事务修改过"这一事实，不需要存储实际值（实际值会存储在 Key::Version 记录中）。
        // 为什么需要这个记录：
        // 在 rollback() 方法中，系统会扫描所有 ActiveWrite(version, key) 记录，找出当前事务写入的所有键
        // 然后删除对应的 Version(key, version) 记录以及 ActiveWrite 记录本身
        // 如果没有这些记录，系统将无法知道需要回滚哪些键
        // 与活跃事务集的关系：
        // 活跃事务集(active set)是通过 Active(version) 记录来跟踪的，不是 ActiveWrite 记录
        // ActiveWrite 只记录事务内部的写操作，与其他事务的版本无关
        session.set(
            &Key::ActiveWrite(self.state.version, key.into()).encode()?,
            &vec![],
        )?;
        // 写入key
        session.set(
            &Key::Version(key.into(), self.state.version).encode()?,
            &bin_coder::encode(value)?,
        )?;
        Ok(())
    }

    /// 扫描活跃事务
    fn scan_active(session: &mut MutexGuard<E>) -> Result<BTreeSet<Version>> {
        // 初始化活跃事务集合
        let mut active = BTreeSet::new();
        // 扫描所有活跃事务
        let mut scan = session.scan_prefix(&KeyPrefix::Active.encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Active(version) => {
                    active.insert(version);
                }
                _ => return errdata!("require active key"),
            }
        }
        Ok(active)
    }

    /// 获取当前事务版本号
    pub fn get_version(&self) -> Version {
        self.state.version
    }

    /// 查询当前事务是否为只读类型
    pub fn is_readonly(&self) -> bool {
        self.state.readonly
    }

    /// 获取事务相关状态
    pub fn state(&self) -> &TransactionState {
        &self.state
    }

    /// 写入键值
    pub fn set(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        Ok(self.write(key, value)?)
    }

    /// 删除键
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        Ok(self.write(key, None)?)
    }

    /// 读取键
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut session = self.engine.lock()?;
        let from = Key::Version(key.into(), 0).encode()?;
        let to = Key::Version(key.into(), self.get_version()).encode()?;
        let mut scan = session.scan(from..=to).rev();
        while let Some((key_version, val)) = scan.next().transpose()? {
            let k = Key::decode(&key_version)?;
            let v: Option<Vec<u8>> = bin_coder::decode(&val)?;
            // println!("key=>{:?}", k);
            // println!("val{:?}=>{:?}", val, v);
            match Key::decode(&key_version)? {
                Key::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return bin_coder::decode(&val);
                    }
                }
                key => return errdata!("require Key::Version got {key:?}"),
            }
        }
        Ok(None)
    }

    /// 事务提交
    pub fn commit(self) -> Result<()> {
        //1、只读事务不用处理直接返回
        if self.state.readonly {
            return Ok(());
        }
        //2、删除当前版本下所有写事务标记键
        let mut session = self.engine.lock()?;
        let remove: Vec<_> = session
            .scan_prefix(&KeyPrefix::ActiveWrite(self.state.version).encode()?)
            .map_ok(|(k, _)| k)
            .try_collect()?;

        for key in remove {
            session.delete(&key)?
        }
        //3、删除当前版本所有的活跃事务键
        session.delete(&Key::Active(self.state.version).encode()?)
    }

    /// 事务回滚
    pub fn rollback(&self) -> Result<()> {
        //1、只读事务不需要处理
        if self.state.readonly {
            return Ok(());
        }
        //2、扫描当前版本所有具备【写事务】标记的key
        let mut session = self.engine.lock()?;
        let mut rollback = Vec::<Vec<u8>>::new();
        let mut scan = session.scan_prefix(&KeyPrefix::ActiveWrite(self.state.version).encode()?);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::ActiveWrite(_, key) => {
                    rollback.push(Key::Version(key, self.get_version()).encode()?);
                }
                key => return errdata!("require Key::ActiveWrite got {key:?}"),
            }
        }
        drop(scan);
        //3、删除当前版本所有有【写事务】标记的key
        for key in rollback {
            self.delete(&key)?;
        }
        //4、删除当前版本所有的活跃事务
        self.delete(&Key::Active(self.state.version).encode()?)
    }

    /// 恢复指定事务的状态
    fn resume(engine: Arc<Mutex<E>>, s: TransactionState) -> Result<Self> {
        // 检验合法性，如果事务不是只读事务且没有活跃事务存在则报错
        if !s.readonly
            && engine
            .lock()?
            .get(&Key::Active(s.version).encode()?)?
            .is_none()
        {
            return Err(errdata!("no active key"));
        }
        Ok(Self { engine, state: s })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BitCask;

    #[test]
    #[ignore]
    fn test_create_mvcc() -> Result<()> {
        let db = BitCask::init_db()?;
        let mvcc = MVCC::new(db);
        let txn = mvcc.begin()?;
        let key1 = "mvcc_key_1".as_bytes();
        let value1 = "mvcc_value_1".as_bytes();
        txn.set(key1, Some(value1))?;
        let result1 = txn.get(key1)?;
        println!("{:?}", result1);
        txn.commit()?;
        let read_txn = mvcc.begin_readonly()?;
        let read_key1 = read_txn.get(key1)?;
        assert_eq!(Some(value1.to_vec()), read_key1);
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_version_encode() -> Result<()> {
        let mut db = BitCask::init_db()?;
        let version = 1u64;
        let encoded = &KeyPrefix::ActiveWrite(version).encode()?;
        println!("{:?}", encoded);
        let val_encoded = &bin_coder::encode(None::<&[u8]>)?;
        println!("{:?}", val_encoded);
        db.set(encoded, &vec![])?;
        db.get(encoded)?;
        Ok(())
    }

    #[test]
    /// 事务隔离行测试：
    /// 初始情况
    /// 1、创建一个读事务version：1
    /// 2、创建一个写事务 version：1,并写入下一个版本号 version：2
    /// 3、写入 键值 mvcc_set_key<=>mvcc_set_val
    /// 4、用写事务读取 key,此时与真实值应该是相等的
    /// 5、用读事务读取 key,因为 写事务版本小于读事务版本不成立(1 < 1 =>false) 所以事务隔离了，此时key为None
    /// 6、开启一个新的读事务 version:2
    /// 7、读取key,因为 写事务版本小于读事务版本(1<2=>true),故能读取到对应的key
    fn test_mvcc_isolation() -> Result<()> {
        let db = BitCask::init_db()?;
        let mvcc = MVCC::new(db);
        let read_txn = mvcc.begin_readonly()?;
        let txn = mvcc.begin()?;
        let key = "mvcc_set_key".as_bytes();
        let value = "mvcc_set_val".as_bytes();
        txn.set(key, Some(value))?;
        assert_eq!(Some(value.to_vec()), txn.get(key)?);
        assert_ne!(Some(value.to_vec()), read_txn.get(key)?);
        // 如果不写事务提交，上述断言就会失败，但是按照执行顺序来看，上述断言应该报错才对
        txn.commit()?;
        assert_ne!(Some(value.to_vec()), read_txn.get(key)?);
        let read_txn = mvcc.begin_readonly()?;
        assert_eq!(Some(value.to_vec()), read_txn.get(key)?);
        Ok(())
    }

    #[test]
    fn test_begin_readonly() -> Result<()> {
        let db = BitCask::init_db()?;
        let mvcc = MVCC::new(db);
        let txn = mvcc.begin_readonly()?;
        let key1 = "mvcc_key_1".as_bytes();
        let result1 = txn.get(key1)?.unwrap();
        println!("{:?}", String::from_utf8_lossy(&result1));
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_bin_coder() -> Result<()> {
        let value = "mvcc_value_1".as_bytes();
        let encoded = bin_coder::encode(Some(&value))?;
        println!("{:?}", encoded);
        let decoded: Option<Vec<u8>> = bin_coder::decode(&encoded)?;
        println!("{:?}", decoded);
        Ok(())
    }

    #[test]
    fn test_double_mvcc() -> Result<()> {
        let db = BitCask::init_db()?;
        let mvcc = MVCC::new(db);
        let _ = mvcc.begin()?;
        let _ = mvcc.begin_readonly()?;
        Ok(())
    }
}
