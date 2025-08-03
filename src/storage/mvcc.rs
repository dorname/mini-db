use crate::db_error::Result;
use crate::errdata;
use crate::storage::engine::Engine;
use crate::utils::{bin_coder, Key as KeyTrait, Value};
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
use std::{borrow::Cow, sync::{Arc, Mutex}};

#[allow(dead_code)]
pub struct MVCC<E: Engine> {
    // 引擎 增加原子指针和互斥锁，目的是实现线程安全的引擎
    engine: Arc<Mutex<E>>,
}

impl<E: Engine> MVCC<E> {
    pub fn new(engine: E) -> Self {
        Self { engine: Arc::new(Mutex::new(engine)) }
    }
}

/// 一个事务的版本号是逻辑上的时间戳
/// 每个版本属于一个独立的读/写事务
/// 每次读/写事务开始时，需要更新版本号
pub type Version = u64;

impl Value for Version {}

/// 事务状态的枚举类
#[derive(Serialize, Deserialize)]
#[derive(Debug)]
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
enum KeyPrefix<'a> {
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
        let version =
            match session.get(&Key::NextVersion.encode()?)? {
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
        // 清空当前活跃事务=>开启一个新的事务
        session.set(&KeyPrefix::Active.encode()?, &vec![])?;
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
    pub fn begin_readonly(engine: Arc<Mutex<E>>, target: Option<Version>) -> Result<Transaction<E>> {
        // 1、开启一个只读事务
        let mut session = engine.lock()?;
        // 2、获取当最新事务的版本号
        let next_version = match session.get(&Key::NextVersion.encode()?)? {
            Some(ref v) => Version::decode(v)?,
            None => 1u64,
        };
        // 3、如果存在目标版本号，则返回该版本号的快照
        let active_snapshot = match target {
            Some(target) => {
                // 获取指定版本号的快照
                match session.get(&Key::Snapshot(target).encode()?)? {
                    Some(ref v) => BTreeSet::<Version>::decode(v)?,
                    None => return errdata!("snapshot not found")
                }
            }
            None => {
                // 获取当前活跃事务集合
                Self::scan_active(&mut session)?
            }
        };
        // 4、删除锁
        drop(session);
        // 5、返回事务对象
        Ok(Self {
            engine,
            state: TransactionState {
                version: next_version,
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
        let from = Key::Version(key.into(),
                                self.state.active.first()
                                    .copied()
                                    .unwrap_or(self.state.version + 1)).encode()?;
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
        // 5、写入新版本
        // 表示这个 key 在当前事务版本（self.state.version）中有写入行为。
        session.set(&Key::ActiveWrite(self.state.version, key.into()).encode()?, 
                    &vec![])?;
        // 写入key
        session.set(&Key::Version(key.into(), self.state.version).encode()?
        ,&bin_coder::encode(value)?)?;
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
                _ => return errdata!("require active key")
            }
        }
        Ok(active)
    }
}