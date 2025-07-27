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

use crate::storage::engine::Engine;
use serde::{Serialize, Deserialize};

#[allow(dead_code)]
pub struct MVCC<E:Engine> {
    // 引擎 增加原子指针和互斥锁，目的是实现线程安全的引擎
    engine: Arc<Mutex<E>>,
}

impl<E:Engine> MVCC<E> {
    pub fn new(engine: E) -> Self {
        Self { engine: Arc::new(Mutex::new(engine)) }
    }
}

/// 一个事务的版本号是逻辑上的时间戳
/// 每个版本属于一个独立的读/写事务
/// 每次读/写事务开始时，需要更新版本号
pub type Version = u64;

/// 事务状态的枚举类
#[derive(Serialize, Deserialize)]
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

/// 事务结构体
pub struct Transaction {
    
}