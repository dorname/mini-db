/// 数据事务模块
use std::sync::{Arc, Mutex};

use crate::storage::engine::Engine;

pub struct MVCC<E:Engine> {
    // 引擎 增加原子指针和互斥锁，目的是实现线程安全的引擎
    engine: Arc<Mutex<E>>,
}

impl<E:Engine> MVCC<E> {
    pub fn new(engine: E) -> Self {
        Self { engine: Arc::new(Mutex::new(engine)) }
    }
}