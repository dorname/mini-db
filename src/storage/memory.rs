use std::collections::BTreeMap;
use crate::storage::engine::{Engine, EngineStatus};
use crate::db_error::Result;

// 实现内存引擎
#[derive(Default)]
pub struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);

impl Engine for Memory {
    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<String>> {
        Ok(self.0.get(key).map(|v| String::from_utf8_lossy(v).to_string()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn scan(&self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Result<Vec<(Vec<u8>, String)>> {
        let mut result = Vec::new();
        for (key, value) in self.0.range(range) {
            result.push((key.clone(), String::from_utf8_lossy(value).to_string()));
        }
        Ok(result)
    }

    fn clear(&mut self) -> Result<()> {
        self.0.clear();
        Ok(())
    }

    fn status(&mut self) -> Result<EngineStatus> {
        Ok(EngineStatus {
            name: "memory".to_string(),
            logical_size: self.0.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as u64,
            total_count: self.0.len() as u64,
            total_size: self.0.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as u64,
            live_size: self.0.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as u64,
            garbage_size: 0,
        })
    }
}