use crate::db_error::Result;
use crate::storage::engine::{Engine, EngineStatus};
use std::collections::btree_map::Range;
use std::collections::BTreeMap;

// 实现内存引擎
#[derive(Default)]
pub struct Memory(BTreeMap<Vec<u8>, Vec<u8>>);

impl Engine for Memory {
    type ScanIter<'a> = ScanIterator<'a>;

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.0.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.get(key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn scan(&mut self, range: impl std::ops::RangeBounds<Vec<u8>>) -> Self::ScanIter<'_> {
        ScanIterator(self.0.range(range))
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

pub struct ScanIterator<'a>(Range<'a, Vec<u8>, Vec<u8>>);

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}