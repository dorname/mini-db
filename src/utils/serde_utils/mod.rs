use crate::db_error::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

pub mod bin_coder;
pub mod key_coder;


/// 实现一个特征，用于注入自定义序列化方法
pub trait Key<'de>: Serialize + Deserialize<'de> {
    fn encode(&self) -> Result<Vec<u8>> {
        key_coder::encode(self)
    }
    fn decode(bytes: &'de [u8]) -> Result<Self> {
        key_coder::decode(bytes)
    }
}


/// 实现一个特征，用于注入自定义反序列化方法
pub trait Value: Serialize + DeserializeOwned {
    fn encode(&self) -> Result<Vec<u8>> {
        bin_coder::encode(self)
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        bin_coder::decode(bytes)
    }

    fn encode_into<W, T>(writer: &mut W, value: &T) -> Result<()>
    where
        W: std::io::Write,
        T: Serialize,
    {
        bin_coder::encode_into(writer, value)?;
        Ok(())
    }

    fn decode_from<R, T>(mut reader: R) -> Result<T>
    where
        R: std::io::Read,
        T: DeserializeOwned,
    {
        bin_coder::decode_from(&mut reader)
    }
}

/// 为包含 Value 类型的常见容器和组合类型自动实现了 Value trait，
/// 无需为每种组合类型单独编写实现代码
impl<V: Value> Value for Option<V> {}
impl<V: Value> Value for Result<V> {}
impl<V: Value> Value for Vec<V> {}
impl<V1: Value, V2: Value> Value for (V1, V2) {}
impl<V: Value + Eq + Hash> Value for HashSet<V> {}
impl<V: Value + Eq + Ord + Hash> Value for BTreeSet<V> {}