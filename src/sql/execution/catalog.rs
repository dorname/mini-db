use crate::db_error::Result;
use crate::storage::engine::Engine;
use crate::storage::mvcc::MVCC;
use crate::types::Table;
use crate::utils::bin_coder;

fn catalog_key(table_name: &str) -> Vec<u8> {
    format!("__catalog__\x00{}", table_name).into_bytes()
}

/// 目录管理：使用 MVCC 引擎直接存储表结构（无版本键）
pub struct Catalog;

impl Catalog {
    pub fn get_table<E: Engine>(mvcc: &MVCC<E>, name: &str) -> Result<Option<Table>> {
        match mvcc.get_unversioned(&catalog_key(name))? {
            Some(bytes) if !bytes.is_empty() => Ok(Some(bin_coder::decode(&bytes)?)),
            _ => Ok(None),
        }
    }

    pub fn set_table<E: Engine>(mvcc: &MVCC<E>, table: &Table) -> Result<()> {
        let bytes = bin_coder::encode(table)?;
        mvcc.set_unversioned(&catalog_key(&table.name).to_vec(), &bytes)
    }

    pub fn drop_table<E: Engine>(mvcc: &MVCC<E>, name: &str) -> Result<()> {
        mvcc.set_unversioned(&catalog_key(name).to_vec(), &[])
    }
}
