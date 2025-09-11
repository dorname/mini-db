use serde::{Deserialize, Serialize};
use crate::types::{DataType, Value};

/// 表的模式，指定其数据结构和约束。
///
/// 表在创建后无法更改。没有 ALTER TABLE 也没有 CREATE/DROP INDEX，
/// 只能使用 CREATE TABLE 和 DROP TABLE。
#[derive(Clone,Debug,PartialEq,Deserialize,Serialize)]
pub struct Table {
    /// 表名,不可为空
    pub name: String,
    /// 主键字段的索引
    pub primary_key: usize,
    /// 列集合,至少一个
    pub columns: Vec<Column>
}

impl crate::utils::Value for Table {}

#[derive(Clone,Debug,Deserialize,Serialize,PartialEq)]
pub struct Column {
    /// 列名 不可为空
    pub name: String,

    /// 列类型
    pub data_type: DataType,

    /// 是否允许为空。对主键无效
    pub nullable: bool,

    /// 列的默认值。如果为 None，用户必须显式指定。
    /// 默认值必须与列的数据类型匹配。可为空的列需要有默认值（通常为 Null）。
    /// 只有当列允许为空时，Null 才是有效的默认值。
    pub default: Option<Value>,

    /// 是否该列只允许唯一值（忽略 NULL）。
    /// 主键列必须为 true。需要索引支持。
    pub unique: bool,

    /// 该列是否应有二级索引。主键列必须为 false（因为主键本身就是主索引）。
    /// 唯一列或引用列必须为 true。
    pub index: bool,

    /// 如果设置了该字段，此列就是对指定表主键的外键引用。
    /// 必须与目标主键的类型相同。需要索引支持。
    pub references: Option<String>,
}