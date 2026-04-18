//! # SQL 基础类型与行迭代模块概览
//!
//! 本模块提供 SQL 层面**原始数据类型**、**值表示**、**行与行迭代器**以及
//! **列标签（Label）** 等通用抽象，供解析、规划与执行阶段复用。

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize, Serializer};

use crate::db_error::{Error, Result};

/// 原始的 SQL 数据类型。为简化实现，仅支持少量标量类型（不支持复合类型）。
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::String => write!(f, "STRING"),
        }
    }
}

impl DataType {
    /// 返回该类型的默认值
    pub fn default_value(&self) -> Value {
        match self {
            DataType::Boolean => Value::Boolean(false),
            DataType::Integer => Value::Integer(0),
            DataType::Float => Value::Float(0.0),
            DataType::String => Value::String(String::new()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(#[serde(serialize_with = "serialize_f64")] f64),
    String(String),
}

impl crate::utils::Value for Value {}

impl Value {
    pub fn datatype(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Integer(_) => Some(DataType::Integer),
            Value::Float(_) => Some(DataType::Float),
            Value::String(_) => Some(DataType::String),
        }
    }

    pub fn is_undefined(&self) -> bool {
        match self {
            Value::Null => true,
            Value::Float(f) => f.is_nan(),
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn checked_add(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => a.checked_add(*b).map(Value::Integer).ok_or_else(|| Error::InvalidData("integer overflow".into())),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            _ => Err(Error::InvalidData(format!("cannot add {:?} and {:?}", self, other))),
        }
    }

    pub fn checked_sub(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => a.checked_sub(*b).map(Value::Integer).ok_or_else(|| Error::InvalidData("integer overflow".into())),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a - *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            _ => Err(Error::InvalidData(format!("cannot subtract {:?} and {:?}", self, other))),
        }
    }

    pub fn checked_mul(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => a.checked_mul(*b).map(Value::Integer).ok_or_else(|| Error::InvalidData("integer overflow".into())),
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a * *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            _ => Err(Error::InvalidData(format!("cannot multiply {:?} and {:?}", self, other))),
        }
    }

    pub fn checked_div(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Integer(a / b))
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(a / *b as f64))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(a / b))
            }
            _ => Err(Error::InvalidData(format!("cannot divide {:?} and {:?}", self, other))),
        }
    }

    pub fn checked_rem(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => {
                if *b == 0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Integer(a % b))
            }
            (Value::Integer(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(*a as f64 % b))
            }
            (Value::Float(a), Value::Integer(b)) => {
                if *b == 0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(a % *b as f64))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 { return Err(Error::InvalidData("division by zero".into())); }
                Ok(Value::Float(a % b))
            }
            _ => Err(Error::InvalidData(format!("cannot remainder {:?} and {:?}", self, other))),
        }
    }

    pub fn checked_pow(&self, other: &Self) -> Result<Self> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Integer(a), Value::Integer(b)) => {
                if *b < 0 {
                    return Ok(Value::Float((*a as f64).powi(*b as i32)));
                }
                a.checked_pow(*b as u32).map(Value::Integer).ok_or_else(|| Error::InvalidData("integer overflow".into()))
            }
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float((*a as f64).powf(*b))),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a.powi(*b as i32))),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a.powf(*b))),
            _ => Err(Error::InvalidData(format!("cannot pow {:?} and {:?}", self, other))),
        }
    }

    pub fn not(&self) -> Result<Self> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Boolean(b) => Ok(Value::Boolean(!b)),
            _ => Err(Error::InvalidData(format!("cannot NOT {:?}", self))),
        }
    }

    pub fn negate(&self) -> Result<Self> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Integer(i) => i.checked_neg().map(Value::Integer).ok_or_else(|| Error::InvalidData("integer overflow".into())),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(Error::InvalidData(format!("cannot negate {:?}", self))),
        }
    }

    /// SQL 语义：Null => false，Boolean => 原值，其他非零/非空 => true
    pub fn to_bool(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Boolean(b) => *b,
            Value::Integer(i) => *i != 0,
            Value::Float(f) => *f != 0.0 && !f.is_nan(),
            Value::String(s) => !s.is_empty(),
        }
    }
}

fn serialize_f64<S: Serializer>(value: &f64, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    let mut value = *value;
    if (value.is_nan() || value == 0.0) && value.is_sign_negative() {
        value = -value;
    }
    serializer.serialize_f64(value)
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(a), Self::Boolean(b)) => a == b,
            (Self::Integer(a), Self::Integer(b)) => a == b,
            (Self::Integer(a), Self::Float(b)) => *a as f64 == *b,
            (Self::Float(a), Self::Float(b)) => a == b,
            (Self::Float(a), Self::Integer(b)) => *a == *b as f64,
            (Self::String(a), Self::String(b)) => a == b,
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        use Value::*;
        let kind_order = |v: &Value| match v {
            Null => 0,
            Boolean(_) => 1,
            Integer(_) | Float(_) => 2,
            String(_) => 3,
        };
        let self_kind = kind_order(self);
        let other_kind = kind_order(other);
        self_kind.cmp(&other_kind).then_with(|| match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Boolean(a), Boolean(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (Integer(a), Float(b)) => (*a as f64).total_cmp(b),
            (Float(a), Integer(b)) => a.total_cmp(&(*b as f64)),
            (Float(a), Float(b)) => a.total_cmp(b),
            (String(a), String(b)) => a.cmp(b),
            _ => Ordering::Equal,
        })
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => {
                let mut v = *f;
                if (v.is_nan() || v == 0.0) && v.is_sign_negative() {
                    v = -v;
                }
                v.to_bits().hash(state);
            }
            Value::String(s) => s.hash(state),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self { Value::Boolean(v) }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self { Value::Integer(v) }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self { Value::Integer(v as i64) }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self { Value::Float(v) }
}

impl From<String> for Value {
    fn from(v: String) -> Self { Value::String(v) }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self { Value::String(v.to_owned()) }
}

impl TryFrom<Value> for bool {
    type Error = Error;
    fn try_from(v: Value) -> Result<Self> {
        match v {
            Value::Boolean(b) => Ok(b),
            _ => Err(Error::InvalidData(format!("expected boolean, got {:?}", v))),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = Error;
    fn try_from(v: Value) -> Result<Self> {
        match v {
            Value::Integer(i) => Ok(i),
            _ => Err(Error::InvalidData(format!("expected integer, got {:?}", v))),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = Error;
    fn try_from(v: Value) -> Result<Self> {
        match v {
            Value::Float(f) => Ok(f),
            Value::Integer(i) => Ok(i as f64),
            _ => Err(Error::InvalidData(format!("expected float, got {:?}", v))),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = Error;
    fn try_from(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => Ok(s),
            _ => Err(Error::InvalidData(format!("expected string, got {:?}", v))),
        }
    }
}

// --- Row 与 RowIterator ---

pub type Row = Vec<Value>;

dyn_clone::clone_trait_object!(RowIterator);

pub trait RowIterator: dyn_clone::DynClone + Iterator<Item = Result<Row>> + Send {}

impl<T> RowIterator for T where T: dyn_clone::DynClone + Iterator<Item = Result<Row>> + Send {}

pub type Rows = Box<dyn RowIterator>;

// --- Label ---

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Label {
    None,
    Unqualified(String),
    Qualified(String, String),
}

impl Label {
    pub fn as_header(&self) -> String {
        match self {
            Label::None => String::new(),
            Label::Unqualified(name) => name.clone(),
            Label::Qualified(_, name) => name.clone(),
        }
    }
}

impl Display for Label {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Label::None => write!(f, "?"),
            Label::Unqualified(name) => write!(f, "{}", name),
            Label::Qualified(table, name) => write!(f, "{}.{}", table, name),
        }
    }
}

impl From<Option<String>> for Label {
    fn from(v: Option<String>) -> Self {
        match v {
            Some(name) => Label::Unqualified(name),
            None => Label::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_ord() {
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Boolean(true) < Value::Integer(0));
        assert!(Value::Integer(1) < Value::Float(100.0));
        assert!(Value::Float(1.0) < Value::String("a".into()));
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Float(1.5) > Value::Integer(1));
    }

    #[test]
    fn test_value_arithmetic() {
        let a = Value::Integer(10);
        let b = Value::Integer(3);
        assert_eq!(a.checked_add(&b).unwrap(), Value::Integer(13));
        assert_eq!(a.checked_sub(&b).unwrap(), Value::Integer(7));
        assert_eq!(a.checked_mul(&b).unwrap(), Value::Integer(30));
        assert_eq!(a.checked_div(&b).unwrap(), Value::Integer(3));
        assert_eq!(a.checked_rem(&b).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_value_null_propagation() {
        let a = Value::Integer(5);
        let null = Value::Null;
        assert_eq!(a.checked_add(&null).unwrap(), Value::Null);
        assert_eq!(null.checked_sub(&a).unwrap(), Value::Null);
    }

    #[test]
    fn test_value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Boolean(true)), "true");
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::String("hello".into())), "'hello'");
    }

    #[test]
    fn test_label() {
        let l = Label::Qualified("users".into(), "name".into());
        assert_eq!(format!("{}", l), "users.name");
        assert_eq!(l.as_header(), "name");
    }
}
