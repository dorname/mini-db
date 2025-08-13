//! # SQL 基础类型与行迭代模块概览
//!
//! 本模块提供 SQL 层面**原始数据类型**、**值表示**、**行与行迭代器**以及
//! **列标签（Label）** 等通用抽象，供解析、规划与执行阶段复用。
//!
//! ## 主要组成
//! - `DataType`：受限的原始 SQL 类型枚举（`Boolean`/`Integer`/`Float`/`String`）。
//!   实现 `Display` 以标准 SQL 大写形式输出（如 `INTEGER`）。
//!
//! - `Value`：SQL 值的统一承载（含 `Null`/`Boolean(bool)`/
//!   `Integer(i64)`/`Float(f64)`/`String(String)`）。
//!   - **显示/序列化规则**：`Display` 以接近 SQL 的字面量展示；
//!     `Float` 自定义序列化，统一将 **-0.0 与 -NaN 归一为正号**，
//!     以保证键值存储与索引查找的一致性。
//!   - **等价与排序语义**：
//!     * `Null == Null`、`NaN == NaN`（便于检测/索引/排序；真正的 SQL 三值逻辑在表达式求值期实现）；
//!     * `Ord`/`PartialOrd` 定义了跨类型的**全序**：`String > Integer/Float > Boolean > Null`；
//!       混合数值比较使用 `f64` 的 `total_cmp`。
//!   - **哈希语义**：可对 `Null` 与浮点数哈希；`-0.0`/`-NaN` 按正号位等价哈希。
//!   - **类型与判定**：`datatype()` 返回值对应的 `DataType`（`Null` 返回 `None`）；
//!     `is_undefined()` 判定 `NULL` 或 `NaN`。
//!   - **算术检查**：提供 `checked_add` / `checked_sub` / `checked_mul` /
//!     `checked_div` / `checked_pow` / `checked_rem`，在不合法输入（如整型溢出、除零、类型不兼容）时返回错误；
//!     与 `NULL` 的运算遵循“遇 `NULL` 则 `NULL`”的传播规则（数值类）。
//!   - **互转**：实现了与 `bool`/`i64`/`f64`/`String` 的 `From` / `TryFrom`。
//!
//! - 行与迭代：
//!   - `type Row = Vec<Value>`：一行数据即值向量；
//!   - `type Rows = Box<dyn RowIterator>`：行迭代器对象；
//!   - `RowIterator`：**可克隆（`DynClone`）且对象安全**的行迭代 trait，
//!     便于在执行器中“重置/复制”迭代器（例如嵌套循环连接）；
//!     提供 **泛型迭代器的空 blanket 实现** 与 `dyn_clone::clone_trait_object!` 支持。
//!
//! - `Label`：结果集与执行计划中的**列标签**：
//!   - 形态：`None` / `Unqualified(String)` / `Qualified(String, String)`；
//!   - 展示：`Display` 输出 `table.column` 或列名；`as_header()` 返回用于表头的短名称；
//!   - 转换：可从 `Option<String>` 构造；支持转为 `ast::Expression::Column`（不接受 `None`）。
//!
//! ## 适用场景
//! - SQL 解析（AST 构建后）、逻辑/物理计划生成、执行器实现与索引层交互；
//! - 需要统一的值语义（含 `NULL`/`NaN` 等边界）、稳定的比较/哈希规则以及可克隆行迭代的场景。
//!
//! ## 备注
//! - 本模块**不**实现完整的 SQL 三值逻辑与类型提升策略——这些在表达式求值及上层规划/执行阶段处理。
//! - 浮点序列化与哈希的“负号归一化”仅用于**存储键一致性**，不改变运行期的数值语义。

use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};

/// 原始的 SQL 数据类型。为简化实现，仅支持少量标量类型（不支持复合类型）。
/// 符合类型后面再拓展
#[derive(Clone, Copy, Debug, Hash, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    /// 布尔类型：true/false
    Boolean,
    /// 64bit有符号整形
    Integer,
    /// 浮点类型
    Float,
    /// UTF-8编码的字符串
    String,
}

/// 实现格式化打印
impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Integer => write!(f, "Integer"),
            DataType::Float => write!(f, "Float"),
            DataType::String => write!(f, "String"),
        }
    }
}

