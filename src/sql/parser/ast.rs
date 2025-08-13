use crate::types::DataType;
use std::collections::BTreeMap;
pub enum Statement {
    /// BEGIN: 开启一个新事务
    /// - read_only: 只读标记
    /// - target_version: 事务版本号
    Begin {
        read_only: bool,
        target_version: Option<u64>,
    },
    /// COMMIT: 事务提交
    Commit,
    /// ROLLBACK: 事务回滚
    Rollback,
    /// EXPLAIN: 展示sql执行计划
    /// 由于不确认sql语言的大小，所以存储在堆里
    Explain(Box<Statement>),
    /// 建表语句
    /// - name: 表名
    /// - columns：列信息
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    /// 删除表语句
    /// - name: 表名
    /// - if_exists: 如果设置为true,即使表不存在也不报错
    DropTable {
        name: String,
        if_exists: bool,
    },
    /// 从指定表里删除数据
    /// - table: 表名
    /// - r#where: 删除条件表达式
    Delete {
        table: String,
        r#where: Option<Expression>,
    },

    /// 插入语句
    /// - table: 表名
    /// - columns: 待插入数据的列
    /// - values: 待插入的数据
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },

    ///更新语句
    /// - table: 表名
    /// - set: 待更新的列和数据
    /// - r#where: 更新条件
    Update {
        table: String,
        set: BTreeMap<String, Option<Expression>>,
        r#where: Option<Expression>,
    },

    ///查询语句
    /// - select: 选中的表达式、列 以及 别名（可选）
    /// - from: 来源表集合
    /// - r#where: 更新条件
    /// - group_by: 分组条件
    /// - having: 过滤条件
    /// - order_by: 排序条件
    /// - offset: 从几行开始返回
    /// - limit: 返回的总条数
    Select {
        select: Vec<(Expression, Option<String>)>,
    },
}

/// From语句
pub enum From {
    /// 表信息
    Table {
        name: String,
        alias: Option<String>,
    },

    /// 连接方式和连接条件
    Join {
        left: Box<From>,
        right: Box<From>,
        r#type: JoinType,
        predicates: Option<Expression>,
    },
}

/// 连接类型
#[derive(Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

/// 表示 `CREATE TABLE` 语句中的列定义。
///
/// 该结构体封装了列的名称、数据类型、约束条件及其他相关元数据，
/// 用于在解析或构建 SQL 表定义时描述单个列的完整信息。
///
/// # 字段说明
///
/// - `name`
///   列名。
///
/// - `datatype`
///   列的数据类型（`DataType` 枚举）。
///
/// - `primary_key`
///   是否为主键列。
///
/// - `nullable`
///   是否允许为 `NULL`。
///   - `Some(true)`：允许 `NULL`
///   - `Some(false)`：不允许 `NULL`
///   - `None`：未显式指定
///
/// - `default`
///   列的默认值（表达式形式），如果未设置则为 `None`。
///
/// - `unique`
///   是否为唯一列（`UNIQUE` 约束）。
///
/// - `index`
///   是否为该列创建索引。
///
/// - `references`
///   外键引用的表名，如果没有外键约束则为 `None`。
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

/// 表示 SQL 表达式（如 `a + 7 > b`），支持嵌套结构。
///
/// 该枚举涵盖了常见的 SQL 表达式类型，包括通配符、列引用、字面量、
/// 函数调用以及运算符等，用于在解析阶段构建抽象语法树（AST）。
///
/// # 变体说明
///
/// - `All`
///   所有列（`*`）。
///
/// - `Column(Option<String>, String)`
///   列引用，可选表名前缀。
///   * `Some(table)`：带表名限定的列，如 `table.column`
///   * `None`：未限定表名的列，如 `column`
///
/// - `Literal(Literal)`
///   字面量值（如字符串、数值、布尔值等）。
///
/// - `Function(String, Vec<Expression>)`
///   函数调用，包含函数名及参数表达式列表。
///
/// - `Operator(Operator)`
///   运算符表达式（如 `+`、`-`、`>` 等），可与其它表达式组合形成更复杂的逻辑或算术运算。
pub enum Expression {
    /// 所有列
    All,
    /// 列关联关系,可以携带一个表别名
    Column(Option<String>, String),
    /// 字面常量
    Literal(Literal),
    /// 函数调用
    Function(String, Vec<Expression>),
    /// 操作
    Operator(Operator),
}


/// 表达式的字面常量
pub enum Literal {
    Null,
    Boolean(bool),
    String(String),
    Integer(i64),
    Float(f64),
}


/// 表达式操作
pub enum Operator {
    /// a and b
    And(Box<Expression>, Box<Expression>),
    /// !a
    Not(Box<Expression>),
    /// a OR b
    Or(Box<Expression>, Box<Expression>),
    /// a=b
    Eq(Box<Expression>, Box<Expression>),
    /// a>b
    Greater(Box<Expression>, Box<Expression>),
    /// a >= b
    GreaterEq(Box<Expression>, Box<Expression>),
    /// is null or is true ...
    IS(Box<Expression>, Literal),
    /// a < b
    Less(Box<Expression>, Box<Expression>),
    /// a <= b
    LessEq(Box<Expression>, Box<Expression>),
    /// a!=b
    NotEq(Box<Expression>, Box<Expression>),
    /// a + b
    Add(Box<Expression>, Box<Expression>),
    /// a / b
    Div(Box<Expression>, Box<Expression>),
    /// a ^ b
    Exp(Box<Expression>, Box<Expression>),
    /// a!
    Factor(Box<Expression>, Box<Expression>),
    /// +a
    Identifier(Box<Expression>),
    /// a*b
    Multiply(Box<Expression>, Box<Expression>),
    /// -a
    Negate(Box<Expression>),
    /// a%b
    Remainder(Box<Expression>),
    /// a-b
    Sub(Box<Expression>, Box<Expression>),
    /// a like b
    Like(Box<Expression>, Box<Expression>),
}
