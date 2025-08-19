use crate::types::DataType;
use serde::de::Visitor;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::ops::Mul;

#[derive(Debug)]
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
        from: Vec<From>,
        r#where: Option<Expression>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
        order_by: Vec<(Expression, Direction)>,
        offset: Option<Expression>,
        limit: Option<Expression>,
    },
}

/// From语句
#[derive(Debug)]
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

impl JoinType {
    /// 内外连接判断函数
    /// - cross | inner => false
    /// - left | right => true
    pub fn is_outer(&self) -> bool {
        match self {
            JoinType::Cross | JoinType::Inner => false,
            JoinType::Left | JoinType::Right => true
        }
    }
}

/// 升降序
#[derive(Debug, Default)]
pub enum Direction {
    #[default]
    Asc,
    Desc,
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
#[derive(Debug)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Boolean(bool),
    String(String),
    Integer(i64),
    Float(f64),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Null, Literal::Null) => true,
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::String(a), Literal::String(b)) => a == b,
            (Literal::Integer(a), Literal::Integer(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(), //使用to_bits来比较 是因为f64没有实现Eq特征，不具备完全等价的特性
            _ => false
        }
    }
}

impl Eq for Literal {}

impl Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state); // 因为最终都是使用内部数据进行hash,所以如果不加这个处理，对于同样的内部数据可能会出现hash碰撞
        match self {
            Literal::Null => {}
            Literal::Boolean(b) => b.hash(state),
            Literal::String(s) => s.hash(state),
            Literal::Integer(i) => i.hash(state),
            Literal::Float(f) => f.to_bits().hash(state),
        }
    }
}


/// 表达式操作
#[derive(Debug, Clone)]
pub enum Operator {
    /// a and b
    And(Box<Expression>, Box<Expression>),
    /// a OR b
    Or(Box<Expression>, Box<Expression>),
    /// a=b
    Eq(Box<Expression>, Box<Expression>),
    /// a>b
    Greater(Box<Expression>, Box<Expression>),
    /// a >= b
    GreaterEq(Box<Expression>, Box<Expression>),
    /// is null or is true ...
    Is(Box<Expression>, Literal),
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
    /// a*b
    Multiply(Box<Expression>, Box<Expression>),
    /// a%b
    Remainder(Box<Expression>, Box<Expression>),
    /// a-b
    Sub(Box<Expression>, Box<Expression>),
    /// a like b
    Like(Box<Expression>, Box<Expression>),
    /// !a
    Not(Box<Expression>),
    /// a!
    Factor(Box<Expression>),
    /// +a
    Identifier(Box<Expression>),
    /// -a
    Negate(Box<Expression>),
}

impl Expression {
    /// 深度优先遍历（DFS）当前表达式树，并对每个节点调用提供的闭包。
    ///
    /// 遍历过程中会将当前节点的引用传递给 `visitor` 闭包，
    /// 若闭包返回 `false`，则会**立即终止遍历**并返回 `false`；
    /// 若遍历完整棵树且未中途终止，则返回 `true`。
    ///
    /// # 参数
    /// - `visitor`
    ///   一个可变闭包，签名为 `FnMut(&Expression) -> bool`：
    ///   * 参数为当前访问到的表达式节点引用；
    ///   * 返回 `true` 继续遍历子节点，返回 `false` 停止遍历。
    ///
    /// # 返回值
    /// - `true`：已遍历完整棵表达式树；
    /// - `false`：闭包返回了 `false`，提前中止。
    ///
    /// # 遍历规则
    /// - 对于二元运算符（如 `+`、`-`、`>` 等），会依次递归遍历左右子表达式。
    /// - 对于一元运算符（如取反 `-`、逻辑非 `NOT`、阶乘等），递归遍历其唯一子表达式。
    /// - 对于函数调用，会遍历函数的全部参数表达式。
    /// - 对于 `All`（`*`）、`Column`、`Literal` 等叶子节点，不会继续向下遍历。
    ///
    /// # 示例
    /// ```ignore
    /// // 打印表达式树中所有节点
    /// expr.walk(&mut |node| {
    ///     println!("{:?}", node);
    ///     true // 返回 true 继续遍历
    /// });
    /// ```

    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        use Operator::*;
        // 递归提前终止条件
        // 如果当前表达式，在传入的闭包结果已经是false,则没必要拆解多个表达式放到闭包函数中执行
        if !visitor(self) {
            return false;
        }
        match self {
            Self::Operator(op) => match op {
                Add(left, right)
                | Div(left, right)
                | Exp(left, right)
                | Sub(left, right)
                | Like(left, right)
                | NotEq(left, right)
                | And(left, right)
                | Or(left, right)
                | Eq(left, right)
                | Greater(left, right)
                | GreaterEq(left, right)
                | Less(left, right)
                | LessEq(left, right)
                | Multiply(left, right)
                | Remainder(left, right)
                => left.walk(visitor) && right.walk(visitor),
                Factor(expr)
                | Identifier(expr)
                | Negate(expr)
                | Not(expr)
                | Is(expr, _)
                => expr.walk(visitor),
            },
            Expression::Function(_, expresses) => expresses.iter().any(|expr| expr.walk(visitor)),
            Expression::All
            | Expression::Column(_, _)
            | Expression::Literal(_) => true
        }
    }

    /// 检查当前表达式树中是否存在满足条件的节点。
    ///
    /// 该方法会对表达式树执行深度优先遍历（基于 [`walk`](#method.walk)），
    /// 并在遍历过程中将每个节点传递给提供的 `visitor` 闭包进行检测。
    ///
    /// 一旦 `visitor` 对任意节点返回 `true`，`contains` 会立即返回 `true`，
    /// 不再继续遍历；如果遍历完整棵树都未命中条件，则返回 `false`。
    ///
    /// # 参数
    /// - `visitor`
    ///   判定闭包，类型为 `&impl Fn(&Expression) -> bool`：
    ///   * 参数：当前访问到的表达式节点引用；
    ///   * 返回值：是否命中条件（`true` 表示找到目标）。
    ///
    /// # 返回值
    /// - `true`：表达式树中存在至少一个满足条件的节点；
    /// - `false`：未找到符合条件的节点。
    ///
    /// # 实现细节
    /// 内部通过调用 [`walk`](#method.walk) 实现：
    /// - 将 `visitor` 包装成反逻辑闭包传给 `walk`，使得 `walk` 在 `visitor` 返回 `true`
    ///   时立即终止（通过 `walk` 的“提前退出”机制）。
    ///
    /// # 示例
    /// ```ignore
    /// // 判断表达式中是否包含任何列引用
    /// let has_column = expr.contains(&|node| matches!(node, Expression::Column(_, _)));
    /// assert!(has_column);
    /// ```

    pub fn contains(&self, visitor: impl Fn(&Expression) -> bool) -> bool {
        !self.walk(&mut |expr| !visitor(expr))
    }

    pub fn collect(&self, visitor: &impl Fn(&Expression) -> bool, expresses: &mut Vec<Expression>) {
        use Operator::*;

        if !visitor(self) {
            expresses.push(self.clone());
            return;
        }
        match self {
            Self::Operator(op) => match op {
                Add(left, right)
                | Div(left, right)
                | Exp(left, right)
                | Sub(left, right)
                | Like(left, right)
                | NotEq(left, right)
                | And(left, right)
                | Or(left, right)
                | Eq(left, right)
                | Greater(left, right)
                | GreaterEq(left, right)
                | Less(left, right)
                | LessEq(left, right)
                | Multiply(left, right)
                | Remainder(left, right)
                => {
                    left.collect(visitor, expresses);
                    right.collect(visitor, expresses);
                }
                Factor(expr)
                | Identifier(expr)
                | Negate(expr)
                | Not(expr)
                | Is(expr, _)
                => expr.collect(visitor, expresses),
            },
            Expression::Function(_, args) => args.iter().for_each(|expr| expr.collect(visitor, expresses)),
            Expression::All
            | Expression::Column(_, _)
            | Expression::Literal(_) => {}
        }
    }
}

impl core::convert::From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Self::Literal(literal)
    }
}

impl core::convert::From<Operator> for Expression {
    fn from(operator: Operator) -> Self {
        Self::Operator(operator)
    }
}

impl core::convert::From<Operator> for Box<Expression> {
    fn from(operator: Operator) -> Self {
        Box::new(operator.into())
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::mem;

    #[test]
    #[ignore]
    fn f64_eq_test() {
        let a = f64::NAN;
        let b = f64::NAN;
        assert_ne!(a, b); // a!=b 因为f64没有实现Eq所以不具备完全等价的特性
    }

    #[test]
    #[ignore]
    fn discriminants_test() {
        #[derive(Debug)]
        enum Shape {
            Circle(u64),
            Rectangle { w: u64, h: u64 },
        }

        let a = Shape::Circle(1);
        let b = Shape::Circle(2);
        let c = Shape::Rectangle { w: 3, h: 4 };

        // 打印 discriminant 值
        println!("{:?}", core::mem::discriminant(&a));
        println!("{:?}", core::mem::discriminant(&b));
        println!("{:?}", core::mem::discriminant(&c));

        // 比较 discriminant
        println!("a 与 b 变体相同吗? {}", core::mem::discriminant(&a) == core::mem::discriminant(&b));
        println!("a 与 c 变体相同吗? {}", core::mem::discriminant(&a) == core::mem::discriminant(&c));
    }

    #[test]
    #[ignore]
    fn discriminants_test_2() {
        #[derive(Debug)]
        enum Value {
            Int(i32),
            Float(f64),
        }

        impl Hash for Value {
            fn hash<H: Hasher>(&self, state: &mut H) {
                mem::discriminant(self).hash(state);
                match self {
                    Value::Int(v) => v.hash(state),
                    Value::Float(v) => v.to_bits().hash(state),
                }
            }
        }

        let x = Value::Int(42);
        let y = Value::Float(42.0);

        let mut hx = DefaultHasher::new();
        x.hash(&mut hx);

        let mut hy = DefaultHasher::new();
        y.hash(&mut hy);

        println!("hash(x) = {}", hx.finish());
        println!("hash(y) = {}", hy.finish());
    }
}
