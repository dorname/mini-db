# mini-db 技术实现文档

> 本文档聚焦代码层面的实现细节，包含文件结构、关键类型、数据格式、编码规则与测试策略，面向需要深入阅读或修改代码的开发者。

---

## 1. 项目文件结构

```
mini-db/
├── Cargo.toml                  # 依赖配置
├── config.toml                 # 运行时配置文件
├── README.md                   # 项目概述与快速开始
├── docs/                       # 技术文档（本目录）
│   ├── architecture.md         # 架构设计文档
│   ├── technical-design.md     # 技术设计文档
│   └── technical-implementation.md  # 本文档
├── src/
│   ├── main.rs                 # 程序入口：HTTP Server / CLI / Exec 三种形态
│   ├── lib.rs                  # 库入口：Database 会话封装、tracing 初始化
│   ├── db_error.rs             # 统一错误类型 Error 枚举与 Result 别名
│   ├── cfg/                    # 配置系统
│   │   ├── mod.rs              # 全局单例 CONFIG、辅助函数、测试覆盖
│   │   ├── config.rs           # Config 结构体、Builder、加载逻辑
│   │   └── watcher.rs          # notify 热重载实现
│   ├── sql/                    # SQL 引擎
│   │   ├── mod.rs              # SQL 模块入口
│   │   ├── lexer.rs            # 词法分析器：SQL 字符串 → Token 流
│   │   ├── parser/
│   │   │   ├── mod.rs          # 语法分析器：Token 流 → AST
│   │   │   └── ast.rs          # AST 节点定义：Statement、Expression、Operator、From 等
│   │   ├── planner/
│   │   │   ├── mod.rs          # Planner 模块入口
│   │   │   ├── planner.rs      # AST → Plan 转换、Catalog 校验
│   │   │   └── plan.rs         # 执行计划树：Node 枚举定义
│   │   └── execution/
│   │       └── mod.rs          # 执行引擎：各算子实现与表达式求值
│   ├── storage/                # 存储引擎
│   │   ├── mod.rs              # Storage 模块入口
│   │   ├── engine.rs           # Engine trait 定义
│   │   ├── bitcask.rs          # BitCask 日志结构化存储引擎
│   │   ├── mvcc.rs             # MVCC 多版本并发控制层
│   │   └── memory.rs           # 内存存储引擎（BTreeMap 封装，测试用）
│   ├── types/                  # 核心类型系统
│   │   ├── mod.rs              # Types 模块入口
│   │   ├── value.rs            # Value 枚举、Row、Label、三值逻辑
│   │   ├── schema.rs           # Table、Column 表结构定义
│   │   └── expression.rs       # Expression 重导出
│   └── utils/                  # 工具模块
│       ├── mod.rs              # Utils 模块入口
│       ├── timestamp.rs        # Unix 时间戳获取
│       ├── format.rs           # 原始字节调试格式化
│       └── serde_utils/
│           ├── mod.rs          # Key / Value 序列化 trait 定义
│           ├── bin_coder.rs    # bincode 封装
│           └── key_coder.rs    # 自定义 order-preserving 键编码器/解码器
├── tests/
│   └── integration_test.rs     # 集成测试：完整 CRUD、ORDER BY/GROUP BY、持久化
└── db/                         # 默认数据目录（运行期生成）
```

---

## 2. SQL 模块实现细节

### 2.1 Lexer（`src/sql/lexer.rs`）

**核心结构：**

```rust
pub struct Lexer<'a> {
    input: &'a str,      // 原始输入
    pos: usize,          // 当前字节位置
    current: char,       // 当前字符（lookahead）
}
```

**工作流程：**
1. `Lexer::new(input)` 初始化，读取第一个字符到 `current`
2. `next_token()` 循环返回 Token，直到 `EOF`
3. 每次 `advance()` 移动 `pos` 并更新 `current`
4. 根据 `current` 的类型分派到不同的解析逻辑：
   - 空白字符 → 跳过
   - 字母/下划线 → 解析标识符或关键字
   - 数字 → 解析整数或浮点数
   - 单引号 → 解析字符串字面量
   - 运算符字符 → 解析单字符或多字符运算符（如 `<=`, `!=`）

**数字解析逻辑：** 先读取整数部分，若遇到 `.` 则继续读取小数部分，返回 `Token::Float` 或 `Token::Integer`。

**字符串解析逻辑：** 遇到 `'` 开始，持续读取直到下一个未转义的 `'`。转义规则：`''` → 单个 `'`。

### 2.2 Parser（`src/sql/parser/mod.rs` + `ast.rs`）

**核心结构：**

```rust
pub struct Parser<'a> {
    lexer: Lexer<'a>,
    current: Token,    // lookahead token
}
```

**解析入口：** `parse(sql)` 创建 Parser，调用 `parse_statement()`，根据当前 Token 分派到具体解析函数。

**表达式解析（优先级爬升）：**

```rust
fn parse_expression(&mut self, precedence: u8) -> Result<Expression> {
    let mut left = self.parse_prefix()?;  // 解析一元/主表达式
    while precedence < self.current_precedence() {
        let op = self.current.clone();
        self.advance();
        let right = self.parse_expression(self.infix_precedence(&op))?;
        left = Expression::Operator(op.into(), vec![left, right]);
    }
    Ok(left)
}
```

**AST 核心类型（`ast.rs`）：**

```rust
pub enum Statement {
    CreateTable { name: String, columns: Vec<Column> },
    DropTable { name: String, if_exists: bool },
    Insert { table: String, columns: Option<Vec<String>>, values: Vec<Vec<Expression>> },
    Update { table: String, set: Vec<(String, Expression)>, where_clause: Option<Expression> },
    Delete { table: String, where_clause: Option<Expression> },
    Select { columns: Vec<(Expression, Option<String>)>, from: Vec<From>,
             where_clause: Option<Expression>, group_by: Vec<Expression>,
             having: Option<Expression>, order: Vec<(Expression, OrderDirection)>,
             limit: Option<Expression>, offset: Option<Expression> },
    Begin { mode: TransactionMode, as_of: Option<Expression> },
    Commit,
    Rollback,
    Explain(Box<Statement>),
}

pub enum Expression {
    Literal(Value),
    Field(Option<String>, String),   // (table, column)
    Operator(Operator, Vec<Expression>),
    Function(String, Vec<Expression>),
}

pub enum Operator {
    Add, Subtract, Multiply, Divide, Modulo, Power, Factorial,
    Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual,
    Is, Like, In, Between,
    And, Or, Not,
    Negative, BitwiseNot,
    Period,         // table.column
}
```

### 2.3 Planner（`src/sql/planner/planner.rs` + `plan.rs`）

**入口函数：**

```rust
pub fn plan<E: Engine>(mvcc: &MVCC<E>, statement: &Statement) -> Result<Plan> {
    // 根据 statement 类型生成对应的 Plan / Node
}
```

**`Plan` 结构：**

```rust
pub struct Plan {
    pub node: Node,
}

pub enum Node {
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expression>,
    },
    NestedLoopJoin {
        left: Box<Node>,
        right: Box<Node>,
        join_type: JoinType,
        predicate: Option<Expression>,
    },
    Projection {
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Filter {
        source: Box<Node>,
        predicate: Expression,
    },
    Aggregate {
        source: Box<Node>,
        aggregates: Vec<Aggregate>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
    },
    Order {
        source: Box<Node>,
        orders: Vec<(Expression, OrderDirection)>,
    },
    Limit {
        source: Box<Node>,
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Values {
        values: Vec<Vec<Expression>>,
    },
    Insert { table: String, columns: Vec<String>, source: Box<Node> },
    Update { table: String, set: Vec<(String, Expression)>, source: Box<Node> },
    Delete { table: String, source: Box<Node> },
    Explain(Box<Node>),
    Nothing,
}
```

**SELECT 计划生成流程：**

```rust
// 1. FROM / JOIN → Scan / NestedLoopJoin
let mut node = build_from(&from_tables)?;

// 2. WHERE → Filter
if let Some(pred) = where_clause {
    node = Node::Filter { source: Box::new(node), predicate: pred };
}

// 3. GROUP BY + HAVING + 聚合 → Aggregate
if !group_by.is_empty() || has_aggregates(&columns) {
    node = Node::Aggregate { source: Box::new(node), aggregates, group_by, having };
}

// 4. SELECT 列 → Projection
node = Node::Projection { source: Box::new(node), expressions: columns };

// 5. ORDER BY → Order
if !order.is_empty() {
    node = Node::Order { source: Box::new(node), orders };
}

// 6. LIMIT / OFFSET → Limit
if limit.is_some() || offset.is_some() {
    node = Node::Limit { source: Box::new(node), limit, offset };
}
```

**Catalog 校验：** Planner 使用 `Catalog`（封装 MVCC 读操作）获取表元数据，校验列名存在性。`Catalog::get_table(name)` 从 MVCC 读取表定义并反序列化。

### 2.4 Executor（`src/sql/execution/mod.rs`）

**入口函数：**

```rust
pub fn execute<E: Engine>(mvcc: &MVCC<E>, plan: &Plan) -> Result<ResultSet> {
    execute_node(mvcc, &plan.node)
}
```

**核心算子实现要点：**

#### Scan

```rust
Node::Scan { table, alias, filter } => {
    // 1. 构造表前缀键（编码表名）
    let prefix = encode_table_prefix(&table);
    // 2. 从 MVCC scan_prefix 获取所有行
    let rows = mvcc.scan_prefix(&prefix)?;
    // 3. 反序列化每行
    // 4. 如有 filter，在 Scan 层预过滤
}
```

#### NestedLoopJoin

```rust
Node::NestedLoopJoin { left, right, join_type, predicate } => {
    let left_rows = execute_node(mvcc, left)?;
    let right_rows = execute_node(mvcc, right)?;
    
    for left_row in left_rows {
        let mut matched = false;
        for right_row in right_rows.clone() {
            let combined = combine_rows(left_row, right_row);
            if evaluate(predicate, &combined)?.to_bool() {
                matched = true;
                output.push(combined);
            }
        }
        // LEFT JOIN: 未匹配的行补 NULL 输出
        if !matched && join_type == Left {
            output.push(combine_with_null(left_row, right_schema_len));
        }
    }
}
```

#### Aggregate

```rust
Node::Aggregate { source, aggregates, group_by, having } => {
    let input = execute_node(mvcc, source)?;
    let mut groups: HashMap<Vec<Value>, Vec<AggregateState>> = HashMap::new();
    
    for row in input {
        let group_key: Vec<Value> = group_by.iter()
            .map(|expr| evaluate(expr, &row))
            .collect();
        let state = groups.entry(group_key).or_default();
        
        for (i, agg) in aggregates.iter().enumerate() {
            state[i].accumulate(evaluate(&agg.expr, &row)?)?;
        }
    }
    
    // 输出每组聚合结果，应用 HAVING 过滤
}
```

**表达式求值函数：**

```rust
fn evaluate<E: Engine>(expr: &Expression, row: &Row, scope: &Scope) -> Result<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Field(table, column) => scope.resolve(table, column, row),
        Expression::Operator(op, args) => {
            let values: Vec<Value> = args.iter()
                .map(|a| evaluate(a, row, scope))
                .collect();
            apply_operator(op, values)
        }
        Expression::Function(name, args) => {
            let values: Vec<Value> = args.iter()
                .map(|a| evaluate(a, row, scope))
                .collect();
            apply_function(name, values)
        }
    }
}
```

---

## 3. Storage 模块实现细节

### 3.1 Engine Trait（`src/storage/engine.rs`）

```rust
pub trait Engine: Clone + Send + 'static {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;
    fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIterator>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<ScanIterator>;
}
```

**设计要点：**
- `Clone`：Engine 需要被多层封装（MVCC 需要持有 Engine 的克隆）
- `Send + 'static`：支持在 async 上下文中使用
- 键值均为 `Vec<u8>`：完全的字节抽象，上层负责语义编码

### 3.2 BitCask（`src/storage/bitcask.rs`）

**核心结构：**

```rust
pub struct BitCask {
    keydir: Arc<Mutex<BTreeMap<Vec<u8>, KeyDirEntry>>>,  // 内存索引
    active: Arc<Mutex<Log>>,                              // 当前活跃日志文件
    file_cache: Arc<Mutex<LruCache<u64, File>>>,          // 历史文件句柄缓存
}

struct KeyDirEntry {
    file_id: u64,       // 文件标识
    crc_pos: u64,       // CRC 校验码在文件中的偏移
    // value 的位置可通过 crc_pos + 固定头长度 计算
}

struct Log {
    file_id: u64,
    file: File,
    offset: u64,        // 当前写入偏移
}
```

**日志条目格式（磁盘）：**

```
Offset    Content                              Size
─────────────────────────────────────────────────────────
0         CRC (SHA3-256 前 8 字节)              8 bytes
8         TSID (时间戳 ID)                       8 bytes
16        Key Length (变长: u8/u16/u32/u64)      1/2/4/8 bytes
...       Key bytes                              KeyLen
...       Value Length (变长)                    1/2/4/8 bytes
...       Value bytes                            ValueLen
```

**写入流程：**
1. 构建完整日志条目字节
2. 追加写入 `active` 文件的当前 `offset`
3. 更新 `keydir`：`key → (active.file_id, crc_pos)`
4. 根据 `sync_strategy` 决定是否 fsync

**读取流程：**
1. 查 `keydir` 获取 `(file_id, crc_pos)`
2. 若 `file_id == active.file_id`，从 `active` 文件读取
3. 否则从 `file_cache` 获取历史文件句柄，定位到 `crc_pos`
4. 读取 CRC，验证 SHA3-256
5. 读取 TSID、KeyLen、Key、ValueLen、Value
6. 返回 Value

**文件轮转：**

```rust
fn check_size_limit(&mut self) -> Result<()> {
    if active.offset >= get_max_size() {
        // 1. 关闭当前 active 文件
        // 2. 生成新 file_id（时间戳）
        // 3. 创建新的 active 日志文件
        // 4. 更新 self.active
    }
}
```

**Compaction：**

```rust
fn compact(&mut self) -> Result<()> {
    // 1. 遍历所有历史日志文件
    // 2. 对每个条目，检查 keydir 是否仍指向该文件的该位置
    // 3. 收集所有"存活"条目
    // 4. 写入新的紧凑化文件
    // 5. 更新 keydir 指向新位置
    // 6. 删除旧文件
}
```

**启动恢复：**

`BitCask::init_db()` 启动时会扫描数据目录下的所有日志文件，逐条读取 CRC/TSID/Key/Value，重建内存中的 `keydir`。这是 BitCask 的"代价"——启动时间与数据量成正比。

### 3.3 MVCC（`src/storage/mvcc.rs`）

**核心结构：**

```rust
pub struct MVCC<E: Engine> {
    engine: E,
    // 事务状态通过键空间编码管理，无额外内存结构
}

pub enum Key<'a> {
    Version(&'a [u8], u64),       // (data_key, version)
    Active(u64),                   // version
    ActiveWrite(u64, &'a [u8]),   // (version, data_key)
    Snapshot(u64),                 // version
    NextVersion,                   // 单例
}
```

**键编码示例：**

```rust
// 数据版本键: [enum_variant(1B), version(8B BE), data_key...]
// 活跃事务键: [enum_variant(1B), version(8B BE)]
// 写集合键:   [enum_variant(1B), version(8B BE), data_key...]
```

**事务生命周期实现：**

```rust
impl<E: Engine> MVCC<E> {
    pub fn begin(&mut self, mode: TransactionMode) -> Result<u64> {
        let version = self.next_version()?;  // 原子递增 NextVersion
        // 写入 Key::Active(version) 标记事务开始
        // 如为 READ ONLY，写入 Key::Snapshot(version)
        Ok(version)
    }
    
    pub fn commit(&mut self, version: u64) -> Result<()> {
        // 1. 检查写冲突：遍历其他活跃事务的 ActiveWrite，与当前事务读集合比较
        // 2. 如无冲突，删除 Key::Active(version) 和 Key::ActiveWrite(...)
        // 3. 数据已写入 Key::Version(..., version)，无需额外操作
        Ok(())
    }
    
    pub fn rollback(&mut self, version: u64) -> Result<()> {
        // 1. 遍历所有 Key::ActiveWrite(version, key)
        // 2. 删除对应的 Key::Version(key, version)（tombstone）
        // 3. 删除 Key::Active(version) 和 Key::ActiveWrite(...)
        Ok(())
    }
}
```

**扫描可见性实现：**

MVCC 的 `scan_prefix` 会先获取底层 Engine 的所有匹配键，然后过滤：

```rust
fn filter_visible(&self, entries: ScanIterator, tx_version: Option<u64>) -> Result<ScanIterator> {
    // 无事务：取每个 key 的最新版本
    // 有事务：只取 version <= tx_version 的版本，且未被更新版本覆盖
}
```

### 3.4 Memory Engine（`src/storage/memory.rs`）

```rust
#[derive(Clone)]
pub struct Memory {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}
```

- 基于 `BTreeMap` 实现 `Engine` trait
- 所有操作持有 Mutex
- 用于单元测试，避免磁盘 IO

---

## 4. Types 模块实现细节

### 4.1 Value（`src/types/value.rs`）

**枚举定义：**

```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
```

**算术实现（溢出检查）：**

```rust
impl Value {
    pub fn checked_add(&self, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => {
                a.checked_add(*b).map(Value::Integer)
                    .ok_or_else(|| Error::InvalidData("integer overflow".into()))
            }
            (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            _ if self.is_null() || other.is_null() => Ok(Value::Null),
            _ => Err(errdata!("unsupported operand types for +")),
        }
    }
    // checked_sub, checked_mul, checked_div, checked_rem, checked_pow 类似
}
```

**三值逻辑：**

```rust
// 比较运算：涉及 Null → Null
Value::Null == Value::Null  →  NULL（在 SQL 语义中，不是 true）
Value::Null == Value::Integer(1) → NULL

// 逻辑运算
AND: true  AND NULL → NULL
     false AND NULL → false
     NULL  AND NULL → NULL

OR:  true  OR NULL  → true
     false OR NULL  → NULL
     NULL  OR NULL  → NULL

NOT: NOT NULL → NULL
```

**排序规则（Total Order）：**

```rust
// 按 kind 排序：Null < Boolean < Integer|Float < String
// Integer 和 Float 可交叉比较，通过 f64::total_cmp 实现
// String 按字典序
```

**Hash 处理（Float NaN/负零规范化）：**

```rust
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Float(f) => {
                // 规范化负零和 NaN 的符号位，保证 Hash 一致性
                let bits = if *f == 0.0 { 0u64 } else { f.to_bits() };
                bits.hash(state);
            }
            _ => { /* 标准 Hash */ }
        }
    }
}
```

### 4.2 Schema（`src/types/schema.rs`）

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub primary_key: usize,    // PK 列在 columns 中的索引
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}
```

`Table` 和 `Column` 均实现 `crate::utils::Value`（bincode 序列化），存储在 MVCC 中。

### 4.3 Label（`src/types/value.rs`）

```rust
pub enum Label {
    None,
    Unqualified(String),       // "name"
    Qualified(String, String), // ("users", "name") → 显示为 "users.name"
}
```

`as_header()` 提取裸列名用于结果表头输出。

---

## 5. Utils 模块实现细节

### 5.1 Key Coder（`src/utils/serde_utils/key_coder.rs`）

**整数编码（以 i64 为例）：**

```rust
// 原始值: 0x0000_0000_0000_0001 (+1)
// 大端序: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
// MSB 翻转: [0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
// 结果: 翻转后 0x800...01 > 0x7FF...FF，保证正数 > 负数

// 原始值: 0xFFFF_FFFF_FFFF_FFFF (-1)
// 大端序: [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
// MSB 翻转: [0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
// 结果: 翻转后 0x7FF...FF 是负数中最大的编码值
```

**浮点编码（IEEE-754，以 f64 为例）：**

```rust
// 正数: 符号位 0 → 翻转符号位为 1
//        编码后高位为 1，大于所有负数编码
// 负数: 符号位 1 → 整体按位取反
//        这样 -1.0 的编码 < -0.5 的编码（因为取反后大小关系反转）
```

**字符串编码：**

```rust
// 输入: "a\0b"
// 转义 0x00 → 0x00 0xFF
// 终止: 追加 0x00 0x00
// 结果: [0x61, 0x00, 0xFF, 0x62, 0x00, 0x00]
//
// 保序证明: "a" < "a\0" < "a\0b" < "ab"
// 编码后: [0x61, 0x00, 0x00] < [0x61, 0x00, 0xFF, 0x00, 0x00] < [0x61, 0x00, 0xFF, 0x62, ...] < [0x61, 0x62, ...]
```

**前缀扫描范围构造：**

```rust
pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let mut upper = prefix.to_vec();
    for i in (0..upper.len()).rev() {
        if upper[i] < 0xFF {
            upper[i] += 1;
            upper.truncate(i + 1);
            return (Bound::Included(prefix.to_vec()), Bound::Excluded(upper));
        }
    }
    // 所有字节都是 0xFF，上界为无界
    (Bound::Included(prefix.to_vec()), Bound::Unbounded)
}
```

### 5.2 Bin Coder（`src/utils/serde_utils/bin_coder.rs`）

简单的 `bincode` 封装：

```rust
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| Error::EncodeError(e.to_string()))
}

pub fn decode<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    let (value, _) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| Error::DecodeError(e.to_string()))?;
    Ok(value)
}
```

---

## 6. 配置系统实现细节

### 6.1 全局单例（`src/cfg/mod.rs`）

```rust
lazy_static! {
    pub static ref CONFIG: Mutex<Config> = Mutex::new(
        load_config().unwrap_or_else(|e| {
            panic!("Failed to load config: {}", e)
        })
    );
}
```

**辅助访问函数：**

```rust
pub fn get_db_base() -> String {
    let cfg = CONFIG.lock().unwrap();
    let mut path = cfg.storage_path.to_string_lossy().to_string();
    if !path.ends_with('/') {
        path.push('/');
    }
    path
}

pub fn get_max_size() -> u64 {
    let cfg = CONFIG.lock().unwrap();
    cfg.single_file_limit * 1024 * 1024 * 1024  // GiB → bytes
}
```

### 6.2 热重载（`src/cfg/watcher.rs`）

```rust
pub async fn watch_config(mut shutdown: broadcast::Receiver<()>) {
    tokio::task::spawn_blocking(move || {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(tx).unwrap();
        watcher.watch(config_path.parent().unwrap(), RecursiveMode::NonRecursive).unwrap();
        
        loop {
            match rx.recv() {
                Ok(Event { kind: EventKind::Modify(_), .. }) => {
                    if let Ok(new_config) = Config::load_config() {
                        let mut cfg = CONFIG.lock().unwrap();
                        // 字段级覆盖（storage_path 除外）
                        cfg.single_file_limit = new_config.single_file_limit;
                        cfg.sync_strategy = new_config.sync_strategy;
                        // ...
                    }
                }
                Ok(Event { kind: EventKind::Remove(_), .. }) => { /* 文件被删除，保持当前配置 */ }
                _ => {}
            }
        }
    });
}
```

### 6.3 测试覆盖（`src/cfg/mod.rs`）

```rust
#[cfg(test)]
pub fn override_config_for_test(config: Config) {
    let mut cfg = CONFIG.lock().unwrap();
    *cfg = config;
}

#[cfg(test)]
pub fn test_config_with_path(path: PathBuf) -> Config {
    Config::builder(path).build().unwrap()
}
```

BitCask 和 MVCC 的单元测试使用 `tempfile::TempDir` + `test_config_with_path` 将数据目录重定向到临时目录，保证测试隔离。

---

## 7. 错误处理实现细节

### 7.1 Error 枚举（`src/db_error.rs`）

```rust
#[derive(Debug)]
pub enum Error {
    Abort,
    InvalidData(String),
    ParserError(String),
    IO(std::io::Error),
    ReadOnly,
    Serialization,
    ConfigError(String),
    ConfigWatcherError(String),
    ServerError(String),
    EncodeError(String),
    DecodeError(String),
    SerializationError(String),
    DeserializationError(String),
    TryFromIntError,
    PoisonError(String),
    UnExpectedInput(String),
    ParseError(String),
}
```

### 7.2 特殊 From 实现

```rust
// 支持将 Error 直接转为 Result<T>（极少见但项目中有使用）
impl<T> From<Error> for Result<T> {
    fn from(err: Error) -> Result<T> {
        Err(err)
    }
}
```

### 7.3 便捷宏

```rust
#[macro_export]
macro_rules! errdata {
    ($($arg:tt)*) => {
        $crate::db_error::Error::InvalidData(format!($($arg)*))
    };
}

#[macro_export]
macro_rules! errinput {
    ($($arg:tt)*) => {
        $crate::db_error::Error::UnExpectedInput(format!($($arg)*))
    };
}
```

---

## 8. 测试策略

### 8.1 测试分类

| 类型 | 位置 | 覆盖内容 |
|------|------|---------|
| **单元测试** | 嵌入各 `*.rs` 文件（`#[cfg(test)]`） | 模块级功能 |
| **集成测试** | `tests/integration_test.rs` | 端到端 SQL 流程 |

### 8.2 关键单元测试模块

**BitCask（`src/storage/bitcask.rs`）：**
- CRUD 基本操作
- 扫描（scan / scan_prefix）
- 关闭后重新打开（持久化验证）
- Tombstone 删除
- Compaction 压缩
- 批量操作
- 文件轮转

**MVCC（`src/storage/mvcc.rs`）：**
- 事务隔离（读不阻塞写）
- 回滚清理
- 写冲突检测
- 扫描可见性（多版本过滤）
- 重启恢复

**执行器（`src/sql/execution/mod.rs`）：**
- CREATE TABLE / INSERT
- WHERE 过滤
- ORDER BY
- UPDATE / DELETE
- GROUP BY + 聚合函数

**表达式求值（嵌入执行器测试）：**
- 字面量求值
- 算术运算（含溢出）
- 比较运算（含 NULL）
- 三值逻辑

**类型系统（`src/types/value.rs`）：**
- 排序一致性
- 算术正确性
- Display 格式化

### 8.3 集成测试（`tests/integration_test.rs`）

```rust
#[tokio::test]
async fn test_sql_crud() {
    let dir = tempfile::tempdir().unwrap();
    let engine = BitCask::init_db_at(dir.path()).unwrap();
    let db = Database::new(engine);
    
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)").await.unwrap();
    db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").await.unwrap();
    
    let result = db.execute("SELECT * FROM users").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    // ... 完整 CRUD 验证
}

#[tokio::test]
async fn test_sql_order_by_and_group_by() {
    // ORDER BY、GROUP BY + COUNT(*) 验证
}

#[tokio::test]
async fn test_sql_persistence() {
    // 写入 → 关闭 → 重新打开 → 读取验证
}
```

### 8.4 测试隔离机制

1. **`tempfile::TempDir`**：每个测试使用独立的临时目录作为数据目录
2. **`override_config_for_test`**：重定向配置中的 `storage_path`
3. **`BitCask::init_db_at(path)`**：显式指定数据目录，避免与全局配置耦合

---

## 9. 数据持久化格式速查

### 9.1 表元数据存储键

```
Key:   [table_meta_prefix, table_name_bytes]
Value: bincode(Table { name, primary_key, columns })
```

### 9.2 表数据存储键

```
Key:   [table_data_prefix, table_name_bytes, pk_value_encoded]
Value: bincode(Row [Value, Value, ...])
```

### 9.3 BitCask 日志文件

```
Filename: <tsid>.log  (tsid = 创建时间戳)
Format:   [CRC(8B)][TSID(8B)][KeyLen(var)][Key][ValueLen(var)][Value] × N
```

### 9.4 MVCC 版本键

```
Key::Version(data_key, version):
    [0x00, version(8B BE), data_key...]

Key::Active(version):
    [0x01, version(8B BE)]

Key::ActiveWrite(version, data_key):
    [0x02, version(8B BE), data_key...]

Key::Snapshot(version):
    [0x03, version(8B BE)]

Key::NextVersion:
    [0x04]
```
