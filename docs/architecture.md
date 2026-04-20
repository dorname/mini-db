# mini-db 架构设计文档

> 本文档从宏观视角描述 mini-db 的系统架构、模块划分、组件关系与数据流，面向需要快速理解系统全貌的开发者与评审者。

---

## 1. 系统全景

mini-db 是一个用 Rust 从零实现的**嵌入式 SQL 数据库**，核心由两大子系统构成：

1. **SQL 引擎** —— 负责将 SQL 文本转换为查询结果
2. **存储引擎** —— 负责数据的持久化与事务管理

```
┌─────────────────────────────────────────────────────────────────────┐
│                          用户接口层                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │ HTTP Server │  │  CLI REPL   │  │   Exec      │  │  Library    │ │
│  │  (axum)     │  │ (interactive│  │  (single)   │  │   (Rust)    │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │
└─────────┼────────────────┼────────────────┼────────────────┼────────┘
          │                │                │                │
          └────────────────┴────────────────┴────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SQL 引擎 (src/sql/)                          │
│                                                                      │
│   SQL String → Lexer → Parser → Planner → Executor → ResultSet      │
│                                                                      │
│   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐                │
│   │ Lexer  │ → │ Parser │ → │Planner │ → │Executor│                │
│   │ Token  │   │  AST   │   │  Plan  │   │  Rows  │                │
│   └────────┘   └────────┘   └────────┘   └────────┘                │
│                                              │                      │
│                                              ▼                      │
│                                       ┌─────────────┐               │
│                                       │  Catalog    │               │
│                                       │ (table meta)│               │
│                                       └─────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       存储引擎 (src/storage/)                         │
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                      MVCC 事务层                              │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │   │
│   │  │ Snapshot    │  │ Write Conflict│  │ Versioned Keys      │  │   │
│   │  │ Isolation   │  │ Detection   │  │ (key + version)     │  │   │
│   │  └─────────────┘  └─────────────┘  └─────────────────────┘  │   │
│   └──────────────────────────────┬──────────────────────────────┘   │
│                                  │                                   │
│                                  ▼                                   │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                     BitCask 存储引擎                          │   │
│   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │   │
│   │  │ Append-Only │  │   KeyDir    │  │   Compaction        │  │   │
│   │  │   Log       │  │ (Mem Index) │  │ (Garbage Collect)   │  │   │
│   │  └─────────────┘  └─────────────┘  └─────────────────────┘  │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. 模块职责

### 2.1 `src/sql/` — SQL 引擎

SQL 引擎是数据库的"大脑"，负责理解用户意图并执行查询。

| 子模块 | 文件 | 职责 |
|--------|------|------|
| **Lexer** | `lexer.rs` | 将原始 SQL 字符串切分为 Token 流（关键字、标识符、字面量、运算符等） |
| **Parser** | `parser/mod.rs`, `parser/ast.rs` | 将 Token 流解析为抽象语法树（AST），包括 `Statement` 和 `Expression` |
| **Planner** | `planner/planner.rs` | 将 AST 转换为执行计划树（`Plan` / `Node`），进行语义校验（表/列存在性） |
| **Executor** | `execution/mod.rs` | 执行计划树，物化查询结果。包含 Scan、Filter、Join、Aggregate、Order、Limit 等算子 |

### 2.2 `src/storage/` — 存储引擎

存储引擎负责数据的可靠存储与高效访问。

| 子模块 | 文件 | 职责 |
|--------|------|------|
| **Engine Trait** | `engine.rs` | 定义存储引擎接口：`get`/`set`/`delete`/`scan`/`scan_prefix` 等 |
| **BitCask** | `bitcask.rs` | 日志结构化哈希表实现，提供持久化 KV 存储 |
| **MVCC** | `mvcc.rs` | 在 Engine 之上实现多版本并发控制与事务语义 |
| **Memory** | `memory.rs` | 内存版 Engine（基于 `BTreeMap`），用于测试 |

### 2.3 `src/types/` — 类型系统

定义 SQL 运行时的核心数据类型和表结构。

| 文件 | 职责 |
|------|------|
| `value.rs` | `Value` 枚举（Null / Boolean / Integer / Float / String）、算术/比较/三值逻辑、`Row`、`Label` |
| `schema.rs` | `Table` 和 `Column` 结构体，描述表元数据 |
| `expression.rs` | 重导出 `Expression` AST 节点 |

### 2.4 `src/utils/` — 工具模块

| 文件 | 职责 |
|------|------|
| `serde_utils/key_coder.rs` | 自定义**保序序列化**编码器，用于 B-tree 键的字节编码 |
| `serde_utils/bin_coder.rs` | 基于 `bincode` 的值序列化 |
| `format.rs` | 原始字节的调试格式化 |
| `timestamp.rs` | Unix 时间戳获取 |

### 2.5 `src/cfg/` — 配置系统

| 文件 | 职责 |
|------|------|
| `config.rs` | `Config` 结构体、Builder、加载逻辑 |
| `mod.rs` | 全局单例 `CONFIG`、辅助访问函数 |
| `watcher.rs` | 基于 `notify` 的文件热重载 |

### 2.6 `src/db_error.rs` — 错误系统

统一的 `Error` 枚举，覆盖从解析到存储的全栈错误，通过大量 `From` 实现支持 `?` 运算符传播。

---

## 3. 关键抽象与接口

### 3.1 `Engine` Trait

存储引擎的核心接口，定义在 `src/storage/engine.rs`：

```rust
pub trait Engine: Clone {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn delete(&mut self, key: Vec<u8>) -> Result<()>;
    fn scan(&self, range: Range) -> Result<ScanIterator>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<ScanIterator>;
}
```

所有存储操作均为**字节级 KV 接口**。上层（MVCC / SQL 引擎）负责键值语义。

### 3.2 `Database` Struct

`src/lib.rs` 中定义的会话封装：

```rust
pub struct Database {
    mvcc: Arc<Mutex<MVCC<BitCask>>>,
}

impl Database {
    pub fn new(engine: BitCask) -> Self;
    pub async fn execute(&self, sql: &str) -> Result<ResultSet>;
}
```

`Database::execute()` 是 SQL 请求的统一入口，内部串行执行：

```
Parser::parse(sql) → plan(&mvcc, &statement) → execute(&mvcc, &plan)
```

### 3.3 `Plan` / `Node`

执行计划树，定义在 `src/sql/planner/plan.rs`：

```rust
pub enum Node {
    Scan { table: String, alias: Option<String>, filter: Option<Expression> },
    NestedLoopJoin { left: Box<Node>, right: Box<Node>, ... },
    Projection { source: Box<Node>, expressions: Vec<(Expression, Option<String>)> },
    Filter { source: Box<Node>, predicate: Expression },
    Aggregate { source: Box<Node>, aggregates: Vec<Aggregate>, group_by: Vec<Expression> },
    Order { source: Box<Node>, orders: Vec<(Expression, OrderDirection)> },
    Limit { source: Box<Node>, limit: Option<Expression>, offset: Option<Expression> },
    Values { values: Vec<Vec<Expression>> },
    Insert { table: String, columns: Vec<String>, source: Box<Node> },
    Update { table: String, ... },
    Delete { table: String, source: Box<Node> },
    Explain(Box<Node>),
    Nothing,
}
```

Planner 将 AST 转换为自底向上的 `Node` 树，Executor 自底向上物化执行。

### 3.4 `Expression`

SQL 表达式 AST，定义在 `src/sql/parser/ast.rs`。支持：

- 字面量（整数、浮点、字符串、布尔、NULL）
- 列引用（`column` 或 `table.column`）
- 算术运算符（`+` `-` `*` `/` `%` `^` `!`）
- 比较运算符（`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `IS`, `LIKE`）
- 逻辑运算符（`AND`, `OR`, `NOT`）
- 标量函数（`ABS`, `UPPER`, `LOWER`）
- 聚合函数（`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`）

---

## 4. 数据流：一条 SELECT 语句的全栈之旅

以 `SELECT name, COUNT(*) FROM users WHERE age > 18 GROUP BY name ORDER BY COUNT(*) DESC LIMIT 10` 为例：

```
1. 用户接口层
   └─ HTTP Server / CLI / Library 接收 SQL 字符串

2. SQL 引擎 —— Lexer（src/sql/lexer.rs）
   └─ SQL 字符串 → Token 流
      [SELECT, Ident("name"), Comma, COUNT, LParen, Star, RParen, FROM, ...]

3. SQL 引擎 —— Parser（src/sql/parser/mod.rs）
   └─ Token 流 → AST（Statement::Select）
      Statement::Select { columns, from, where, group_by, having, order, limit }

4. SQL 引擎 —— Planner（src/sql/planner/planner.rs）
   └─ AST → Plan / Node 树
      Node::Limit
        └─ Node::Order
              └─ Node::Aggregate
                    └─ Node::Filter (age > 18)
                          └─ Node::Scan (table: "users")

5. SQL 引擎 —— Executor（src/sql/execution/mod.rs）
   └─ 自底向上执行 Node 树
      Scan: 从 MVCC 读取 "users" 表所有行
      Filter: 过滤 age > 18 的行
      Aggregate: 按 name 分组，计算 COUNT(*)
      Order: 按 COUNT(*) 降序排序
      Limit: 取前 10 条
      └─ ResultSet { labels: ["name", "COUNT(*)"], rows: [...] }

6. 用户接口层
   └─ 将 ResultSet 格式化为 HTTP JSON / CLI 表格 / 库返回值
```

---

## 5. 部署视图

mini-db 提供四种使用形态：

| 形态 | 启动方式 | 适用场景 |
|------|---------|---------|
| **HTTP 服务** | `cargo run -- server` 或默认 | 作为微服务的嵌入式数据库，通过 REST API 访问 |
| **交互式 CLI** | `cargo run -- cli` | 开发调试、手动数据操作 |
| **单次执行** | `cargo run -- exec "SQL"` | 脚本化操作、CI/CD |
| **库 API** | `mini_db::Database::new(engine)` | 嵌入到其他 Rust 应用中 |

所有形态共享同一套 `Database::execute()` 入口，底层复用 SQL 引擎与存储引擎。

---

## 6. 设计权衡

| 维度 | 选择 | 理由 |
|------|------|------|
| 解析器 | 手写递归下降 | 无外部依赖，完全可控，适合学习 |
| 执行模型 | 火山模型（迭代器） | 实现简单，内存友好，适合单线程 |
| 存储引擎 | BitCask（日志结构化） | 写放大低，恢复简单，适合嵌入式场景 |
| 事务 | MVCC + 乐观锁 | 读不阻塞写，实现相对简单 |
| 配置 | 全局单例 + 热重载 | 嵌入式场景下足够，使用便捷 |
| 并发 | `Arc<Mutex<Engine>>` | 实现简单，但限制了引擎级并行 |
