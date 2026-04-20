# mini-db 技术设计文档

> 本文档深入阐述各模块的技术选型、核心算法、数据结构与设计权衡，面向需要理解"为什么这样设计"的开发者。

---

## 1. SQL 解析

### 1.1 整体方案

采用**手写递归下降解析器**（Recursive Descent Parser），表达式部分使用**优先级爬升**（Precedence Climbing / Top-Down Operator Precedence）算法。

**不选用生成器（如 nom、lalrpop）的原因：**
- 学习目的：从零实现 parser 有助于深入理解 SQL 语法结构
- 零外部依赖：除标准库外无需引入 parser combinator 框架
- 完全可控：错误信息、AST 结构、扩展性一手掌握

### 1.2 Lexer 设计

`src/sql/lexer.rs` 将输入字符串转换为 `Token` 流。

**Token 分类：**

| 类别 | 示例 |
|------|------|
| 关键字 | `SELECT`, `FROM`, `WHERE`, `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`, `AND`, `OR`, `NOT`, `NULL`, `TRUE`, `FALSE`, `AS`, `JOIN`, `INNER`, `LEFT`, `RIGHT`, `CROSS`, `ON`, `GROUP`, `BY`, `HAVING`, `ORDER`, `LIMIT`, `OFFSET`, `EXPLAIN`, `TRANSACTION`, `READ`, `WRITE`, `ONLY`, `IS`, `LIKE`, `IN`, `BETWEEN`, `CASE`, `WHEN`, `THEN`, `ELSE`, `END`, `EXISTS`, `UNIQUE`, `INDEX`, `REFERENCES`, `DEFAULT`, `PRIMARY`, `KEY`, `DROP`, `TABLE`, `IF`, `EXISTS`, `INT`, `INTEGER`, `FLOAT`, `DOUBLE`, `STRING`, `TEXT`, `VARCHAR`, `BOOLEAN`, `BOOL` |
| 标识符 | `users`, `id`, `name`（区分大小写） |
| 字面量 | 整数 `123`、浮点 `3.14`、字符串 `'hello'`、布尔 `TRUE`/`FALSE` |
| 运算符 | `+`, `-`, `*`, `/`, `%`, `^`, `!`, `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `.`, `,`, `(`, `)`, `;` |
| 特殊 | `Null`, `EOF` |

**关键字处理：** 使用 `Keyword` 枚举统一管理，Lexer 在识别标识符时先查关键字表，非关键字再作为普通标识符返回。

### 1.3 Parser 设计

**语法层次结构：**

```
Statement
├── DDL: CreateTable, DropTable
├── DML: Insert, Update, Delete
├── DQL: Select
└── TCL: Begin, Commit, Rollback, Explain

Expression (优先级从低到高)
├── OR
├── AND
├── IS, LIKE, IN, BETWEEN
├── =, !=, <>, <, <=, >, >=
├── +, -, ||
├── *, /, %
├── ^, !
├── Unary: -, ~, NOT
├── Postfix: .column
└── Primary: literal, identifier, (expr), function_call
```

**优先级爬升算法：** 每个二元运算符有左结合性/右结合性 + 优先级数值。Parser 根据当前 Token 的优先级决定是"吸收"右侧表达式还是返回给上层。

### 1.4 AST 设计

`src/sql/parser/ast.rs` 定义核心 AST 节点。

**`Statement` 枚举**涵盖所有支持的 SQL 语句类型，每种变体携带必要的子结构（如 `Select` 携带 `columns`、`from`、`where`、`group_by`、`having`、`order`、`limit`、`offset`）。

**`Expression` 枚举**涵盖所有表达式类型：

```rust
pub enum Expression {
    Literal(Value),
    Field(Option<String>, String),        // (table, column)
    Operator(Operator, Vec<Expression>),  // 一元/二元/多元运算符
    Function(String, Vec<Expression>),    // 标量/聚合函数
}
```

`Operator` 枚举统一封装所有运算符语义，便于 Planner 和 Executor 统一处理。

---

## 2. 查询计划

### 2.1 朴素计划生成

Planner **没有查询优化器**，直接按 SQL 语法顺序生成执行计划。

**计划生成顺序（从底向上）：**

```
FROM / JOIN → WHERE → GROUP BY → HAVING → SELECT (Projection) → ORDER BY → LIMIT/OFFSET
```

对应 `Node` 树结构：

```
Node::Limit
  └─ Node::Order
        └─ Node::Projection
              └─ Node::Aggregate
                    └─ Node::Filter
                          └─ Node::NestedLoopJoin
                                ├─ Node::Scan (left table)
                                └─ Node::Scan (right table)
```

### 2.2 语义校验

Planner 通过 `Catalog`（从 MVCC 读取表元数据）进行基础语义检查：

- 表是否存在
- 列是否存在于表中
- 表达式引用的列在当前作用域内是否可见（处理别名和 JOIN 后的列歧义）

### 2.3 设计权衡

| 方案 | 优点 | 缺点 | 本项目选择 |
|------|------|------|---------|
| 朴素计划 | 实现简单、无状态、易调试 | 全表扫描、JOIN 顺序固定、无下推 | ✅ 当前 |
| 基于代价的优化器（CBO） | 高效执行计划 | 实现复杂、需要统计信息 | ❌ 未来 |
| 基于规则的优化器（RBO） | 中等复杂度 | 需要维护规则集 | ❌ 未来 |

---

## 3. 执行引擎

### 3.1 火山模型（Iterator Model）

执行器采用经典的**火山模型**（Volcano / Iterator Model）：每个算子实现统一的 "next row" 接口，上层算子拉取下层算子的数据。

在本项目中的简化体现：每个 `Node` 被 `execute_node()` 处理后返回一个 `Rows`（`Box<dyn RowIterator>`），上层节点消费这个迭代器。

### 3.2 算子设计

| 算子 | 职责 | 实现要点 |
|------|------|---------|
| **Scan** | 从底层存储读取表数据 | 使用 MVCC `scan_prefix` 读取特定表前缀的所有键值对，反序列化为 `Row` |
| **Filter** | 按 WHERE 条件过滤 | 对每行求值 `Expression`，`to_bool()` 为真则保留 |
| **Projection** | SELECT 列裁剪/表达式求值 | 对每行求值投影表达式，生成新行 |
| **NestedLoopJoin** | 表连接 | 双重循环遍历左右子树结果。支持 CROSS / INNER / LEFT / RIGHT |
| **Aggregate** | 分组聚合 | 用 `HashMap` 缓存分组键 → 聚合状态，最后输出 |
| **Order** | 排序 | 物化全部行后按 ORDER BY 键排序 |
| **Limit** | 分页 | 跳过 offset 行，取 limit 行 |
| **Insert** | 插入数据 | 从 VALUES 算子或子查询获取行，写入 MVCC |
| **Update** | 更新数据 | 先 Scan 定位行，修改后写回 |
| **Delete** | 删除数据 | 先 Scan 定位行，写 tombstone |

### 3.3 表达式求值

表达式求值在 `execution/mod.rs` 中实现，核心函数对每行上下文求值：

- 字面量：直接返回
- 列引用：从当前行的字段位置取值
- 运算符：递归求值子表达式后应用运算
- 函数：递归求值参数后调用函数

**三值逻辑（Three-Valued Logic）：** 涉及 `NULL` 的比较运算返回 `NULL`，`AND`/`OR`/`NOT` 遵循 SQL 标准真值表。

---

## 4. 存储引擎（BitCask）

### 4.1 BitCask 核心原理

BitCask 是一种**日志结构化哈希表**（Log-Structured Hash Table），核心思想：

1. **追加写（Append-Only）**：所有写操作（set/delete）只追加到当前活跃日志文件末尾
2. **内存索引（KeyDir）**：内存中的 `BTreeMap<key, (file_id, value_pos, value_len)>` 记录每个键的最新位置
3. **读操作**：通过 KeyDir 定位到文件偏移，直接读取

**优势：** 写操作全是顺序 IO，性能极高；读操作是一次随机 IO（通过 KeyDir 定位）。

### 4.2 日志文件格式

每个日志条目（Log Entry）：

```
┌──────────┬──────────┬──────────────┬──────────┬────────────┐
│ CRC (8B) │ TSID (8B)│ KeyLen (var) │ Key      │ ValueLen   │
│ (sha3)   │          │              │          │ (var)      │
└──────────┴──────────┴──────────────┴──────────┴────────────┘
```

- **CRC**：SHA3-256 的前 8 字节，用于数据完整性校验
- **TSID**：时间戳 ID，唯一标识一条写入记录
- **KeyLen / ValueLen**：变长整数编码（根据大小选择 u8/u16/u32/u64）
- **Key / Value**：原始字节

### 4.3 文件管理

```
data/
├── active/           # 当前活跃写入文件
│   └── <tsid>.log    # 正在接收追加写的日志文件
├── <file_id>.log     # 已关闭的历史日志文件
└── ...
```

**文件轮转（Rotation）：** 当活跃文件大小超过 `single_file_limit`（配置项，单位 GiB）时：
1. 关闭当前活跃文件
2. 创建新的活跃文件（以当前时间戳命名）
3. 更新 KeyDir（无需修改，因为 KeyDir 只存 file_id，而活跃文件的 file_id 在关闭时确定）

### 4.4 Compaction（数据压缩）

随着写入进行，历史文件中会积累大量"垃圾"（被覆盖或删除的键的旧版本）。Compaction 负责清理。

**触发条件：** 垃圾数据比例超过 `compaction_threshold`（配置项，默认 0.6）。

**Compaction 流程：**
1. 遍历所有历史文件（不包括活跃文件）
2. 对每个键，检查 KeyDir 中记录的位置是否在当前文件中
3. 只保留 KeyDir 中仍然指向该文件的键值对（即最新版本）
4. 将存活数据写入新的紧凑化文件
5. 删除旧的历史文件
6. 更新 KeyDir 指向新的文件位置

### 4.5 设计权衡

| 维度 | BitCask | B-Tree 页式存储（对比） |
|------|---------|---------------------|
| 写性能 | 顺序 IO，极高 | 随机 IO，需要页分裂 |
| 读性能 | 一次随机 IO（KeyDir 定位） | 从根节点遍历，多次随机 IO |
| 内存占用 | 必须容纳全部 KeyDir | 只需缓存部分页 |
| 启动时间 | 需要全量扫描重建 KeyDir | 较短（只需加载少量元页） |
| 范围查询 | 差（KeyDir 是哈希结构） | 优（B-Tree 天然有序） |
| 本项目选择 | ✅ | ❌ |

本项目选择 BitCask 的原因：实现简单、写入性能优秀、恢复逻辑直观，非常适合学习和小型嵌入式场景。

---

## 5. MVCC（多版本并发控制）

### 5.1 版本化键空间

MVCC 层在 Engine 的 KV 接口之上构建事务语义。所有键都带有版本信息，通过键前缀区分类型：

| 键类型 | 编码格式 | 用途 |
|--------|---------|------|
| `Key::Version(key, version)` | 数据键 + 版本号 | 存储数据在特定版本下的值 |
| `Key::Active(version)` | 活跃事务标记 | 标识一个正在进行的事务 |
| `Key::ActiveWrite(version, key)` | 活跃事务的写集合 | 记录某事务写过的键，用于冲突检测 |
| `Key::Snapshot(version)` | 快照元数据 | 记录只读事务的快照信息 |
| `Key::NextVersion` | 单例键 | 全局版本号计数器 |

### 5.2 事务状态机

```
         BEGIN
           │
           ▼
      ┌─────────┐
      │ ACTIVE  │◄──── 读写操作在此状态执行
      └────┬────┘
           │
     ┌─────┴─────┐
     ▼           ▼
  COMMIT     ROLLBACK
     │           │
     ▼           ▼
  持久化       清理版本键
  新版本       （tombstone）
```

### 5.3 隔离级别

**只读事务（READ ONLY）：**
- 获取事务开始时全局最新版本号作为快照版本
- 读取时只读 `version <= 快照版本` 的数据
- 不受并发写入影响（快照隔离）

**读写事务（READ WRITE）：**
- 获取新版本号
- 写入时创建 `Key::Version(key, version)`
- 同时写入 `Key::ActiveWrite(version, key)` 记录写集合
- **提交时（COMMIT）：** 检查是否有其他活跃事务的写集合与当前事务的读集合冲突。若有冲突 → 返回写冲突错误（乐观并发控制）
- **回滚时（ROLLBACK）：** 清理所有 `Key::Version(..., version)` 和 `Key::ActiveWrite(...)`

### 5.4 扫描可见性

MVCC 的 `scan` 和 `scan_prefix` 操作需要过滤不可见的版本：

- 对于**无事务上下文**的扫描：读取最新版本的数据
- 对于**有事务上下文**的扫描：
  - 只读事务：只读 `version <= 快照版本` 的数据，且该版本未被更新事务覆盖
  - 读写事务：可读到自身写入的最新数据

### 5.5 设计权衡

| 方案 | 隔离级别 | 实现复杂度 | 冲突处理 | 本项目选择 |
|------|---------|----------|---------|---------|
| MVCC + 乐观锁 | 快照隔离 | 中等 | 提交时检测冲突，失败则报错 | ✅ 当前 |
| 两阶段锁（2PL） | 可串行化 | 较高 | 获取锁时可能阻塞 | ❌ |
| 时间戳排序（TO） | 可串行化 | 中等 | 读写时检测冲突 | ❌ |

选择乐观 MVCC 的原因：读操作从不阻塞，实现相对简单，适合嵌入式单实例场景。

---

## 6. 序列化

### 6.1 值的序列化：bincode

所有表结构（`Table`、`Column`）和行数据（`Row`）使用 **bincode** 进行序列化。

**选择 bincode 的原因：**
- 紧凑的二进制格式
- 与 serde 生态无缝集成
- 速度极快（零拷贝友好的紧凑编码）
- 小端序（默认），与大多数现代 CPU 一致

**使用方式：**
```rust
// 编码
let bytes = bincode::encode_to_vec(&value, bincode::config::standard())?;

// 解码
let (value, _) = bincode::decode_from_slice(&bytes, bincode::config::standard())?;
```

### 6.2 键的序列化：自定义 Order-Preserving 编码

Engine 的键需要支持**范围扫描**和**前缀扫描**，因此序列化后必须**保序**（lexicographic order 与逻辑 order 一致）。

本项目实现了自定义的 `KeyEncoder` / `KeyDecoder`，位于 `src/utils/serde_utils/key_coder.rs`。

**各类型编码规则：**

| 类型 | 编码规则 | 保序原理 |
|------|---------|---------|
| `bool` | `0` / `1` | 直接映射 |
| `i8`–`i64` | 大端序 + **MSB 翻转** | 翻转符号位后，负数 < 正数 |
| `u8`–`u64` | 大端序 | 自然保序 |
| `f32`/`f64` | IEEE-754 大端序 + **符号位处理** | 正数符号位翻转；负数整体按位取反 |
| `String`/`[u8]` | null-terminated + `0x00` escape | 字节序 = 字典序 |
| enum | variant_index 单字节 | 声明顺序 = 编码顺序 |

**字符串编码细节：**
- 终止符：`0x00 0x00`
- 字节 `0x00` 转义为 `0x00 0xFF`
- 这样可保证 `"a" < "ab" < "b"` 的字典序关系在编码后仍然成立

**前缀扫描：** `prefix_range(prefix: &[u8])` 函数构造一个 `Bound` 范围，用于 BTreeMap 的 `range()` 操作，实现前缀匹配。

---

## 7. 配置系统

### 7.1 配置加载优先级

```
1. 环境变量 MINI_DB_CONFIG_PATH（最高优先级，开发/运维覆盖）
2. 可执行文件所在目录的 config.toml（部署时优先）
3. 当前工作目录的 config.toml（本地开发）
4. 内置默认值（兜底）
```

### 7.2 热重载

使用 `notify` crate 监听配置文件变化：

1. 在独立线程（`tokio::spawn_blocking`）中运行文件监听器
2. 检测到 `Modify` 事件后，重新解析 TOML
3. 字段级覆盖全局 `CONFIG`（`Mutex<Config>`）
4. **`storage_path` 被排除在热重载外** —— 运行时移动数据目录不安全

### 7.3 设计权衡

| 方案 | 优点 | 缺点 | 选择 |
|------|------|------|------|
| 全局单例 | 访问方便、任意位置可读 | 需要锁、测试时需要 mock | ✅ 当前 |
| 依赖注入 | 测试友好、无全局状态 | 每层都需要传递 Config 引用 | ❌ |
| 热重载 | 无需重启服务 | 瞬时状态不一致 | ✅ 当前（排除 storage_path）|

---

## 8. 错误处理

### 8.1 统一错误类型

整个项目使用单一的 `Error` 枚举（`src/db_error.rs`），包含 17 个变体：

```rust
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

### 8.2 `?` 运算符支持

通过大量 `From` 实现，使外部错误可无缝转换：

```rust
impl From<std::io::Error> for Error { ... }
impl From<toml::de::Error> for Error { ... }
impl From<notify::Error> for Error { ... }
impl From<axum::Error> for Error { ... }
impl From<bincode::error::EncodeError> for Error { ... }
impl From<bincode::error::DecodeError> for Error { ... }
// ...
```

### 8.3 便捷宏

```rust
errdata!("unexpected value {}", x)   // → Error::InvalidData(...)
errinput!("expected integer")         // → Error::UnExpectedInput(...)
```

### 8.4 设计权衡

| 方案 | 优点 | 缺点 | 选择 |
|------|------|------|------|
| 单一 Error 枚举 | `?` 统一、无类型爆炸 | 粒度较粗 | ✅ 当前 |
| 每层独立 Error | 精确、类型安全 | 转换繁琐、类型爆炸 | ❌ |
| thiserror | 自动生成 From/Display | 额外依赖 | ❌（手写实现）|
