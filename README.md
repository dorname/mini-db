# mini-db

一个用 **Rust** 从零实现的嵌入式 SQL 数据库。包含自定义 SQL 解析器、查询计划器、执行引擎、MVCC 事务层，以及基于 BitCask 的日志结构化存储引擎。

```
SQL String → Lexer → Parser → Planner → Executor → ResultSet
                                      ↓
                              MVCC Transaction Layer
                                      ↓
                              BitCask Storage Engine
```

## 功能特性

### SQL 支持

| 类别 | 支持的功能 |
|------|-----------|
| **DDL** | `CREATE TABLE`（含 PRIMARY KEY / NOT NULL / UNIQUE / INDEX / REFERENCES / DEFAULT 约束语法）、`DROP TABLE [IF EXISTS]` |
| **DML** | `INSERT INTO`（多行 VALUES、可选列列表）、`UPDATE ... SET ... WHERE`、 `DELETE FROM ... WHERE` |
| **查询** | `SELECT * / 列 / 表达式 / 别名`、`FROM`（表别名）、`JOIN`（CROSS / INNER / LEFT / RIGHT）、`WHERE`、`GROUP BY`、`HAVING`、`ORDER BY ASC/DESC`、`LIMIT / OFFSET` |
| **聚合** | `COUNT(*)`、`COUNT(expr)`、`SUM`、`AVG`、`MIN`、`MAX` |
| **表达式** | 整数 / 浮点 / 字符串 / 布尔 / NULL 字面量、`+ - * / % ^ !`、比较（`= != <> < <= > >= IS LIKE`）、逻辑（`AND OR NOT`）、标量函数（`ABS` / `UPPER` / `LOWER`） |
| **事务** | `BEGIN [TRANSACTION] [READ ONLY / READ WRITE] [AS OF SYSTEM TIME ...]`、`COMMIT`、`ROLLBACK`，基于 MVCC 的快照隔离 + 乐观写冲突检测 |

### 存储引擎

- **BitCask**：日志结构化哈希表，追加写 + 内存索引（`KeyDir`），支持文件轮转、数据压缩（Compaction）、SHA3-256 完整性校验
- **MVCC**：在存储引擎之上实现多版本并发控制，支持快照隔离读、写冲突检测、墓碑删除

### 数据类型

| SQL 类型 | 说明 |
|---------|------|
| `BOOLEAN` / `BOOL` | 布尔值 `TRUE` / `FALSE` |
| `INTEGER` / `INT` | 64 位有符号整数 |
| `FLOAT` / `DOUBLE` | 64 位浮点数（支持 `NaN`、`Infinity`） |
| `STRING` / `TEXT` / `VARCHAR` | 变长字符串 |
| `NULL` | 空值，支持三值逻辑 |

---

## 快速开始

### 构建

```bash
cargo build --release
```

### 运行 HTTP 服务

```bash
cargo run -- server
# 或默认（不传子命令即为 server）
cargo run
```

服务监听 `127.0.0.1:6666`。

### 交互式 SQL Shell

```bash
cargo run -- cli
```

```
mini-db interactive SQL shell
Type 'exit' or 'quit' to leave.

mini-db> CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING);
mini-db> INSERT INTO users VALUES (1, 'alice'), (2, 'bob');
mini-db> SELECT * FROM users;
+----+-------+
| id | name  |
+----+-------+
|  1 | alice |
|  2 | bob   |
+----+-------+
(2 rows)
mini-db> exit
Bye!
```

### 单次执行 SQL

```bash
cargo run -- exec "SELECT * FROM users"
```

---

## HTTP API

### 执行 SQL

```http
POST / HTTP/1.1
Content-Type: application/json

{"sql": "SELECT * FROM users WHERE id = 1"}
```

**响应示例：**

```json
{
  "success": true,
  "labels": ["id", "name"],
  "rows": [[1, "alice"]],
  "error": null
}
```

**curl 示例：**

```bash
curl -X POST http://127.0.0.1:6666/ \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

---

## SQL 示例

### 建表与插入

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name STRING NOT NULL,
    price FLOAT DEFAULT 0.0,
    category STRING
);

INSERT INTO products (id, name, price, category) VALUES
    (1, 'Apple', 5.5, 'Fruit'),
    (2, 'Banana', 3.0, 'Fruit'),
    (3, 'Carrot', 2.5, 'Vegetable');
```

### 查询

```sql
-- 简单查询
SELECT * FROM products WHERE price > 3.0;

-- JOIN
SELECT p.name, p.price, p.category
FROM products AS p
INNER JOIN categories AS c ON p.category = c.name;

-- 聚合 + 分组 + 排序 + 分页
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM products
GROUP BY category
HAVING COUNT(*) >= 2
ORDER BY avg_price DESC
LIMIT 10 OFFSET 0;
```

### 更新与删除

```sql
UPDATE products SET price = 4.5 WHERE name = 'Banana';
DELETE FROM products WHERE category = 'Vegetable';
```

### 事务

```sql
BEGIN TRANSACTION READ WRITE;
INSERT INTO products VALUES (4, 'Durian', 15.0, 'Fruit');
UPDATE products SET price = price * 0.9 WHERE category = 'Fruit';
COMMIT;
```

---

## 配置

配置文件默认为 `config.toml`，支持以下选项：

```toml
[config]
storage_path = "./db/"          # 数据存储目录
single_file_limit = 1            # 单个活跃文件大小上限（GiB）
sync_strategy = "Never"          # 同步策略：Always / Every / Never
fsync_inteval_ms = 1000          # "Every" 策略下的同步间隔
compaction_threshold = 0.6       # 垃圾数据比例阈值，触发 Compaction
file_cache_capacity = 32         # 旧文件句柄 LRU 缓存容量
```

配置加载优先级（从高到低）：
1. 环境变量 `MINI_DB_CONFIG_PATH`
2. 可执行文件所在目录
3. 当前工作目录
4. 内置默认值

配置文件支持**热重载**——修改后无需重启服务即可生效。

---

## 开发

### 运行测试

```bash
cargo test
```

包含单元测试（BitCask、MVCC、执行器、表达式求值、类型系统）和集成测试（完整 CRUD、ORDER BY / GROUP BY、持久化重启验证）。

### 作为库使用

```rust
use mini_db::{BitCask, Database};

#[tokio::main]
async fn main() -> mini_db::db_error::Result<()> {
    let engine = BitCask::init_db()?;
    let db = Database::new(engine);
    let result = db.execute("SELECT * FROM users").await?;
    // ...
    Ok(())
}
```

---

## 已知限制

- **无 B-Tree 索引**：目前所有查询均为全表扫描，`UNIQUE` / `INDEX` 约束仅解析未强制执行
- **无查询优化器**：执行计划为朴素实现（如嵌套循环 JOIN）
- **无子查询 / UNION**：解析器支持但计划器/执行器暂未实现
- **无 ALTER TABLE**
- **单线程引擎并发**：通过 `Arc<Mutex<Engine>>` 保护

---

## SQL 运算符优先级和结合性速查表

| 优先级 (高 → 低) | 运算符 | 说明 | 结合性 |
|:---:|---|---|:---|
| 1 | `()` | 括号，改变优先级 | - |
| 2 | `.` | 表.列 访问 | 左结合 |
| 3 | `::` (Postgres) / `CAST(expr AS type)` | 类型转换 | 左结合 |
| 4 | `-`（单目负号）、`~`（按位取反）、`NOT` | 一元运算符 | 右结合 |
| 5 | `^` | 幂运算 | **右结合** |
| 6 | `*` `/` `%` | 乘、除、取模 | 左结合 |
| 7 | `+` `-` | 加、减 | 左结合 |
| 8 | `\|\|` | 字符串拼接（PostgreSQL / Oracle） | 左结合 |
| 9 | `=` `<=>` `<>` `!=` `<` `<=` `>` `>=` `LIKE` `ILIKE` `IN` `BETWEEN` `IS NULL` `IS NOT NULL` | 比较运算 | 无结合性 |
| 10 | `AND` | 逻辑与 | 左结合 |
| 11 | `OR` | 逻辑或 | 左结合 |
| 12 | `CASE ... WHEN ... THEN ... ELSE ... END` | 条件表达式 | - |
| 13 | `=` `:=` | 赋值 | **右结合** |
