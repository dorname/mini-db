use crate::db_error::{Error, Result};
use crate::sql::execution::catalog::Catalog;
use crate::sql::execution::expr::{evaluate, Scope};
use crate::sql::parser::ast::{Direction, Expression, JoinType, Literal, Operator};
use crate::sql::planner::plan::{Aggregate, Node, Plan};
use crate::storage::engine::Engine;
use crate::storage::mvcc::MVCC;
use crate::types::{Label, Row, Table, Value};
use crate::utils::bin_coder;
use std::collections::HashMap;

/// 执行结果
#[derive(Debug)]
pub struct ResultSet {
    pub labels: Vec<Label>,
    pub rows: Vec<Row>,
}

impl ResultSet {
    pub fn empty() -> Self {
        Self { labels: vec![], rows: vec![] }
    }
}

/// 执行计划
pub fn execute<E: Engine>(mvcc: &MVCC<E>, plan: &Plan) -> Result<ResultSet> {
    match plan {
        Plan::CreateTable { schema } => {
            Catalog::set_table(mvcc, schema)?;
            Ok(ResultSet::empty())
        }
        Plan::DropTable { name, if_exists } => {
            match Catalog::get_table(mvcc, name)? {
                Some(_) => {
                    // 删除表的所有数据
                    delete_all_rows(mvcc, name)?;
                    Catalog::drop_table(mvcc, name)?;
                }
                None if !if_exists => {
                    return Err(Error::InvalidData(format!("table {} does not exist", name)));
                }
                _ => {}
            }
            Ok(ResultSet::empty())
        }
        Plan::Insert { table, column_map, source } => {
            let txn = mvcc.begin()?;
            let values = execute_node(mvcc, source, &[])?;
            for row in values.rows {
                // 如果有 column_map，需要重排/补全列
                let final_row = if let Some(ref map) = column_map {
                    let mut new_row = vec![Value::Null; table.columns.len()];
                    for (target_idx, source_idx) in map {
                        new_row[*target_idx] = row.get(*source_idx).cloned().unwrap_or(Value::Null);
                    }
                    new_row
                } else {
                    row
                };
                insert_row(&txn, table, &final_row)?;
            }
            txn.commit()?;
            Ok(ResultSet::empty())
        }
        Plan::Delete { table, source, .. } => {
            let txn = mvcc.begin()?;
            let schema = Catalog::get_table(mvcc, table)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", table)))?;
            let scope = table_scope(&schema);
            let result = execute_node(mvcc, source, &scope.labels)?;
            for row in result.rows {
                let pk = row.get(schema.primary_key).cloned().unwrap_or(Value::Null);
                let key = row_key(table, &pk);
                txn.delete(&key)?;
            }
            txn.commit()?;
            Ok(ResultSet::empty())
        }
        Plan::Update { table, expressions, source, .. } => {
            let txn = mvcc.begin()?;
            let scope = table_scope(table);
            let result = execute_node(mvcc, source, &scope.labels)?;
            for mut row in result.rows {
                for (col_idx, expr) in expressions {
                    let val = evaluate(expr, &row, &scope)?;
                    row[*col_idx] = val;
                }
                let pk = row.get(table.primary_key).cloned().unwrap_or(Value::Null);
                let key = row_key(&table.name, &pk);
                let val = bin_coder::encode(&row)?;
                txn.set(&key, Some(&val))?;
            }
            txn.commit()?;
            Ok(ResultSet::empty())
        }
        Plan::Select { root, labels } => {
            execute_node(mvcc, root, labels)
        }
    }
}

fn execute_node<E: Engine>(mvcc: &MVCC<E>, node: &Node, parent_labels: &[Label]) -> Result<ResultSet> {
    match node {
        Node::Empty => Ok(ResultSet::empty()),
        Node::Values { rows } => {
            let mut result_rows = Vec::with_capacity(rows.len());
            let scope = Scope::new(vec![]);
            for expr_row in rows {
                let mut row = Vec::with_capacity(expr_row.len());
                for expr in expr_row {
                    row.push(evaluate(expr, &Vec::new(), &scope)?);
                }
                result_rows.push(row);
            }
            // VALUES 节点的 label 由 parent 提供
            Ok(ResultSet { labels: parent_labels.to_vec(), rows: result_rows })
        }
        Node::Scan { table } => {
            let schema = Catalog::get_table(mvcc, table)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", table)))?;
            let txn = mvcc.begin_readonly()?;
            let prefix = table.as_bytes();
            let mut rows = Vec::new();
            let mut scan = txn.scan_prefix(prefix);
            while let Some((key, value)) = scan.next().transpose()? {
                // 跳过目录键
                if key.starts_with(b"__catalog__") {
                    continue;
                }
                let row: Row = bin_coder::decode(&value)?;
                rows.push(row);
            }
            let labels = schema
                .columns
                .iter()
                .map(|c| Label::Qualified(table.clone(), c.name.clone()))
                .collect();
            Ok(ResultSet { labels, rows })
        }
        Node::Filter { predicate, source } => {
            let mut result = execute_node(mvcc, source, parent_labels)?;
            let scope = Scope::new(result.labels.clone());
            result.rows.retain(|row| {
                match evaluate(predicate, row, &scope) {
                    Ok(val) => val.to_bool(),
                    Err(_) => false,
                }
            });
            Ok(result)
        }
        Node::Projection { expressions, source } => {
            let source_result = execute_node(mvcc, source, parent_labels)?;
            let scope = Scope::new(source_result.labels.clone());
            let mut new_labels = Vec::new();
            let mut rows = Vec::with_capacity(source_result.rows.len());

            // 处理 SELECT * 展开
            let mut flattened = Vec::new();
            for (expr, alias) in expressions {
                if let Expression::All = expr {
                    for (i, label) in source_result.labels.iter().enumerate() {
                        let col_expr = match label {
                            Label::Qualified(table, name) => Expression::Column(Some(table.clone()), name.clone()),
                            Label::Unqualified(name) => Expression::Column(None, name.clone()),
                            Label::None => continue,
                        };
                        flattened.push((col_expr, None));
                    }
                } else {
                    flattened.push((expr.clone(), alias.clone()));
                }
            }

            for (expr, alias) in &flattened {
                let label = match expr {
                    Expression::Column(table, name) => {
                        if let Some(t) = table {
                            Label::Qualified(t.clone(), alias.clone().unwrap_or_else(|| name.clone()))
                        } else {
                            Label::Unqualified(alias.clone().unwrap_or_else(|| name.clone()))
                        }
                    }
                    _ => Label::from(alias.clone()),
                };
                new_labels.push(label);
            }

            for row in source_result.rows {
                let mut new_row = Vec::with_capacity(flattened.len());
                for (expr, _) in &flattened {
                    new_row.push(evaluate(expr, &row, &scope)?);
                }
                rows.push(new_row);
            }
            Ok(ResultSet { labels: new_labels, rows })
        }
        Node::NestedLoopJoin { left, right, r#type, predicate } => {
            let left_result = execute_node(mvcc, left, parent_labels)?;
            let right_result = execute_node(mvcc, right, parent_labels)?;
            let left_scope = Scope::new(left_result.labels.clone());
            let right_scope = Scope::new(right_result.labels.clone());
            let joined_scope = Scope::join(&left_scope, &right_scope);

            let mut rows = Vec::new();
            let is_outer = r#type.is_outer();

            for left_row in &left_result.rows {
                let mut matched = false;
                for right_row in &right_result.rows {
                    let mut joined_row = left_row.clone();
                    joined_row.extend(right_row.clone());

                    let keep = if let Some(pred) = predicate {
                        match evaluate(pred, &joined_row, &joined_scope) {
                            Ok(val) => val.to_bool(),
                            Err(_) => false,
                        }
                    } else {
                        true
                    };

                    if keep {
                        matched = true;
                        rows.push(joined_row);
                    }
                }
                if is_outer && !matched {
                    let mut joined_row = left_row.clone();
                    for _ in 0..right_result.labels.len() {
                        joined_row.push(Value::Null);
                    }
                    rows.push(joined_row);
                }
            }

            let mut labels = left_result.labels.clone();
            labels.extend(right_result.labels.clone());
            Ok(ResultSet { labels, rows })
        }
        Node::Order { expressions, source } => {
            let mut result = execute_node(mvcc, source, parent_labels)?;
            let scope = Scope::new(result.labels.clone());
            let order_keys: Result<Vec<_>> = expressions
                .iter()
                .map(|(expr, dir)| {
                    let vals: Result<Vec<Value>> = result
                        .rows
                        .iter()
                        .map(|row| evaluate(expr, row, &scope))
                        .collect();
                    Ok((vals?, dir.clone()))
                })
                .collect();
            let order_keys = order_keys?;

            let mut indices: Vec<usize> = (0..result.rows.len()).collect();
            indices.sort_by(|a, b| {
                for (keys, dir) in &order_keys {
                    let cmp = keys[*a].cmp(&keys[*b]);
                    let ord = match dir {
                        Direction::Asc => cmp,
                        Direction::Desc => cmp.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            let sorted_rows: Vec<Row> = indices.into_iter().map(|i| result.rows[i].clone()).collect();
            result.rows = sorted_rows;
            Ok(result)
        }
        Node::Limit { offset, limit, source } => {
            let mut result = execute_node(mvcc, source, parent_labels)?;
            let scope = Scope::new(result.labels.clone());
            let off = if let Some(expr) = offset {
                match evaluate(expr, &Vec::new(), &scope)? {
                    Value::Integer(i) if i >= 0 => i as usize,
                    _ => return Err(Error::InvalidData("invalid offset".into())),
                }
            } else {
                0
            };
            let lim = if let Some(expr) = limit {
                match evaluate(expr, &Vec::new(), &scope)? {
                    Value::Integer(i) if i >= 0 => Some(i as usize),
                    _ => return Err(Error::InvalidData("invalid limit".into())),
                }
            } else {
                None
            };
            if off >= result.rows.len() {
                result.rows.clear();
            } else {
                result.rows = result.rows.split_off(off);
                if let Some(l) = lim {
                    if l < result.rows.len() {
                        result.rows.truncate(l);
                    }
                }
            }
            Ok(result)
        }
        Node::Aggregate { group_by, aggregates, source } => {
            let source_result = execute_node(mvcc, source, parent_labels)?;
            let source_scope = Scope::new(source_result.labels.clone());

            // 分组
            let mut groups: HashMap<Vec<Value>, Vec<Row>> = HashMap::new();
            for row in source_result.rows {
                let mut key = Vec::with_capacity(group_by.len());
                for expr in group_by {
                    key.push(evaluate(expr, &row, &source_scope)?);
                }
                groups.entry(key).or_default().push(row);
            }

            // 构建输出 labels
            let mut labels = Vec::with_capacity(group_by.len() + aggregates.len());
            for expr in group_by {
                let label = match expr {
                    Expression::Column(table, name) => {
                        if let Some(t) = table {
                            Label::Qualified(t.clone(), name.clone())
                        } else {
                            // 尝试从 source labels 中找到匹配的限定列名
                            let mut found = Label::Unqualified(name.clone());
                            for lbl in &source_result.labels {
                                if let Label::Qualified(_, lbl_name) = lbl {
                                    if lbl_name == name {
                                        found = lbl.clone();
                                        break;
                                    }
                                }
                            }
                            found
                        }
                    }
                    _ => Label::None,
                };
                labels.push(label);
            }
            for (agg, alias) in aggregates {
                let label = if let Some(a) = alias {
                    Label::Unqualified(a.clone())
                } else {
                    match agg {
                        Aggregate::Count(_) => Label::Unqualified("COUNT(*)".into()),
                        Aggregate::Sum(_) => Label::Unqualified("SUM".into()),
                        Aggregate::Avg(_) => Label::Unqualified("AVG".into()),
                        Aggregate::Min(_) => Label::Unqualified("MIN".into()),
                        Aggregate::Max(_) => Label::Unqualified("MAX".into()),
                    }
                };
                labels.push(label);
            }

            let mut rows = Vec::with_capacity(groups.len());
            for (group_key, group_rows) in groups {
                let mut out_row = group_key.clone();
                for (agg, _) in aggregates {
                    let val = compute_aggregate(agg, &group_rows, &source_scope)?;
                    out_row.push(val);
                }
                rows.push(out_row);
            }

            Ok(ResultSet { labels, rows })
        }
    }
}

fn compute_aggregate(agg: &Aggregate, rows: &[Row], scope: &Scope) -> Result<Value> {
    match agg {
        Aggregate::Count(expr) => {
            if expr.is_none() {
                return Ok(Value::Integer(rows.len() as i64));
            }
            let mut count = 0i64;
            for row in rows {
                let val = evaluate(expr.as_ref().unwrap(), row, scope)?;
                if !val.is_null() {
                    count += 1;
                }
            }
            Ok(Value::Integer(count))
        }
        Aggregate::Sum(expr) => {
            let mut sum = Value::Null;
            for row in rows {
                let val = evaluate(expr, row, scope)?;
                if !val.is_null() {
                    sum = if sum.is_null() { val } else { sum.checked_add(&val)? };
                }
            }
            Ok(sum)
        }
        Aggregate::Avg(expr) => {
            let mut sum = Value::Null;
            let mut count = 0i64;
            for row in rows {
                let val = evaluate(expr, row, scope)?;
                if !val.is_null() {
                    sum = if sum.is_null() { val } else { sum.checked_add(&val)? };
                    count += 1;
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                let sum_f: f64 = match sum {
                    Value::Integer(i) => i as f64,
                    Value::Float(f) => f,
                    _ => return Err(Error::InvalidData("AVG requires numeric".into())),
                };
                Ok(Value::Float(sum_f / count as f64))
            }
        }
        Aggregate::Min(expr) => {
            let mut min: Option<Value> = None;
            for row in rows {
                let val = evaluate(expr, row, scope)?;
                if !val.is_null() {
                    match &min {
                        None => min = Some(val),
                        Some(m) if val < *m => min = Some(val),
                        _ => {}
                    }
                }
            }
            Ok(min.unwrap_or(Value::Null))
        }
        Aggregate::Max(expr) => {
            let mut max: Option<Value> = None;
            for row in rows {
                let val = evaluate(expr, row, scope)?;
                if !val.is_null() {
                    match &max {
                        None => max = Some(val),
                        Some(m) if val > *m => max = Some(val),
                        _ => {}
                    }
                }
            }
            Ok(max.unwrap_or(Value::Null))
        }
    }
}

fn table_scope(table: &Table) -> Scope {
    let labels = table
        .columns
        .iter()
        .map(|c| Label::Qualified(table.name.clone(), c.name.clone()))
        .collect();
    Scope::new(labels)
}

fn row_key(table: &str, pk: &Value) -> Vec<u8> {
    let pk_bytes = encode_pk(pk);
    [table.as_bytes(), b"\x00", &pk_bytes].concat()
}

fn encode_pk(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![0x00],
        Value::Boolean(false) => vec![0x01, 0x00],
        Value::Boolean(true) => vec![0x01, 0x01],
        Value::Integer(i) => {
            let mut buf = vec![0x02];
            buf.extend_from_slice(&i.to_be_bytes());
            buf
        }
        Value::Float(f) => {
            let mut buf = vec![0x03];
            buf.extend_from_slice(&f.to_be_bytes());
            buf
        }
        Value::String(s) => {
            let mut buf = vec![0x04];
            buf.extend_from_slice(s.as_bytes());
            buf
        }
    }
}

fn insert_row<E: Engine>(txn: &crate::storage::mvcc::Transaction<E>, table: &Table, row: &Row) -> Result<()> {
    let pk = row.get(table.primary_key).cloned().unwrap_or(Value::Null);
    let key = row_key(&table.name, &pk);
    let val = bin_coder::encode(row)?;
    txn.set(&key, Some(&val))
}

fn delete_all_rows<E: Engine>(mvcc: &MVCC<E>, table: &str) -> Result<()> {
    let txn = mvcc.begin()?;
    let mut scan = txn.scan_prefix(table.as_bytes());
    while let Some((key, _)) = scan.next().transpose()? {
        if key.starts_with(b"__catalog__") {
            continue;
        }
        txn.delete(&key)?;
    }
    txn.commit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::Memory;
    use crate::types::{DataType, Value};

    fn create_test_mvcc() -> MVCC<Memory> {
        MVCC::new(Memory::default())
    }

    fn exec(mvcc: &MVCC<Memory>, sql: &str) -> ResultSet {
        let stmt = crate::sql::parser::Parser::pasre(sql).unwrap();
        let plan = crate::sql::planner::planner::plan(mvcc, &stmt).unwrap();
        execute(mvcc, &plan).unwrap()
    }

    #[test]
    fn test_create_and_insert() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)");
        exec(&mvcc, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
        let result = exec(&mvcc, "SELECT * FROM users");
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[0][1], Value::String("alice".into()));
    }

    #[test]
    fn test_where_filter() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)");
        exec(&mvcc, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
        let result = exec(&mvcc, "SELECT * FROM users WHERE id > 1");
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_order_by() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)");
        exec(&mvcc, "INSERT INTO users VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob')");
        let result = exec(&mvcc, "SELECT * FROM users ORDER BY id");
        assert_eq!(result.rows[0][0], Value::Integer(1));
        assert_eq!(result.rows[1][0], Value::Integer(2));
        assert_eq!(result.rows[2][0], Value::Integer(3));
    }

    #[test]
    fn test_update() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)");
        exec(&mvcc, "INSERT INTO users VALUES (1, 'alice')");
        exec(&mvcc, "UPDATE users SET name = 'alex' WHERE id = 1");
        let result = exec(&mvcc, "SELECT * FROM users WHERE id = 1");
        assert_eq!(result.rows[0][1], Value::String("alex".into()));
    }

    #[test]
    fn test_delete() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)");
        exec(&mvcc, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
        exec(&mvcc, "DELETE FROM users WHERE id = 1");
        let result = exec(&mvcc, "SELECT * FROM users");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Integer(2));
    }

    #[test]
    fn test_group_by() {
        let mvcc = create_test_mvcc();
        exec(&mvcc, "CREATE TABLE orders (id INTEGER PRIMARY KEY, category STRING, amount INTEGER)");
        exec(&mvcc, "INSERT INTO orders VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)");
        let result = exec(&mvcc, "SELECT category, COUNT(*) FROM orders GROUP BY category");
        assert_eq!(result.rows.len(), 2);
    }
}
