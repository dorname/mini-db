use crate::db_error::{Error, Result};
use crate::sql::execution::catalog::Catalog;
use crate::sql::parser::ast;
use crate::sql::parser::ast::{Expression, From, JoinType, Statement};
use crate::sql::planner::plan::{Aggregate, Node, Plan};
use crate::storage::engine::Engine;
use crate::storage::mvcc::MVCC;
use crate::types::{Column, DataType, Label, Table};
use std::collections::HashMap;

/// 将 AST 语句转换为执行计划
pub fn plan<E: Engine>(mvcc: &MVCC<E>, stmt: &Statement) -> Result<Plan> {
    match stmt {
        Statement::CreateTable { name, columns } => {
            // 确定主键索引
            let pk_idx = columns
                .iter()
                .position(|c| c.primary_key)
                .unwrap_or(0);
            let schema_cols: Result<Vec<Column>> = columns.iter().map(convert_column).collect();
            let schema_cols = schema_cols?;
            let schema = Table {
                name: name.clone(),
                primary_key: pk_idx,
                columns: schema_cols,
            };
            Ok(Plan::CreateTable { schema })
        }
        Statement::DropTable { name, if_exists } => Ok(Plan::DropTable {
            name: name.clone(),
            if_exists: *if_exists,
        }),
        Statement::Insert { table, columns, values } => {
            let schema = Catalog::get_table(mvcc, table)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", table)))?;
            let column_map = if let Some(ref cols) = columns {
                let mut map = HashMap::new();
                for (i, col_name) in cols.iter().enumerate() {
                    let idx = schema
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .ok_or_else(|| Error::InvalidData(format!("column {} not found", col_name)))?;
                    map.insert(i, idx);
                }
                Some(map)
            } else {
                None
            };
            // 默认值填充
            let mut default_rows = Vec::with_capacity(values.len());
            for expr_row in values {
                let mut row = Vec::with_capacity(schema.columns.len());
                for (i, col) in schema.columns.iter().enumerate() {
                    if let Some(ref map) = column_map {
                        if let Some(source_idx) = map.get(&i) {
                            row.push(expr_row[*source_idx].clone());
                        } else if let Some(ref default_val) = col.default {
                            row.push(value_to_expr(default_val)?);
                        } else {
                            row.push(Expression::Literal(ast::Literal::Null));
                        }
                    } else {
                        row.push(expr_row.get(i).cloned().unwrap_or(Expression::Literal(ast::Literal::Null)));
                    }
                }
                default_rows.push(row);
            }
            let values_node = Node::Values { rows: default_rows };
            Ok(Plan::Insert {
                table: schema,
                column_map: None, // 已经在上面做了映射
                source: values_node,
            })
        }
        Statement::Delete { table, r#where } => {
            let schema = Catalog::get_table(mvcc, table)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", table)))?;
            let scan = Node::Scan { table: table.clone() };
            let source = if let Some(ref cond) = r#where {
                Node::Filter {
                    predicate: cond.clone(),
                    source: Box::new(scan),
                }
            } else {
                scan
            };
            Ok(Plan::Delete {
                table: table.clone(),
                primary_key: schema.primary_key,
                source,
            })
        }
        Statement::Update { table, set, r#where } => {
            let schema = Catalog::get_table(mvcc, table)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", table)))?;
            let scan = Node::Scan { table: table.clone() };
            let source = if let Some(ref cond) = r#where {
                Node::Filter {
                    predicate: cond.clone(),
                    source: Box::new(scan),
                }
            } else {
                scan
            };
            let mut expressions = Vec::new();
            for (col_name, expr_opt) in set.iter() {
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == *col_name)
                    .ok_or_else(|| Error::InvalidData(format!("column {} not found", col_name)))?;
                let expr = expr_opt.clone().unwrap_or(Expression::Literal(ast::Literal::Null));
                expressions.push((col_idx, expr));
            }
            let pk = schema.primary_key;
            Ok(Plan::Update {
                table: schema,
                primary_key: pk,
                source,
                expressions,
            })
        }
        Statement::Select {
            select,
            from,
            r#where,
            group_by,
            having,
            order_by,
            offset,
            limit,
        } => {
            // 构建 FROM 节点
            let mut node = build_from(mvcc, from)?;

            // WHERE
            if let Some(ref cond) = r#where {
                node = Node::Filter {
                    predicate: cond.clone(),
                    source: Box::new(node),
                };
            }

            // GROUP BY + 聚合
            let has_aggregates = select.iter().any(|(expr, _)| contains_aggregate(expr))
                || having.as_ref().map_or(false, |h| contains_aggregate(h));
            let has_group_by = !group_by.is_empty();

            let rewritten_select = if has_aggregates || has_group_by {
                let aggregates = extract_aggregates(select)?;
                let mut rewritten = Vec::new();
                for (expr, alias) in select.iter() {
                    if contains_aggregate(expr) {
                        let name = alias.clone().unwrap_or_else(|| aggregate_name(expr));
                        rewritten.push((Expression::Column(None, name), alias.clone()));
                    } else {
                        rewritten.push((expr.clone(), alias.clone()));
                    }
                }
                node = Node::Aggregate {
                    group_by: group_by.clone(),
                    aggregates,
                    source: Box::new(node),
                };
                // HAVING
                if let Some(ref cond) = having {
                    node = Node::Filter {
                        predicate: cond.clone(),
                        source: Box::new(node),
                    };
                }
                rewritten
            } else {
                select.clone()
            };

            // SELECT 投影
            node = Node::Projection {
                expressions: rewritten_select.clone(),
                source: Box::new(node),
            };

            // ORDER BY
            if !order_by.is_empty() {
                node = Node::Order {
                    expressions: order_by.clone(),
                    source: Box::new(node),
                };
            }

            // LIMIT / OFFSET
            if offset.is_some() || limit.is_some() {
                node = Node::Limit {
                    offset: offset.clone(),
                    limit: limit.clone(),
                    source: Box::new(node),
                };
            }

            // 构建 labels
            let labels: Vec<Label> = select
                .iter()
                .map(|(expr, alias)| match expr {
                    Expression::Column(table, name) => {
                        if let Some(t) = table {
                            Label::Qualified(t.clone(), alias.clone().unwrap_or_else(|| name.clone()))
                        } else {
                            Label::Unqualified(alias.clone().unwrap_or_else(|| name.clone()))
                        }
                    }
                    _ => Label::from(alias.clone()),
                })
                .collect();

            Ok(Plan::Select { root: node, labels })
        }
        Statement::Begin { .. } | Statement::Commit | Statement::Rollback | Statement::Explain(_) => {
            Err(Error::InvalidData("unsupported statement for planning".into()))
        }
    }
}

fn build_from<E: Engine>(mvcc: &MVCC<E>, from: &[From]) -> Result<Node> {
    if from.is_empty() {
        return Ok(Node::Empty);
    }
    let mut node = build_from_item(mvcc, &from[0])?;
    for item in &from[1..] {
        let right = build_from_item(mvcc, item)?;
        node = Node::NestedLoopJoin {
            left: Box::new(node),
            right: Box::new(right),
            r#type: JoinType::Cross,
            predicate: None,
        };
    }
    Ok(node)
}

fn build_from_item<E: Engine>(mvcc: &MVCC<E>, item: &From) -> Result<Node> {
    match item {
        From::Table { name, .. } => {
            // 校验表存在
            let _ = Catalog::get_table(mvcc, name)?
                .ok_or_else(|| Error::InvalidData(format!("table {} not found", name)))?;
            Ok(Node::Scan { table: name.clone() })
        }
        From::Join { left, right, r#type, predicate } => {
            let left_node = build_from_item(mvcc, left)?;
            let right_node = build_from_item(mvcc, right)?;
            Ok(Node::NestedLoopJoin {
                left: Box::new(left_node),
                right: Box::new(right_node),
                r#type: r#type.clone(),
                predicate: predicate.clone(),
            })
        }
    }
}

fn convert_column(col: &ast::Column) -> Result<Column> {
    Ok(Column {
        name: col.name.clone(),
        data_type: col.datatype,
        // primary_key 信息在 Table 级别维护
        nullable: col.nullable.unwrap_or(true),
        default: col.default.as_ref().map(|expr| evaluate_literal(expr)).transpose()?,
        unique: col.unique,
        index: col.index,
        references: col.references.clone(),
    })
}

fn value_to_expr(val: &crate::types::Value) -> Result<Expression> {
    match val {
        crate::types::Value::Null => Ok(Expression::Literal(ast::Literal::Null)),
        crate::types::Value::Boolean(b) => Ok(Expression::Literal(ast::Literal::Boolean(*b))),
        crate::types::Value::Integer(i) => Ok(Expression::Literal(ast::Literal::Integer(*i))),
        crate::types::Value::Float(f) => Ok(Expression::Literal(ast::Literal::Float(*f))),
        crate::types::Value::String(s) => Ok(Expression::Literal(ast::Literal::String(s.clone()))),
    }
}

fn evaluate_literal(expr: &Expression) -> Result<crate::types::Value> {
    match expr {
        Expression::Literal(ast::Literal::Null) => Ok(crate::types::Value::Null),
        Expression::Literal(ast::Literal::Boolean(b)) => Ok(crate::types::Value::Boolean(*b)),
        Expression::Literal(ast::Literal::Integer(i)) => Ok(crate::types::Value::Integer(*i)),
        Expression::Literal(ast::Literal::Float(f)) => Ok(crate::types::Value::Float(*f)),
        Expression::Literal(ast::Literal::String(s)) => Ok(crate::types::Value::String(s.clone())),
        _ => Err(Error::InvalidData("default value must be a literal".into())),
    }
}

fn aggregate_name(expr: &Expression) -> String {
    match expr {
        Expression::Function(name, _) => {
            let name = name.to_ascii_uppercase();
            match name.as_str() {
                "COUNT" => "COUNT(*)".into(),
                "SUM" => "SUM".into(),
                "AVG" => "AVG".into(),
                "MIN" => "MIN".into(),
                "MAX" => "MAX".into(),
                _ => name,
            }
        }
        _ => "?".into(),
    }
}

fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Function(name, args) => {
            let name = name.to_ascii_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
                || args.iter().any(contains_aggregate)
        }
        Expression::Operator(op) => {
            use ast::Operator::*;
            match op {
                And(l, r) | Or(l, r) | Eq(l, r) | Greater(l, r) | GreaterEq(l, r)
                | Less(l, r) | LessEq(l, r) | NotEq(l, r) | Add(l, r) | Sub(l, r)
                | Multiply(l, r) | Div(l, r) | Remainder(l, r) | Exp(l, r) | Like(l, r) => {
                    contains_aggregate(l) || contains_aggregate(r)
                }
                Not(e) | Factor(e) | Identifier(e) | Negate(e) | Is(e, _) => contains_aggregate(e),
            }
        }
        _ => false,
    }
}

fn extract_aggregates(select: &[(Expression, Option<String>)]) -> Result<Vec<(Aggregate, Option<String>)>> {
    let mut aggregates = Vec::new();
    for (expr, alias) in select {
        if let Expression::Function(name, args) = expr {
            let name = name.to_ascii_uppercase();
            let agg = match name.as_str() {
                "COUNT" => {
                    let arg = args.get(0).cloned();
                    match arg {
                        Some(Expression::All) | None => Aggregate::Count(None),
                        Some(expr) => Aggregate::Count(Some(expr)),
                    }
                }
                "SUM" => {
                    let arg = args.get(0).cloned()
                        .ok_or_else(|| Error::InvalidData("SUM requires argument".into()))?;
                    Aggregate::Sum(arg)
                }
                "AVG" => {
                    let arg = args.get(0).cloned()
                        .ok_or_else(|| Error::InvalidData("AVG requires argument".into()))?;
                    Aggregate::Avg(arg)
                }
                "MIN" => {
                    let arg = args.get(0).cloned()
                        .ok_or_else(|| Error::InvalidData("MIN requires argument".into()))?;
                    Aggregate::Min(arg)
                }
                "MAX" => {
                    let arg = args.get(0).cloned()
                        .ok_or_else(|| Error::InvalidData("MAX requires argument".into()))?;
                    Aggregate::Max(arg)
                }
                _ => continue,
            };
            aggregates.push((agg, alias.clone()));
        }
    }
    Ok(aggregates)
}
