use std::cmp::Ordering;

use crate::db_error::{Error, Result};
use crate::sql::parser::ast::{Expression, Literal, Operator};
use crate::types::{Label, Row, Value};

/// 表达式求值上下文
#[derive(Clone, Debug)]
pub struct Scope {
    pub labels: Vec<Label>,
}

impl Scope {
    pub fn new(labels: Vec<Label>) -> Self {
        Self { labels }
    }

    /// 根据表达式中的列引用查找行中的索引
    pub fn resolve(&self, table: &Option<String>, name: &str) -> Result<usize> {
        for (i, label) in self.labels.iter().enumerate() {
            match label {
                Label::Unqualified(lbl_name) if lbl_name == name => return Ok(i),
                Label::Qualified(lbl_table, lbl_name) => {
                    if lbl_name == name {
                        if let Some(ref t) = table {
                            if lbl_table == t {
                                return Ok(i);
                            }
                        } else {
                            return Ok(i);
                        }
                    }
                }
                _ => {}
            }
        }
        Err(Error::InvalidData(format!(
            "column {:?}.{} not found in scope",
            table, name
        )))
    }

    /// 合并两个 scope（用于 Join）
    pub fn join(left: &Scope, right: &Scope) -> Self {
        let mut labels = left.labels.clone();
        labels.extend(right.labels.clone());
        Self { labels }
    }
}

/// 求值表达式，返回 Value
pub fn evaluate(expr: &Expression, row: &Row, scope: &Scope) -> Result<Value> {
    match expr {
        Expression::All => Err(Error::InvalidData("cannot evaluate *".into())),
        Expression::Column(table, name) => {
            let idx = scope.resolve(table, name)?;
            Ok(row.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expression::Literal(lit) => Ok(literal_to_value(lit)),
        Expression::Function(name, args) => evaluate_function(name, args, row, scope),
        Expression::Operator(op) => evaluate_operator(op, row, scope),
    }
}

fn literal_to_value(lit: &Literal) -> Value {
    match lit {
        Literal::Null => Value::Null,
        Literal::Boolean(b) => Value::Boolean(*b),
        Literal::Integer(i) => Value::Integer(*i),
        Literal::Float(f) => Value::Float(*f),
        Literal::String(s) => Value::String(s.clone()),
    }
}

fn evaluate_operator(op: &Operator, row: &Row, scope: &Scope) -> Result<Value> {
    use Operator::*;
    match op {
        And(lhs, rhs) => {
            let l = evaluate(lhs, row, scope)?.to_bool();
            // 短路求值
            if !l {
                return Ok(Value::Boolean(false));
            }
            Ok(Value::Boolean(evaluate(rhs, row, scope)?.to_bool()))
        }
        Or(lhs, rhs) => {
            let l = evaluate(lhs, row, scope)?.to_bool();
            if l {
                return Ok(Value::Boolean(true));
            }
            Ok(Value::Boolean(evaluate(rhs, row, scope)?.to_bool()))
        }
        Eq(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o == Ordering::Equal),
        Greater(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o == Ordering::Greater),
        GreaterEq(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o != Ordering::Less),
        Less(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o == Ordering::Less),
        LessEq(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o != Ordering::Greater),
        NotEq(lhs, rhs) => compare(lhs, rhs, row, scope, |o| o != Ordering::Equal),
        Is(expr, lit) => {
            let val = evaluate(expr, row, scope)?;
            let lit_val = literal_to_value(lit);
            Ok(Value::Boolean(val == lit_val))
        }
        Add(lhs, rhs) => evaluate(lhs, row, scope)?.checked_add(&evaluate(rhs, row, scope)?),
        Sub(lhs, rhs) => evaluate(lhs, row, scope)?.checked_sub(&evaluate(rhs, row, scope)?),
        Multiply(lhs, rhs) => evaluate(lhs, row, scope)?.checked_mul(&evaluate(rhs, row, scope)?),
        Div(lhs, rhs) => evaluate(lhs, row, scope)?.checked_div(&evaluate(rhs, row, scope)?),
        Remainder(lhs, rhs) => evaluate(lhs, row, scope)?.checked_rem(&evaluate(rhs, row, scope)?),
        Exp(lhs, rhs) => evaluate(lhs, row, scope)?.checked_pow(&evaluate(rhs, row, scope)?),
        Like(lhs, rhs) => {
            let l = evaluate(lhs, row, scope)?;
            let r = evaluate(rhs, row, scope)?;
            match (l, r) {
                (Value::String(s), Value::String(pattern)) => {
                    // 简化 LIKE：仅支持 % 前缀/后缀匹配
                    let result = if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() > 1 {
                        s.contains(&pattern[1..pattern.len()-1])
                    } else if pattern.starts_with('%') {
                        s.ends_with(&pattern[1..])
                    } else if pattern.ends_with('%') {
                        s.starts_with(&pattern[..pattern.len()-1])
                    } else {
                        s == pattern
                    };
                    Ok(Value::Boolean(result))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Err(Error::InvalidData("LIKE requires strings".into())),
            }
        }
        Not(expr) => evaluate(expr, row, scope)?.not(),
        Factor(expr) => {
            let v = evaluate(expr, row, scope)?;
            match v {
                Value::Integer(i) if i >= 0 => {
                    let mut result = 1i64;
                    for x in 1..=i {
                        result = result.checked_mul(x).ok_or_else(|| Error::InvalidData("integer overflow".into()))?;
                    }
                    Ok(Value::Integer(result))
                }
                Value::Null => Ok(Value::Null),
                _ => Err(Error::InvalidData("factorial requires non-negative integer".into())),
            }
        }
        Identifier(expr) => evaluate(expr, row, scope), // +a
        Negate(expr) => evaluate(expr, row, scope)?.negate(),
    }
}

fn compare<F>(lhs: &Expression, rhs: &Expression, row: &Row, scope: &Scope, check: F) -> Result<Value>
where
    F: Fn(Ordering) -> bool,
{
    let l = evaluate(lhs, row, scope)?;
    let r = evaluate(rhs, row, scope)?;
    // SQL 语义：Null 与任何值比较都返回 Null（三值逻辑）
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }
    Ok(Value::Boolean(check(l.cmp(&r))))
}

fn evaluate_function(name: &str, args: &[Expression], row: &Row, scope: &Scope) -> Result<Value> {
    let name = name.to_ascii_uppercase();
    let vals: Result<Vec<Value>> = args.iter().map(|a| evaluate(a, row, scope)).collect();
    let vals = vals?;

    match name.as_str() {
        "COUNT" => Ok(Value::Integer(vals.len() as i64)),
        "SUM" => {
            let mut sum = Value::Integer(0);
            for v in vals {
                if !v.is_null() {
                    sum = sum.checked_add(&v)?;
                }
            }
            Ok(sum)
        }
        "AVG" => {
            let mut sum = Value::Integer(0);
            let mut count = 0i64;
            for v in vals {
                if !v.is_null() {
                    sum = sum.checked_add(&v)?;
                    count += 1;
                }
            }
            if count == 0 {
                Ok(Value::Null)
            } else {
                // 强制转 float 做平均
                let sum_f: f64 = match sum {
                    Value::Integer(i) => i as f64,
                    Value::Float(f) => f,
                    _ => return Err(Error::InvalidData("AVG requires numeric".into())),
                };
                Ok(Value::Float(sum_f / count as f64))
            }
        }
        "MIN" => {
            let mut min: Option<Value> = None;
            for v in vals {
                if !v.is_null() {
                    match &min {
                        None => min = Some(v),
                        Some(m) if v < *m => min = Some(v),
                        _ => {}
                    }
                }
            }
            Ok(min.unwrap_or(Value::Null))
        }
        "MAX" => {
            let mut max: Option<Value> = None;
            for v in vals {
                if !v.is_null() {
                    match &max {
                        None => max = Some(v),
                        Some(m) if v > *m => max = Some(v),
                        _ => {}
                    }
                }
            }
            Ok(max.unwrap_or(Value::Null))
        }
        "ABS" => {
            if vals.len() != 1 {
                return Err(Error::InvalidData("ABS takes 1 argument".into()));
            }
            match &vals[0] {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::InvalidData("ABS requires numeric".into())),
            }
        }
        "UPPER" => {
            if vals.len() != 1 {
                return Err(Error::InvalidData("UPPER takes 1 argument".into()));
            }
            match &vals[0] {
                Value::String(s) => Ok(Value::String(s.to_uppercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::InvalidData("UPPER requires string".into())),
            }
        }
        "LOWER" => {
            if vals.len() != 1 {
                return Err(Error::InvalidData("LOWER takes 1 argument".into()));
            }
            match &vals[0] {
                Value::String(s) => Ok(Value::String(s.to_lowercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::InvalidData("LOWER requires string".into())),
            }
        }
        _ => Err(Error::InvalidData(format!("unknown function: {}", name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{Expression, Literal, Operator};

    #[test]
    fn test_eval_literal() {
        let scope = Scope::new(vec![]);
        let row: Row = vec![];
        let expr = Expression::Literal(Literal::Integer(42));
        assert_eq!(evaluate(&expr, &row, &scope).unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_eval_column() {
        let scope = Scope::new(vec![Label::Unqualified("id".into()), Label::Unqualified("name".into())]);
        let row: Row = vec![Value::Integer(1), Value::String("alice".into())];
        let expr = Expression::Column(None, "name".into());
        assert_eq!(evaluate(&expr, &row, &scope).unwrap(), Value::String("alice".into()));
    }

    #[test]
    fn test_eval_arithmetic() {
        let scope = Scope::new(vec![]);
        let row: Row = vec![];
        let expr = Expression::Operator(Operator::Add(
            Box::new(Expression::Literal(Literal::Integer(3))),
            Box::new(Expression::Literal(Literal::Integer(4))),
        ));
        assert_eq!(evaluate(&expr, &row, &scope).unwrap(), Value::Integer(7));
    }

    #[test]
    fn test_eval_compare() {
        let scope = Scope::new(vec![]);
        let row: Row = vec![];
        let expr = Expression::Operator(Operator::Greater(
            Box::new(Expression::Literal(Literal::Integer(5))),
            Box::new(Expression::Literal(Literal::Integer(3))),
        ));
        assert_eq!(evaluate(&expr, &row, &scope).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_eval_null_compare() {
        let scope = Scope::new(vec![]);
        let row: Row = vec![];
        let expr = Expression::Operator(Operator::Eq(
            Box::new(Expression::Literal(Literal::Null)),
            Box::new(Expression::Literal(Literal::Integer(3))),
        ));
        assert_eq!(evaluate(&expr, &row, &scope).unwrap(), Value::Null);
    }
}
