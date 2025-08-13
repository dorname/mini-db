use super::ast::Statement;
use crate::sql::parser::lexer::Lexer;
use std::iter::Peekable;

/// # SQL 解析器
/// 从词法分析器（lexer）产生的记号（token）中读取输入，
/// 并将 SQL 语法解析为抽象语法树（AST，Abstract Syntax Tree）。
///
/// AST 表示 SQL 查询的语法结构（例如 SELECT 与 FROM 子句、值、
/// 算术表达式等）。然而，它只保证语法形式正确；它并不知道例如某个
/// 表或列是否存在，或应当使用哪种连接（join）——这些是规划器
/// （planner）的职责。

pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Parser<'a> {
        Self { lexer: Lexer::new(input).peekable() }
    }

    /// 将输入的字符串转化为可以表示【SQL语句】的抽象语法树。
    /// 一个输入必须事单独的一个语句可以通过分号标识结尾
    pub fn pasre(statement: &str) -> crate::db_error::Result<Statement> {
        todo!()
    }
}