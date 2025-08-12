use crate::sql::parser::lexer::Lexer;
use std::iter::Peekable;

/// # 语法分析
pub struct Parser<'a> {
    lexer: Peekable<Lexer<'a>>,
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Parser<'a> {
        Self { lexer: Lexer::new(input).peekable() }
    }
}