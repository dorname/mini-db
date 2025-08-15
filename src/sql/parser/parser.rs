use super::ast::Statement;
use crate::db_error::Result;
use crate::errinput;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
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
    pub fn pasre(statement: &str) -> Result<Statement> {
        let mut parser = Parser::new(statement);
        todo!()
    }

    fn peek(&mut self) -> Result<Option<&Token>> {
        self.lexer.peek().map(|token| token.as_ref().map_err(|err| err.clone())).transpose()
    }

    fn next(&mut self) -> Result<Token> {
        self.lexer.next().transpose()?.ok_or_else(|| errinput!("unexpected end of input"))
    }
    /// 如果下一个词法记号（token）满足给定条件，则消费并返回它；否则不消费并返回 `None`。
    ///
    /// 该方法会先查看（peek）下一个 token：
    /// - 如果无法获取下一个 token（如到达输入结束），直接返回 `None`；
    /// - 如果获取到的 token 不满足 `predicate` 闭包条件，则返回 `None`（并且不前进迭代器）；
    /// - 如果满足条件，则调用 [`next`](#method.next) 消费该 token 并返回它。
    ///
    /// # 参数
    /// - `predicate`
    ///   判断闭包，类型为 `impl Fn(&Token) -> bool`：
    ///   * 参数为下一个 token 的引用；
    ///   * 返回 `true` 表示满足条件，允许消费；
    ///   * 返回 `false` 表示不消费。
    ///
    /// # 返回值
    /// - `Some(Token)`：下一个 token 存在且满足条件，已消费并返回它；
    /// - `None`：下一个 token 不存在或不满足条件（未消费）。
    ///
    /// # 示例
    /// ```ignore
    /// // 仅当下一个 token 是标识符时才消费
    /// if let Some(tok) = lexer.next_if(|t| matches!(t, Token::Identifier(_))) {
    ///     println!("Got identifier token: {:?}", tok);
    /// }
    /// ```
    fn next_predicate(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.peek().ok()?.filter(|token| predicate(token));
        self.next().ok()
    }

    fn next_is(&mut self, token: Token) -> bool {
        self.next_predicate(|tk| token.eq(tk)).is_some()
    }

    fn skip(&mut self, token: Token) {
        self.next_is(token);
    }

    /// 读取下一个词法记号（token），并检查它是否与预期的 `expect` 相同。
    ///
    /// 如果下一个 token 与 `expect` 匹配，则将其消费（读取并前进迭代器）并返回 `Ok(())`；
    /// 如果不匹配，则返回错误，提示期望的 token 与实际读取到的 token。
    ///
    /// # 参数
    /// - `expect`
    ///   预期的 [`Token`] 值。
    ///
    /// # 返回值
    /// - `Ok(())`：成功匹配并消费了预期 token。
    /// - `Err`：下一个 token 与 `expect` 不一致，返回错误信息。
    ///
    /// # 错误
    /// - 当 `self.next()` 返回的 token 不等于 `expect` 时，调用 [`errinput!`] 宏生成输入错误。
    ///
    /// # 示例
    /// ```ignore
    /// // 假设当前 token 流的下一个 token 是 Token::Select
    /// parser.expect(Token::Select)?; // ✅ 匹配成功
    ///
    /// // 如果下一个 token 不是 Token::From，会返回错误
    /// parser.expect(Token::From)?;   // ❌ 将返回 Err(...)
    /// ```
    fn expect(&mut self, token: Token) -> Result<()> {
        let next_token = self.next()?;
        if next_token != token {
            return Err(errinput!("expected {:?}, got {:?}", token, next_token));
        }
        Ok(())
    }

    /// 根据词法单元生成【sql语句】
    fn parse_statement(&mut self) -> Result<Statement> {
        let Some(token) = self.peek()?else {
            return errinput!("Unexpected None token");
        };
        // match token {
        //     Token::Keyword(Keyword::Begin) => self.parse_begin(),
        //     token => errinput!("Unexpected token{:?}",token),
        // }
        todo!()
    }

    /// 解析 SQL `BEGIN` 语句，并返回对应的 [`ast::Statement::Begin`] 节点。
    ///
    /// 支持以下语法变体：
    /// - `BEGIN`
    /// - `BEGIN TRANSACTION`
    /// - `BEGIN READ ONLY` / `BEGIN READ WRITE`
    /// - `BEGIN AS OF SYSTEM TIME <number>`
    /// - 上述选项的组合（顺序受 SQL 规范约束）
    ///
    /// # 解析规则
    /// 1. **`BEGIN` 关键字**：
    ///    必须存在，缺失则报错。
    ///
    /// 2. **可选的 `TRANSACTION` 关键字**：
    ///    若存在则跳过（不影响解析结果）。
    ///
    /// 3. **可选的访问模式**：
    ///    如果下一个 token 是 `READ`：
    ///    - `READ ONLY` → 设置 `read_only = true`；
    ///    - `READ WRITE` → 保持 `read_only = false`；
    ///    - 其他 token → 报错。
    ///
    /// 4. **可选的时间戳限定**：
    ///    如果下一个 token 是 `AS`，则期望依次匹配：
    ///    `AS OF SYSTEM TIME <number>`
    ///    - `<number>` 必须是可解析为整数的数值字符串；
    ///    - 解析后存入 `as_of` 字段。
    ///
    /// # 返回值
    /// - 成功时返回 [`ast::Statement::Begin { read_only, as_of }`]，其中：
    ///   * `read_only`：事务是否只读；
    ///   * `as_of`：可选的系统时间戳（`Option<i64>`）。
    /// - 失败时返回输入错误（`errinput!`）。
    ///
    /// # 示例
    /// ```ignore
    /// // 解析只读事务
    /// let stmt = parser.parse_begin()?;
    /// assert!(matches!(stmt, ast::Statement::Begin { read_only: true, .. }));
    ///
    /// // 解析带时间戳的事务
    /// // BEGIN AS OF SYSTEM TIME 12345
    /// ```

    fn parse_begin(&mut self) -> Result<Statement> {
        //1、校验输入token是不是Begin
        self.expect(Keyword::Begin.into())?;
        //2、跳过事务关键字
        self.skip(Keyword::Transaction.into());

        let mut read_only = false;
        if self.next_is(Keyword::Read.into()) {
            match self.next()? {
                Token::Keyword(Keyword::Only) => {
                    read_only = true;
                }
                Token::Keyword(Keyword::Write) => {}
                token => return errinput!("Unexpected token{:?}",token),
            }
        }

        let mut version = None;
        if self.next_is(Keyword::As.into()) {
            self.expect(Keyword::Of.into())?;
            self.expect(Keyword::System.into())?;
            self.expect(Keyword::Time.into())?;
            match self.next()? {
                Token::Number(number) => version = Some(number.parse()?),
                token => return errinput!("Unexpected token{:?}, wanted number",token),
            }
        }
        Ok(Statement::Begin {
            read_only,
            target_version:version
        })
    }
}