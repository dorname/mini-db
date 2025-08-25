use super::ast::{Column, Direction, Expression, JoinType, Literal, Statement};
use crate::db_error::Result;
use crate::errinput;
use crate::sql::parser::ast;
use crate::sql::parser::ast::Literal::Null;
use crate::sql::parser::ast::Statement::{Delete, Insert, Select};
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::types::DataType;
use std::cmp::PartialEq;
use std::collections::BTreeMap;
use std::iter::Peekable;
use std::ops::Add;

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
        self.peek().ok()?.filter(|token| predicate(token))?;
        self.next().ok()
    }

    fn next_is(&mut self, token: Token) -> bool {
        self.next_predicate(|tk| {
            token.eq(tk)
        }).is_some()
    }

    fn next_matches(&mut self, keywords: Vec<Keyword>) -> Option<Token> {
        self.next_predicate(|tk| {
            keywords.iter().any(|keyword| Token::Keyword(*keyword).eq(tk))
        })
    }

    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Identifier(s) => Ok(s),
            token => errinput!("unexpected token {:?}", token),
        }
    }

    fn next_if_map<T, F>(&mut self, f: F) -> Option<T>
    where
        F: Fn(&Token) -> Option<T>,
    {
        self.peek().ok()?.map(|token| f(&token))?.inspect(|_| {
            drop(self.next());
        })
    }

    fn next_if_keyword(&mut self) -> Option<Keyword> {
        self.next_if_map(|token| {
            match token {
                Token::Keyword(kw) => Some(*kw),
                _ => None,
            }
        })
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
        match token {
            // 事务
            Token::Keyword(Keyword::Begin) => self.parse_begin(),
            Token::Keyword(Keyword::Commit) => self.parse_commit(),
            Token::Keyword(Keyword::Rollback) => self.parse_rollback(),
            Token::Keyword(Keyword::Explain) => self.parse_explain(),
            // 表操作
            Token::Keyword(Keyword::Create) => self.parse_create_table(),
            Token::Keyword(Keyword::Drop) => self.parse_drop_table(),
            // CRUD
            Token::Keyword(Keyword::Update) => self.parse_update(),
            Token::Keyword(Keyword::Delete) => self.parse_delete(),
            Token::Keyword(Keyword::Select) => self.parse_select(),
            Token::Keyword(Keyword::Insert) => self.parse_insert(),
            _ => errinput!("Unexpected token{:?}",token),
        }
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
            target_version: version,
        })
    }


    /// 将词法单元commit转化成语法单元
    fn parse_commit(&mut self) -> Result<Statement> {
        self.expect(Keyword::Commit.into())?;
        Ok(Statement::Commit)
    }

    /// 将词法单元RollBack转化为语法单元
    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(Keyword::Rollback.into())?;
        Ok(Statement::Rollback)
    }

    /// 将词法单元Explain转化为语法单元
    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(Keyword::Explain.into())?;
        if self.next_is(Keyword::Explain.into()) {
            return errinput!("Explain statement cant not nested 解释语法不支持嵌套");
        }
        Ok(Statement::Explain(Box::new(self.parse_statement()?)))
    }


    /// 将词法 create table 转化为语法单元
    fn parse_create_table(&mut self) -> Result<Statement> {
        self.expect(Keyword::Create.into())?;
        self.expect(Keyword::Table.into())?;
        // 读取下一个表名
        let name = self.next_ident()?;
        // 判断下一个是不是左括号
        self.expect(Token::OpenParen.into())?;
        // 收集列信息
        let mut columns: Vec<_> = Vec::<Column>::new();
        loop {
            columns.push(self.parse_create_table_columns()?);
            let flag = self.next_is(Token::Comma);
            if !flag {
                break;
            }
        }
        // 闭合判断
        self.expect(Token::CloseParen.into())?;
        Ok(Statement::CreateTable { name, columns })
    }


    /// 将列定义词法单元token 转化为语法单元
    /// ```sql
    /// create table `table_name` ( `column_name` data_type column_constraint, ... );
    /// ```
    fn parse_create_table_columns(&mut self) -> Result<Column> {
        let column_name = self.next_ident()?;
        let column_type = match self.next()? {
            Token::Keyword(Keyword::Boolean | Keyword::Bool) => DataType::Boolean,
            Token::Keyword(Keyword::Float | Keyword::Double) => DataType::Float,
            Token::Keyword(Keyword::Int | Keyword::Integer) => DataType::Integer,
            Token::Keyword(Keyword::String | Keyword::Text | Keyword::Varchar) => DataType::String,
            token => return errinput!("unexpected token {:?}",token),
        };
        let mut column = Column {
            name: column_name,
            datatype: column_type,
            primary_key: false,
            nullable: None,
            unique: false,
            index: false,
            references: None,
            default: None,
        };
        while let Some(keyword) = self.next_if_keyword() {
            match keyword {
                Keyword::Primary => {
                    self.expect(Keyword::Key.into())?;
                    column.primary_key = true;
                }
                Keyword::Null => {
                    if column.nullable.is_some() {
                        return errinput!("Nullable is already set for column {}", column.name);
                    }
                    column.nullable = Some(true);
                }
                Keyword::Unique => column.unique = true,
                Keyword::Index => column.index = true,
                Keyword::Not => {
                    self.expect(Keyword::Null.into())?;
                    if column.nullable.is_some() {
                        return errinput!("Nullable is already set for column {}", column.name);
                    }
                    column.nullable = Some(false);
                }
                Keyword::References => column.references = Some(self.next_ident()?),
                Keyword::Default => column.default = Some(self.parse_expression()?),
                _ => return errinput!("unexpected keyword {:?}",keyword),
            }
        }
        Ok(column)
    }

    /// 解析 `DROP TABLE` SQL 语句。
    ///
    /// # 语法
    ///
    /// ```sql
    /// DROP TABLE [IF EXISTS] table_name;
    /// ```
    ///
    /// # 行为
    ///
    /// - 依次解析 `DROP` 与 `TABLE` 关键字。
    /// - 如果存在 `IF EXISTS` 子句，则记录 `if_exists = true`。
    /// - 随后解析下一个标识符作为表名。
    ///
    /// # 返回值
    ///
    /// 成功时返回 [`ast::Statement::DropTable`] 枚举变体，包含：
    /// - `name`：要删除的表名。
    /// - `if_exists`：是否包含 `IF EXISTS` 子句。
    ///
    /// # 错误
    ///
    /// 当出现以下情况时会返回错误：
    /// - 缺少 `DROP`、`TABLE`、`IF`、`EXISTS` 等关键字。
    /// - 缺少表名标识符或标识符无效。
    /// - 出现未预期的 token。
    ///
    /// # 示例
    ///
    /// ```text
    /// use crate::sql::parser::Parser;
    ///
    /// let mut parser = Parser::new("DROP TABLE IF EXISTS users;");
    /// let stmt = parser.parse_drop_table().unwrap();
    /// assert_eq!(
    ///     stmt,
    ///     ast::Statement::DropTable {
    ///         name: "users".to_string(),
    ///         if_exists: true,
    ///     }
    /// );
    /// ```
    fn parse_drop_table(&mut self) -> Result<Statement> {
        self.expect(Keyword::Drop.into())?;
        self.expect(Keyword::Table.into())?;
        let mut if_exists = false;
        // IF EXISTS 判断
        if self.next_is(Keyword::If.into()) {
            self.expect(Token::Keyword(Keyword::Exists).into())?;
            if_exists = true;
        }
        let name = self.next_ident()?;
        Ok(Statement::DropTable { name, if_exists })
    }

    /// 条件块构建
    fn parse_where(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Where.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }
    /// 分组块构建
    fn parse_group_by(&mut self) -> Result<Vec<Expression>> {
        if !self.next_is(Keyword::Group.into()) {
            return Ok(vec![]);
        }
        let mut result = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            result.push(self.parse_expression()?);
            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(result)
    }

    /// order by
    fn parse_order_by(&mut self) -> Result<Vec<(Expression, Direction)>> {
        if !self.next_is(Keyword::Order.into()) {
            return Ok(vec![]);
        }
        let mut result = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            let expression = self.parse_expression()?;
            let order = self.next_if_map(|token|
                match token {
                    Token::Keyword(Keyword::Asc) => Some(Direction::Asc),
                    Token::Keyword(Keyword::Desc) => Some(Direction::Desc),
                    _ => None,
                }
            ).unwrap_or_default();
            result.push((expression, order));

            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(result)
    }


    /// Limit
    fn parse_limit(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Limit.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// offset
    fn parse_offset(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Offset.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// having
    fn parse_having(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Having.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// from table t/ as t
    fn parse_from_table(&mut self) -> Result<ast::From> {
        let name = self.next_ident()?;
        let mut alias = None;
        if self.next_is(Keyword::As.into())
            || matches!(self.peek()?,Some(Token::Identifier(_))) {
            alias = Some(self.next_ident()?);
        }
        Ok(ast::From::Table {
            name,
            alias,
        })
    }

    /// 表链接类型
    fn parse_from_join(&mut self) -> Result<Option<JoinType>> {
        let keywords = vec![
            Keyword::Join,
            Keyword::Cross,
            Keyword::Inner,
            Keyword::Left,
            Keyword::Right,
        ];
        let Some(join_type) = self.next_matches(keywords) else {
            return Ok(None);
        };
        match join_type {
            Token::Keyword(Keyword::Join) => Ok(Some(JoinType::Inner)),
            Token::Keyword(Keyword::Cross) => {
                self.expect(Keyword::Join.into())?;
                Ok(Some(JoinType::Cross))
            }
            Token::Keyword(Keyword::Inner) => {
                self.expect(Keyword::Join.into())?;
                Ok(Some(JoinType::Inner))
            }
            Token::Keyword(Keyword::Left) => {
                self.expect(Keyword::Join.into())?;
                Ok(Some(JoinType::Left))
            }
            Token::Keyword(Keyword::Right) => {
                self.expect(Keyword::Join.into())?;
                Ok(Some(JoinType::Right))
            }
            _ => Ok(None)
        }
    }

    /// 表数据删除
    /// ```text
    /// delete from {table_name} where x > 1;
    /// ```
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(Keyword::Delete.into())?;
        self.expect(Keyword::From.into())?;
        Ok(Delete {
            table: self.next_ident()?,
            r#where: self.parse_where()?,
        })
    }

    /// 表数据新增
    /// ```text
    /// insert into {table_name} ({column_1,...,column_n}) values ({value_1,...,value_n});
    /// ```
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Keyword::Insert.into())?;
        self.expect(Keyword::Into.into())?;
        let table = self.next_ident()?;

        let mut columns = None;
        if self.next_is(Token::OpenParen.into()) {
            let columns = columns.insert(Vec::new());
            loop {
                columns.push(self.next_ident()?);
                if !self.next_is(Token::Comma.into()) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
        }
        self.expect(Keyword::Values.into())?;
        let mut values = Vec::<Vec<Expression>>::new();
        loop {
            self.expect(Token::OpenParen)?;
            let mut rows = Vec::new();
            loop {
                rows.push(self.parse_expression()?);
                if !self.next_is(Token::Comma.into()) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
            values.push(rows);
            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(Insert {
            table,
            columns,
            values,
        })
    }

    /// 查询
    fn parse_select(&mut self) -> Result<Statement> {
        Ok(Select {
            select: self.parse_select_clause()?,
            from: self.parse_from_clause()?,
            r#where: self.parse_where()?,
            group_by: self.parse_group_by()?,
            having: self.parse_having()?,
            order_by: self.parse_order_by()?,
            offset: self.parse_offset()?,
            limit: self.parse_limit()?,
        })
    }

    /// 更新
    /// ```text
    /// update {table_name} set {column_1} = {value_1}, {column_2} = {value_2} where x > 1;
    /// ```
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(Keyword::Update.into())?;
        let table = self.next_ident()?;
        self.expect(Keyword::Set.into())?;
        let mut set = BTreeMap::<String, Option<Expression>>::new();
        loop {
            let column = self.next_ident()?;
            self.expect(Token::Equal)?;
            let value = self.parse_expression()?;
            set.insert(column, Some(value));
            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(Statement::Update {
            table,
            set,
            r#where: self.parse_where()?,
        })
    }

    /// select_clause
    fn parse_select_clause(&mut self) -> Result<Vec<(Expression, Option<String>)>> {
        if !self.next_is(Keyword::Select.into()) {
            return Ok(vec![]);
        }
        let mut select = Vec::new();
        loop {
            let expression = self.parse_expression()?;
            let mut alias = None;
            if self.next_is(Keyword::As.into()) || matches!(self.peek()?,Some(Token::Identifier(_))) {
                if expression == Expression::All {
                    return errinput!("can't alias");
                }
                alias = Some(self.next_ident()?);
            }
            select.push((expression, alias));
            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(select)
    }

    /// parse from clause
    fn parse_from_clause(&mut self) -> Result<Vec<ast::From>> {
        if !self.next_is(Keyword::From.into()) {
            return Ok(Vec::new());
        }
        let mut from = Vec::new();
        loop {
            let mut from_item = self.parse_from_table()?;
            while let Some(r#type) = self.parse_from_join()? {
                let left = Box::new(from_item);
                let right = Box::new(self.parse_from_table()?);
                let mut predicate = None;
                if r#type != JoinType::Cross {
                    self.expect(Keyword::On.into())?;
                    predicate = Some(self.parse_expression()?);
                }
                from_item = ast::From::Join {
                    left,
                    right,
                    r#type,
                    predicate,
                };
            }
            from.push(from_item);
            if !self.next_is(Token::Comma.into()) {
                break;
            }
        }
        Ok(from)
    }

    /// 使用“优先级爬升算法（precedence climbing algorithm）”解析表达式。参考：
    ///
    /// <https://eli.thegreenplace.net/2012/08/02/parsing-expressions-by-precedence-climbing>
    ///
    /// 表达式主要由两类元素组成：
    ///
    /// * 原子（Atoms）：值、变量、函数、括号括起来的子表达式。
    /// * 运算符（Operators）：作用于原子和子表达式。
    ///   * 前缀运算符：例如 `-a` 或 `NOT a`。
    ///   * 中缀运算符：例如 `a + b` 或 `a AND b`。
    ///   * 后缀运算符：例如 `a!` 或 `a IS NULL`。
    ///
    /// 在解析过程中，必须遵循数学中的运算符优先级和结合律。例如：
    ///
    /// 2 ^ 3 ^ 2 - 4 * 3
    ///
    /// 按照优先级和结合律规则，该表达式应当解释为：
    ///
    /// (2 ^ (3 ^ 2)) - (4 * 3)
    ///
    /// 其中，指数运算符 `^` 是**右结合**的，所以结果是 `2 ^ (3 ^ 2) = 512`，
    /// 而不是 `(2 ^ 3) ^ 2 = 64`。同样，指数和乘法的优先级高于减法，
    /// 因此整个结果为 `(2 ^ 3 ^ 2) - (4 * 3) = 500`，
    /// 而不是 `2 ^ 3 ^ (2 - 4) * 3 = -3.24`。
    ///
    /// 在使用优先级爬升算法之前，需要将运算符的优先级映射为数值（1 为最低优先级）：
    ///
    /// * 1: OR
    /// * 2: AND
    /// * 3: NOT
    /// * 4: =, !=, LIKE, IS
    /// * 5: <, <=, >, >=
    /// * 6: +, -
    /// * 7: *, /, %
    /// * 8: ^
    /// * 9: !
    /// * 10: +, -（前缀）
    ///
    /// 运算符的结合律规则：
    ///
    /// * 右结合：^ 以及所有前缀运算符。
    /// * 左结合：其他所有运算符。
    ///
    /// 左结合的运算符在数值优先级上 +1，这样它们比右结合运算符更“紧密”地绑定左操作数。
    ///
    /// 优先级爬升算法的基本思路是：
    /// 递归解析表达式的左侧（包括前缀运算符），
    /// 然后解析中缀运算符及右侧子表达式，
    /// 最后再处理后缀运算符。
    ///
    /// 表达式的分组方式由右侧递归何时终止来决定。
    /// 算法会尽可能贪婪地消费运算符，但只有当运算符的优先级大于或等于
    /// 上一个运算符的优先级时才会继续（因此叫“爬升”）。
    /// 一旦遇到优先级更低的运算符，就会结束当前递归，返回当前的子表达式，
    /// 并在上层继续解析。
    ///
    /// 以前面的例子为例，各运算符的优先级如下：
    ///```text
    ///     -----          优先级 9: ^ 右结合
    /// ---------          优先级 9: ^
    ///             -----  优先级 7: *
    /// -----------------  优先级 6: -
    /// 2 ^ 3 ^ 2 - 4 * 3
    ///```
    /// 递归解析过程如下：
    ///```text
    /// parse_expression_at(prec=0)
    ///   lhs = parse_expression_atom() = 2
    ///   op = parse_infix_operator(prec=0) = ^ (prec=9)
    ///   rhs = parse_expression_at(prec=9)
    ///     lhs = parse_expression_atom() = 3
    ///     op = parse_infix_operator(prec=9) = ^ (prec=9)
    ///     rhs = parse_expression_at(prec=9)
    ///       lhs = parse_expression_atom() = 2
    ///       op = parse_infix_operator(prec=9) = None (拒绝 - 因为优先级=6)
    ///       return lhs = 2
    ///     lhs = (lhs op rhs) = (3 ^ 2)
    ///     op = parse_infix_operator(prec=9) = None (拒绝 - 因为优先级=6)
    ///     return lhs = (3 ^ 2)
    ///   lhs = (lhs op rhs) = (2 ^ (3 ^ 2))
    ///   op = parse_infix_operator(prec=0) = - (prec=6)
    ///   rhs = parse_expression_at(prec=6)
    ///     lhs = parse_expression_atom() = 4
    ///     op = parse_infix_operator(prec=6) = * (prec=7)
    ///     rhs = parse_expression_at(prec=7)
    ///       lhs = parse_expression_atom() = 3
    ///       op = parse_infix_operator(prec=7) = None (表达式结束)
    ///       return lhs = 3
    ///     lhs = (lhs op rhs) = (4 * 3)
    ///     op = parse_infix_operator(prec=6) = None (表达式结束)
    ///     return lhs = (4 * 3)
    ///   lhs = (lhs op rhs) = ((2 ^ (3 ^ 2)) - (4 * 3))
    ///   op = parse_infix_operator(prec=0) = None (表达式结束)
    ///   return lhs = ((2 ^ (3 ^ 2)) - (4 * 3))
    /// ```
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_expression_at(0)
    }

    /// 以给定的最小优先级解析一个表达式。
    ///
    /// 该函数实现了 **优先级爬升算法（precedence climbing）**，用于解析复杂表达式，
    /// 支持以下几类运算符：
    /// - **前缀运算符**（例如：`-x`、`NOT x`）
    /// - **后缀运算符**（例如：`x!`、`x IS NULL`）
    /// - **中缀（二元）运算符**（例如：`x + y`、`x AND y`）
    ///
    /// # 算法流程
    ///
    /// 1. **初始化左操作数 (LHS)**
    ///    - 如果当前位置是 **前缀运算符**，则递归解析其右操作数，
    ///      并将结果与前缀运算符组合为一个新表达式。
    ///    - 否则，将当前位置解析为一个原子表达式（字面量、标识符、括号表达式等）。
    ///
    /// 2. **应用后缀运算符**
    ///    - 如果在左操作数后紧跟着后缀运算符（例如 `x!`），则依次应用它们。
    ///
    /// 3. **应用中缀运算符**
    ///    - 当下一个运算符的优先级 **大于等于** 当前的 `min_precedence` 时：
    ///      - 取出该运算符。
    ///      - 递归解析其右操作数 (RHS)，并传入更新后的优先级。
    ///      - 将 LHS、运算符 和 RHS 组合为新的表达式。
    ///
    /// 4. **处理最终的后缀运算符**
    ///    - 在中缀运算符之后，表达式末尾仍可能跟随后缀运算符（例如：`1 + NULL IS NULL`），
    ///      此时需要再次应用这些后缀运算符。
    ///
    /// # 参数
    ///
    /// * `min_precedence` - 当前解析所允许的最小运算符优先级。
    ///   用于控制运算符的结合顺序，保证解析结果符合优先级和结合律规则。
    ///
    /// # 返回值
    ///
    /// 返回一个 [`ast::Expression`]，表示完整的抽象语法树表达式。
    /// 如果输入非法，则返回错误。
    ///
    /// # 可能的错误
    ///
    /// - 语法无效（例如缺少操作数或多余的符号）；
    /// - 表达式不完整；
    /// - 出现了不期望的 Token。
    ///
    /// # 示例
    ///
    /// ```sql
    /// -- SQL 风格示例
    /// 1 + 2 * 3
    /// -- 会被解析为 (1 + (2 * 3))，遵循运算符优先级
    /// ```
    ///
    /// ```text
    /// // 假设 `parser` 已经初始化并装载了 "1 + 2 * 3" 的 Token
    /// let expr = parser.parse_expression_at(0)?;
    /// assert_eq!(expr.to_string(), "1 + (2 * 3)");
    /// ```
    fn parse_expression_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        //1
        let mut lhs = if let Some(prefix_op) = self.parse_prefix_op_at(min_precedence) {
            // 前缀运算符=>右结合
            let next_precedence = prefix_op.precedence() + prefix_op.associativity();
            // 递归处理右边的表达式
            let rhs = self.parse_expression_at(next_precedence)?;
            // 返回最终的表达式
            prefix_op.into_expression(rhs)
        } else {
            // 字面常量/标识符/括号表达式
            self.parse_expression_atom()?
        };
        //2
        while let Some(postfix_op) = self.parse_postfix_op_at(min_precedence)? {
            lhs = postfix_op.into_expression(lhs)
        }
        //3
        while let Some(min_op) = self.parse_mid_op_at(min_precedence) {
            let next_precedence = min_op.precedence() + min_op.associativity();
            let rhs = self.parse_expression_at(next_precedence)?;
            lhs = min_op.into_expression(lhs, rhs)
        }

        //4
        while let Some(postfix_op) = self.parse_postfix_op_at(min_precedence)? {
            lhs = postfix_op.into_expression(lhs)
        }

        Ok(lhs)
    }

    /// 尝试解析一个**前缀运算符 (prefix operator)**。
    ///
    /// 当前缀运算符存在，且其优先级大于等于 `min_precedence` 时，
    /// 本方法会返回对应的 [`PrefixOp`]，否则返回 `None`。
    ///
    /// 支持的前缀运算符包括：
    /// - `NOT`：逻辑取反
    /// - `-`：算术取负（例如 `-5`）
    /// - `+`：算术取正（通常不改变数值，例如 `+5`）
    ///
    /// # 参数
    ///
    /// * `min_precedence` - 最小运算符优先级，只有当前缀运算符的优先级
    ///   大于或等于该值时，才会被接受。
    ///
    /// # 返回值
    ///
    /// - 如果成功匹配到前缀运算符，并且其优先级符合要求，返回 `Some(PrefixOp)`。
    /// - 如果当前位置不是前缀运算符，或其优先级过低，返回 `None`。
    ///
    /// # 示例
    ///
    /// ```sql
    /// -- SQL 片段
    /// NOT TRUE   -- 匹配到前缀运算符 NOT
    /// -123       -- 匹配到前缀运算符 -
    /// +456       -- 匹配到前缀运算符 +
    /// ```
    ///
    /// ```text
    /// // 假设输入 Token 为 "-5"
    /// if let Some(op) = parser.parse_prefix_operator_at(0) {
    ///     assert_eq!(op, PrefixOp::Minus);
    /// }
    /// ```
    fn parse_prefix_op_at(&mut self, min_precedence: Precedence) -> Option<PrefixOp> {
        self.next_if_map(|token| {
            let op = match token {
                Token::Keyword(Keyword::Not) => PrefixOp::Not,
                Token::Minus => PrefixOp::Minus,
                Token::Plus => PrefixOp::Plus,
                _ => return None,
            };
            Some(op).filter(|o| o.precedence() >= min_precedence)
        })
    }

    /// 解析一个**原子表达式 (expression atom)**。
    ///
    /// 原子表达式是表达式的最小单元，本方法支持以下几类：
    ///
    /// * **字面量值**
    ///   - 整数：`123`
    ///   - 浮点数：`3.14`
    ///   - 字符串：`'hello'`
    ///   - 布尔值：`TRUE` / `FALSE`
    ///   - 特殊值：`NULL`、`NaN`、`Infinity`
    ///
    /// * **列名**
    ///   - 未限定列名：`column`
    ///   - 限定列名：`table.column`
    ///
    /// * **函数调用**
    ///   - 格式：`func(arg1, arg2, ...)`
    ///   - 通过识别 `标识符 + 左括号` 来判断是否为函数调用。
    ///
    /// * **括号括起的表达式**
    ///   - 格式：`(expr)`
    ///   - 用于调整优先级或构建子表达式。
    ///
    /// * **特殊符号**
    ///   - 星号 `*`：表示所有列（例如 `SELECT *`）。
    ///
    /// # 返回值
    ///
    /// 成功时返回一个 [`ast::Expression`]，表示已解析的原子表达式。
    ///
    /// # 错误
    ///
    /// - 如果遇到的 Token 不符合任何原子表达式规则，会返回语法错误。
    /// - 如果括号未正确闭合，或函数调用参数列表有语法错误，也会报错。
    ///
    /// # 示例
    ///
    /// ```sql
    /// -- 以下 SQL 片段中的原子表达式
    /// 123               -- 整数字面量
    /// 'hello'           -- 字符串字面量
    /// TRUE              -- 布尔字面量
    /// col1              -- 列名
    /// t.col2            -- 限定列名
    /// ABS(-5)           -- 函数调用
    /// (1 + 2)           -- 括号表达式
    /// *                 -- 所有列
    /// ```
    ///
    /// ```text
    /// // 假设 parser 输入了 "ABS(1)"
    /// let expr = parser.parse_expression_atom()?;
    /// assert_eq!(expr.to_string(), "ABS(1)");
    /// ```
    fn parse_expression_atom(&mut self) -> Result<Expression> {
        Ok(
            match self.next()? {
                // *
                Token::Asterisk => Expression::All,

                // 常量
                Token::Number(number) if number.chars().all(|c| c.is_ascii_digit()) => {
                    Literal::Integer(number.parse()?).into()
                }

                // 浮点数
                Token::Number(num) => Literal::Float(num.parse()?).into(),
                // 字符串
                Token::String(string) => Literal::String(string).into(),
                // 关键字
                Token::Keyword(Keyword::True) => Literal::Boolean(true).into(),
                Token::Keyword(Keyword::False) => Literal::Boolean(false).into(),
                Token::Keyword(Keyword::Infinity) => Literal::Float(f64::INFINITY).into(),
                Token::Keyword(Keyword::NaN) => Literal::Float(f64::NAN).into(),
                Token::Keyword(Keyword::Null) => Literal::Null.into(),

                //函数调用
                Token::Identifier(name) if self.next_is(Token::OpenParen) => {
                    let mut args = Vec::new();;
                    while !self.next_is(Token::CloseParen) {
                        if !args.is_empty() {
                            self.expect(Token::Comma)?;
                        }
                        args.push(self.parse_expression()?);
                    }
                    Expression::Function(name, args)
                }

                //限定列名
                Token::Identifier(table_name) if self.next_is(Token::Period) => {
                    Expression::Column(Some(table_name), self.next_ident()?)
                }

                //普通列名
                Token::Identifier(column) => {
                    Expression::Column(None, column)
                }

                //括号表达式
                Token::OpenParen => {
                    let expr = self.parse_expression()?;
                    self.expect(Token::CloseParen)?;
                    expr
                }
                token => return errinput!("unexpected token {:?}",token),
            }
        )
    }

    /// 尝试解析一个**后缀运算符 (postfix operator)**。
    ///
    /// 当当前位置的 Token 构成一个合法的后缀运算符，并且其优先级
    /// 大于等于 `min_precedence` 时，返回对应的 [`PostfixOp`]。
    /// 否则返回 `None`。
    ///
    /// 支持的后缀运算符包括：
    /// - **IS [NOT] NULL**：判空运算，例如：`expr IS NULL`、`expr IS NOT NULL`
    /// - **IS [NOT] NaN**：判 NaN 运算，例如：`expr IS NaN`、`expr IS NOT NaN`
    /// - **阶乘运算符 `!`**：例如 `5!`
    ///
    /// # 特殊说明
    ///
    /// - `IS NULL` / `IS NOT NULL` / `IS NaN` / `IS NOT NaN` 由多个 Token 组成，
    ///   需要特殊处理。
    /// - 为了保证语法正确，只有在运算符的优先级满足 `min_precedence`
    ///   要求时才会真正消费 Token。
    ///
    /// # 参数
    ///
    /// * `min_precedence` - 最小运算符优先级，控制运算符是否可以被解析。
    ///
    /// # 返回值
    ///
    /// * `Ok(Some(PostfixOp))` - 成功解析到合法的后缀运算符。
    /// * `Ok(None)` - 没有匹配到后缀运算符，或其优先级过低。
    /// * `Err(..)` - 输入 Token 不符合预期，导致语法错误。
    ///
    /// # 示例
    ///
    /// ```sql
    /// -- SQL 片段
    /// col IS NULL        -- 匹配 PostfixOp::Is(Null)
    /// col IS NOT NULL    -- 匹配 PostfixOp::IsNot(Null)
    /// col IS NaN         -- 匹配 PostfixOp::Is(NaN)
    /// col IS NOT NaN     -- 匹配 PostfixOp::IsNot(NaN)
    /// 5!                 -- 匹配 PostfixOp::Factor
    /// ```
    ///
    /// ```text
    /// // 假设输入 Token 为 "value IS NULL"
    /// let op = parser.parse_postfix_operator_at(0)?.unwrap();
    /// assert_eq!(op, PostfixOp::Is(ast::Literal::Null));
    /// ```
    fn parse_postfix_op_at(&mut self, min_precedence: Precedence) -> Result<Option<PostfixOp>> {
        // 如果下一个词法单元是Is
        if self.peek()? == Some(&Token::Keyword(Keyword::Is)) {
            // 判断Is语法的优先级是否大于当前的优先级注意：is null/is nan/is not null/is not nan的优先级大小都是一致的选一个出来比较就行了
            if PostfixOp::Is(Null).precedence() < min_precedence {
                return Ok(None);
            }
            self.expect(Keyword::Is.into())?; // 消费is移动到下一个token
            // 判断下一个词法单元是不是not
            let not = self.next_is(Token::Keyword(Keyword::Not));
            // 获取值 null/nan
            let val = match self.next()? {
                Token::Keyword(Keyword::Null) => Literal::Null,
                Token::Keyword(Keyword::NaN) => Literal::Float(f64::NAN),
                token => return errinput!("unexpected token {:?}",token),
            };
            let op = match not {
                true => PostfixOp::IsNot(val),
                false => PostfixOp::Is(val)
            };
            return Ok(Some(op));
        }
        Ok(self.next_if_map(|token| {
            let op = match token {
                Token::Exclamation => PostfixOp::Factor,
                _ => return None
            };
            Some(op).filter(|o| o.precedence() >= min_precedence)
        }))
    }

    /// 尝试解析一个**中缀运算符 (middle operator)**。
    ///
    /// 当当前位置的 Token 是合法的中缀运算符，且其优先级大于等于
    /// `min_precedence` 时，返回对应的 [`MiddleOp`]；
    /// 否则返回 `None`。
    ///
    /// 支持的中缀运算符包括：
    ///
    /// **算术运算符**
    /// - `+` → 加法
    /// - `-` → 减法
    /// - `*` → 乘法
    /// - `/` → 除法
    /// - `%` → 取余
    /// - `^` → 幂运算 (Exponentiate)
    ///
    /// **比较运算符**
    /// - `=` → 等于
    /// - `<>` / `!=` → 不等于
    /// - `<` → 小于
    /// - `<=` → 小于等于
    /// - `>` → 大于
    /// - `>=` → 大于等于
    ///
    /// **逻辑运算符**
    /// - `AND`
    /// - `OR`
    ///
    /// **模式匹配运算符**
    /// - `LIKE`
    ///
    /// # 参数
    ///
    /// * `min_precedence` - 最小运算符优先级。
    ///   只有当运算符的优先级大于等于该值时，才会被解析。
    ///
    /// # 返回值
    ///
    /// * `Some(MiddleOp)` - 成功解析到中缀运算符。
    /// * `None` - 当前位置不是中缀运算符，或其优先级过低。
    ///
    /// # 示例
    ///
    /// ```sql
    /// -- SQL 片段
    /// a + b            -- 匹配加法运算符
    /// x * y            -- 匹配乘法运算符
    /// age >= 18        -- 匹配大于等于运算符
    /// name LIKE 'A%'   -- 匹配 LIKE 运算符
    /// flag AND status  -- 匹配 AND 运算符
    /// ```
    ///
    /// ```text
    /// // 假设输入 Token 为 "+"
    /// if let Some(op) = parser.parse_infix_operator_at(0) {
    ///     assert_eq!(op, InfixOperator::Add);
    /// }
    fn parse_mid_op_at(&mut self, min_precedence: Precedence) -> Option<MiddleOp> {
        self.next_if_map(|token| {
            let op = match token {
                Token::Plus => MiddleOp::Add,
                Token::Asterisk => MiddleOp::Multiply,
                Token::Slash => MiddleOp::Divide,
                Token::Equal => MiddleOp::Equal,
                Token::NotEqual => MiddleOp::NotEqual,
                Token::LessThan => MiddleOp::LessThan,
                Token::GreaterThan => MiddleOp::GreaterThan,
                Token::LessOrGreaterThan => MiddleOp::NotEqual,
                Token::GreaterThanOrEqual => MiddleOp::GreaterThanEqual,
                Token::LessThanOrEqual => MiddleOp::LessThanEqual,
                Token::Keyword(Keyword::Like) => MiddleOp::Like,
                Token::Keyword(Keyword::And) => MiddleOp::And,
                Token::Keyword(Keyword::Or) => MiddleOp::Or,
                Token::Minus => MiddleOp::Subtract,
                Token::Percent => MiddleOp::Remainder,
                Token::Caret => MiddleOp::Exponent,
                _ => return None
            };
            Some(op).filter(|o| o.precedence() >= min_precedence)
        })
    }
}

/// Operator precedence.
/// 操作优先级
type Precedence = u8;

/// 优先级别调整
enum Associativity {
    Left,
    Right,
}

/// 表达式标准，默认是从左到右：
/// 左结合 优先级提高
/// 右结合 优先级不变
impl Add<Associativity> for Precedence {
    type Output = Self;
    fn add(self, other: Associativity) -> Self {
        self + match other {
            Associativity::Left => 1,
            Associativity::Right => 0,
        }
    }
}

/// 前缀操作
/// 负号： -a
/// 取反： !a
/// 正号： +a
enum PrefixOp {
    Minus,
    Not,
    Plus,
}

impl PrefixOp {
    fn precedence(&self) -> Precedence {
        match self {
            Self::Minus | Self::Plus => 10,
            Self::Not => 3,
        }
    }

    /// 闭合的时候都不需要提高执行优先级
    fn associativity(&self) -> Associativity {
        Associativity::Right
    }

    ///根据操作构建表达式的抽象语法树
    fn into_expression(self, right: Expression) -> Expression {
        let right = Box::new(right);
        match self {
            Self::Minus => ast::Operator::Negate(right).into(),
            Self::Plus => ast::Operator::Identifier(right).into(),
            Self::Not => ast::Operator::Not(right).into(),
        }
    }
}

/// 常规运算
/// 加：a+b
/// 与：a and b
/// 除：a/b
/// 等：a = b
/// 指数运算： a^b
/// 大于：a>b
/// 大于等于：a>=b
/// 小于：a<b
/// 小于等于：a<=b
/// 模糊匹配：a like b
/// 乘法：a * b
/// 不等于： a!=b
/// 或者： a OR b
/// 取余： a%b
/// 减： a-b
enum MiddleOp {
    Add,
    And,
    Divide,
    Equal,
    Exponent,  // a^b
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
    Like,
    Multiply,
    NotEqual,
    Or,
    Remainder,
    Subtract,
}

impl MiddleOp {
    fn precedence(&self) -> Precedence {
        match self {
            Self::Or => 1,
            Self::And => 2,
            Self::Equal | Self::NotEqual | Self::Like => 4,
            Self::GreaterThan
            | Self::GreaterThanEqual
            | Self::LessThan
            | Self::LessThanEqual => 5,
            Self::Add | Self::Subtract => 6,
            Self::Multiply | Self::Divide | Self::Remainder => 7,
            Self::Exponent => 8
        }
    }

    fn associativity(&self) -> Associativity {
        match self {
            // 本次实现的运算符中只有幂运算^是右结合的
            Self::Exponent => Associativity::Right,
            _ => Associativity::Left,
        }
    }

    fn into_expression(self, left: Expression, right: Expression) -> Expression {
        let (left, right) = (Box::new(left), Box::new(right));
        match self {
            Self::Add => ast::Operator::Add(left, right).into(),
            Self::And => ast::Operator::And(left, right).into(),
            Self::Divide => ast::Operator::Div(left, right).into(),
            Self::Equal => ast::Operator::Eq(left, right).into(),
            Self::Exponent => ast::Operator::Exp(left, right).into(),
            Self::GreaterThan => ast::Operator::Greater(left, right).into(),
            Self::GreaterThanEqual => ast::Operator::GreaterEq(left, right).into(),
            Self::LessThan => ast::Operator::Less(left, right).into(),
            Self::LessThanEqual => ast::Operator::LessEq(left, right).into(),
            Self::Like => ast::Operator::Like(left, right).into(),
            Self::Multiply => ast::Operator::Multiply(left, right).into(),
            Self::NotEqual => ast::Operator::NotEq(left, right).into(),
            Self::Or => ast::Operator::Or(left, right).into(),
            Self::Remainder => ast::Operator::Remainder(left, right).into(),
            Self::Subtract => ast::Operator::Sub(left, right).into(),
        }
    }
}

enum PostfixOp {
    Factor, // 阶乘 a!
    Is(Literal), // a is NULL | NAN
    IsNot(Literal), // a is NOT NULL | NAN
}

impl PostfixOp {
    // The operator precedence.
    fn precedence(&self) -> Precedence {
        match self {
            Self::Is(_) | Self::IsNot(_) => 4,
            Self::Factor => 9,
        }
    }

    /// Builds an AST expression for the operator.
    fn into_expression(self, lhs: ast::Expression) -> ast::Expression {
        let lhs = Box::new(lhs);
        match self {
            Self::Factor => ast::Operator::Factor(lhs).into(),
            Self::Is(v) => ast::Operator::Is(lhs, v).into(),
            Self::IsNot(v) => ast::Operator::Not(ast::Operator::Is(lhs, v).into()).into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::parser::Parser;

    #[test]
    fn parser_create_table() -> crate::db_error::Result<()> {
        // 定义一个建表语句
        let create_table = "create table ACT_RU_JOB (
            ID varchar NOT NULL primary key,
            REV integer,
            LOCK_OWNER varchar,
            EXCLUSIVE boolean,
            RETRIES integer,
            TENANT_ID varchar default ''
        )";
        // 创建语法分析器
        let mut parser = Parser::new(create_table);
        println!("{:?}", parser.parse_statement()?);
        Ok(())
    }

    #[test]
    fn parser_where_clause() -> crate::db_error::Result<()> {
        let where_clause = "WHERE REV IS NOT NULL";
        let mut parser = Parser::new(where_clause);
        println!("{:?}", parser.parse_where()?);
        Ok(())
    }

    #[test]
    fn parser_group_by() -> crate::db_error::Result<()> {
        let group_by = "GROUP BY REV";
        let mut parser = Parser::new(group_by);
        println!("{:?}", parser.parse_group_by()?);
        Ok(())
    }

    #[test]
    fn parser_having() -> crate::db_error::Result<()> {
        let having = "HAVING COUNT(*) > 1";
        let mut parser = Parser::new(having);
        println!("{:?}", parser.parse_having()?);
        Ok(())
    }

    #[test]
    fn parser_order_by() -> crate::db_error::Result<()> {
        let order_by = "ORDER BY A.REV DESC";
        let mut parser = Parser::new(order_by);
        println!("{:?}", parser.parse_order_by()?);
        Ok(())
    }

    #[test]
    fn parser_select() -> crate::db_error::Result<()> {
        let select = "SELECT A.ID, A.REV FROM ACT_RU_JOB A WHERE A.REV IS NOT NULL";
        let mut parser = Parser::new(select);
        println!("{:?}", parser.parse_select()?);
        Ok(())
    }

    #[test]
    fn parser_insert() -> crate::db_error::Result<()> {
        let insert = "INSERT INTO ACT_RU_JOB (ID, REV) VALUES ('1', '2')";
        let mut parser = Parser::new(insert);
        println!("{:?}", parser.parse_insert()?);
        Ok(())
    }

    #[test]
    fn parser_update() -> crate::db_error::Result<()> {
        let update = "UPDATE ACT_RU_JOB SET REV = '2' WHERE ID = '1'";
        let mut parser = Parser::new(update);
        println!("{:?}", parser.parse_update()?);
        Ok(())
    }

    #[test]
    fn parser_delete() -> crate::db_error::Result<()> {
        let delete = "DELETE FROM ACT_RU_JOB WHERE ID = '1'";
        let mut parser = Parser::new(delete);
        println!("{:?}", parser.parse_delete()?);
        Ok(())
    }

    #[test]
    fn parser_explain() -> crate::db_error::Result<()> {
        let explain = "EXPLAIN SELECT A.ID, A.REV FROM ACT_RU_JOB A WHERE A.REV IS NOT NULL";
        let mut parser = Parser::new(explain);
        println!("{:?}", parser.parse_explain()?);
        Ok(())
    }

    #[test]
    fn parser_begin() -> crate::db_error::Result<()> {
        let begin = "BEGIN";
        let mut parser = Parser::new(begin);
        println!("{:?}", parser.parse_begin()?);
        Ok(())
    }

    #[test]
    fn parser_commit() -> crate::db_error::Result<()> {
        let commit = "COMMIT";
        let mut parser = Parser::new(commit);
        println!("{:?}", parser.parse_commit()?);
        Ok(())
    }

    #[test]
    fn parser_rollback() -> crate::db_error::Result<()> {
        let rollback = "ROLLBACK";
        let mut parser = Parser::new(rollback);
        println!("{:?}", parser.parse_rollback()?);
        Ok(())
    }
}
