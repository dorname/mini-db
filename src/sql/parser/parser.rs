use super::ast::{Column, Expression, Literal, Statement};
use crate::db_error::Result;
use crate::errinput;
use crate::sql::parser::ast;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::types::DataType;
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
        self.peek().ok()?.filter(|token| predicate(token));
        self.next().ok()
    }

    fn next_is(&mut self, token: Token) -> bool {
        self.next_predicate(|tk| token.eq(tk)).is_some()
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
            if !self.next_is(Token::Comma) {
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

    /// 使用“优先级爬升算法（precedence climbing algorithm）”解析表达式。参考：
    ///
    /// <https://zh.wikipedia.org/wiki/运算符优先级解析#优先级爬升法>
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

    /// 在给定的最小优先级下解析一个表达式。
    ///
    /// 该函数会根据“优先级爬升算法”递归地解析表达式，处理前缀运算符、
    /// 原子表达式、中缀运算符和后缀运算符。其核心规则是：
    ///
    /// 1. **前缀运算符**
    ///    - 如果左操作数是前缀运算符，则递归解析其右操作数，并根据前缀运算符的
    ///      优先级与结合律决定递归深度。
    ///    - 否则，将左操作数解析为原子（数值、变量、函数、括号表达式等）。
    ///
    /// 2. **后缀运算符（第一阶段）**
    ///    - 如果存在后缀运算符（例如 `!`、`IS NULL`），立即作用在当前左操作数上。
    ///
    /// 3. **中缀运算符**
    ///    - 只要下一个中缀运算符的优先级大于等于当前的最小优先级，就会继续解析。
    ///    - 对右操作数递归调用本函数，并传入新的最小优先级（由运算符的优先级和结合律决定）。
    ///    - 最终将中缀运算符应用到左、右操作数上，形成新的表达式。
    ///
    /// 4. **后缀运算符（第二阶段）**
    ///    - 在处理完一个中缀运算符及其右操作数后，还需要再次检查是否存在后缀运算符，
    ///      并将其应用到当前表达式上。例如：`1 + NULL IS NULL`。
    ///
    /// 算法会持续递归，直到遇到优先级更低的运算符为止，
    /// 然后返回当前解析完成的子表达式。
    fn parse_expression_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        todo!()
    }

    fn parse_prefix_op_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        todo!()
    }
    fn parse_expression_atom() -> Result<Expression> {
        todo!()
    }

    fn parse_postfix_op_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        todo!()
    }

    fn parse_mid_op_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        todo!()
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
/// 取反： not
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
    fn associativity() -> Associativity {
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
    Exponent,
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
