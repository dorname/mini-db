use crate::errinput;
use std::fmt::{Debug, Display};
use std::iter::Peekable;
use std::str::Chars;

/// 词法解析Token
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Number(String),
    String(String),
    Identifier(String), // 普通标志符
    Keyword(Keyword),
    Period,             // .
    Equal,              // =
    NotEqual,           // !=
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    LessOrGreaterThan,  // <>
    Plus,               // +
    Minus,              // -
    Asterisk,           // *
    Slash,              // /
    Caret,              // ^
    Percent,            // %
    Exclamation,        // !
    Question,           // ?
    Comma,              // ,
    Semicolon,          // ;
    OpenParen,          // (
    CloseParen,         // )
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Number(n) => n,
            Self::String(s) => s,
            Self::Identifier(s) => s,
            Self::Keyword(k) => return std::fmt::Display::fmt(&k, f),
            Self::Period => ".",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::LessOrGreaterThan => "<>",
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Asterisk => "*",
            Self::Slash => "/",
            Self::Caret => "^",
            Self::Percent => "%",
            Self::Exclamation => "!",
            Self::Question => "?",
            Self::Comma => ",",
            Self::Semicolon => ";",
            Self::OpenParen => "(",
            Self::CloseParen => ")",
        })
    }
}

impl From<Keyword> for Token {
    fn from(key: Keyword) -> Self {
        Self::Keyword(key)
    }
}

/// 词法关键字
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Keyword {
    And,
    As,
    Asc,
    Begin,
    Bool,
    Boolean,
    By,
    Commit,
    Create,
    Cross,
    Default,
    Delete,
    Desc,
    Double,
    Drop,
    Exists,
    Explain,
    False,
    Float,
    From,
    Group,
    Having,
    If,
    Index,
    Infinity,
    Inner,
    Insert,
    Int,
    Integer,
    Into,
    Is,
    Join,
    Key,
    Left,
    Like,
    Limit,
    NaN,
    Not,
    Null,
    Of,
    Offset,
    On,
    Only,
    Or,
    Order,
    Outer,
    Primary,
    Read,
    References,
    Right,
    Rollback,
    Select,
    Set,
    String,
    System,
    Table,
    Text,
    Time,
    Transaction,
    True,
    Unique,
    Update,
    Union,
    Values,
    Varchar,
    Where,
    Write,
}
impl TryFrom<&str> for Keyword {
    // Use a cheap static error string. This just indicates it's not a keyword.
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Only compare lowercase, which is enforced by the lexer. This avoids
        // allocating a string to change the case. Assert this.
        debug_assert!(value.chars().all(|c| !c.is_uppercase()), "keyword must be lowercase");
        Ok(match value {
            "as" => Self::As,
            "asc" => Self::Asc,
            "and" => Self::And,
            "begin" => Self::Begin,
            "bool" => Self::Bool,
            "boolean" => Self::Boolean,
            "by" => Self::By,
            "commit" => Self::Commit,
            "create" => Self::Create,
            "cross" => Self::Cross,
            "default" => Self::Default,
            "delete" => Self::Delete,
            "desc" => Self::Desc,
            "double" => Self::Double,
            "drop" => Self::Drop,
            "exists" => Self::Exists,
            "explain" => Self::Explain,
            "false" => Self::False,
            "float" => Self::Float,
            "from" => Self::From,
            "group" => Self::Group,
            "having" => Self::Having,
            "if" => Self::If,
            "index" => Self::Index,
            "infinity" => Self::Infinity,
            "inner" => Self::Inner,
            "insert" => Self::Insert,
            "int" => Self::Int,
            "integer" => Self::Integer,
            "into" => Self::Into,
            "is" => Self::Is,
            "join" => Self::Join,
            "key" => Self::Key,
            "left" => Self::Left,
            "like" => Self::Like,
            "limit" => Self::Limit,
            "nan" => Self::NaN,
            "not" => Self::Not,
            "null" => Self::Null,
            "of" => Self::Of,
            "offset" => Self::Offset,
            "on" => Self::On,
            "only" => Self::Only,
            "or" => Self::Or,
            "order" => Self::Order,
            "outer" => Self::Outer,
            "primary" => Self::Primary,
            "read" => Self::Read,
            "references" => Self::References,
            "right" => Self::Right,
            "rollback" => Self::Rollback,
            "select" => Self::Select,
            "set" => Self::Set,
            "string" => Self::String,
            "system" => Self::System,
            "table" => Self::Table,
            "text" => Self::Text,
            "time" => Self::Time,
            "transaction" => Self::Transaction,
            "true" => Self::True,
            "unique" => Self::Unique,
            "update" => Self::Update,
            "union" => Self::Union,
            "values" => Self::Values,
            "varchar" => Self::Varchar,
            "where" => Self::Where,
            "write" => Self::Write,
            _ => return Err("not a keyword"),
        })
    }
}
impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::And => "AND",
            Self::Begin => "BEGIN",
            Self::Bool => "BOOL",
            Self::Boolean => "BOOLEAN",
            Self::By => "BY",
            Self::Commit => "COMMIT",
            Self::Create => "CREATE",
            Self::Cross => "CROSS",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Double => "DOUBLE",
            Self::Drop => "DROP",
            Self::Exists => "EXISTS",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::Float => "FLOAT",
            Self::From => "FROM",
            Self::Group => "GROUP",
            Self::Having => "HAVING",
            Self::If => "IF",
            Self::Index => "INDEX",
            Self::Infinity => "INFINITY",
            Self::Inner => "INNER",
            Self::Insert => "INSERT",
            Self::Int => "INT",
            Self::Integer => "INTEGER",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Left => "LEFT",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::NaN => "NAN",
            Self::Not => "NOT",
            Self::Null => "NULL",
            Self::Of => "OF",
            Self::Offset => "OFFSET",
            Self::On => "ON",
            Self::Only => "ONLY",
            Self::Outer => "OUTER",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Primary => "PRIMARY",
            Self::Read => "READ",
            Self::References => "REFERENCES",
            Self::Right => "RIGHT",
            Self::Rollback => "ROLLBACK",
            Self::Select => "SELECT",
            Self::Set => "SET",
            Self::String => "STRING",
            Self::System => "SYSTEM",
            Self::Table => "TABLE",
            Self::Text => "TEXT",
            Self::Time => "TIME",
            Self::Transaction => "TRANSACTION",
            Self::True => "TRUE",
            Self::Unique => "UNIQUE",
            Self::Update => "UPDATE",
            Self::Union => "UNION",
            Self::Values => "VALUES",
            Self::Varchar => "VARCHAR",
            Self::Where => "WHERE",
            Self::Write => "WRITE",
        })
    }
}

/// # 词法分析器 SQL 语法分析拆解
///
/// ## 1. **建表**
///
/// ```sql
/// create table `table_name` ( `column_name` data_type column_constraint, ... );
/// ```
///
/// - **关键字**：`create`、`table`
/// - **参数**：
///   - 表名：`table_name`
///   - 列数组：(`column_name` **列名**, `data_type` **列类型**, `column_constraint` **列约束**, ...)
///
/// ---
///
/// ## 2. **插入**
///
/// ```sql
/// insert into `table_name` (`column1_name`, `column2_name`, ...)
/// values (`value1`, `value2`, ...);
/// ```
///
/// ---
///
/// ## 3. **更新**
///
/// ```sql
/// update `table_name`
/// set `column1_name` = `column1_value`, ...
/// where expression
/// group by `column_name`, ...
/// order by `column_name`;
/// ```
///
/// ---
///
/// ## 4. **删除**
///
/// ```sql
/// delete from `table_name`
/// where expression
/// group by `column_name`, ...
/// order by `column_name`;
/// ```
pub struct Lexer<'a> {
    chars: Peekable<Chars<'a>>,
}

impl Iterator for Lexer<'_> {
    type Item = crate::db_error::Result<Token>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.scan_token() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => self.chars.peek().map(|c| errinput!("unexpected character {c}")),
            Err(err) => Some(Err(err)),
        }
    }
}
impl<'a> Lexer<'a> {
    /// 创建一个解析器结构体：
    ///
    /// 输入 `input`链式调用[`str::chars`] 和 [`Iterator::peekable`]函数
    /// 返回一个[`Peekable`]的迭代器
    pub fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
        }
    }

    /// 返回下一个字符（`char`），仅当它满足给定的谓词条件。
    ///
    /// # 行为描述
    ///
    /// 该方法会**预览**迭代器的下一个字符，并判断它是否满足指定条件：
    /// - 如果满足条件：消费并返回该字符。
    /// - 如果不满足条件：不消费字符，返回 `None`。
    ///
    /// ## 处理步骤
    ///
    /// 1. 调用 [`peek`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.peek) 查看下一个字符（`Option<&char>`）。
    /// 2. 使用 [`Option::filter`](https://doc.rust-lang.org/std/option/enum.Option.html#method.filter) 对字符引用进行条件判断（谓词 `predicate`）。
    ///    - 若返回 `None`，直接提前返回 `None`（使用 `?` 运算符）。
    /// 3. 如果条件满足，调用 [`next`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#tymethod.next) 消费并返回该字符。
    ///
    /// # 参数
    /// - `predicate`：一个函数或闭包，接收 `&char` 并返回 `bool`，用于判断字符是否符合条件。
    ///
    /// # 返回值
    /// - `Some(char)`：下一个字符满足条件，并被消费。
    /// - `None`：下一个字符不存在或不满足条件，迭代器位置保持不变。
    ///
    /// # 示例
    /// ```rust,ignore
    /// let mut iter = "abc123".chars().peekable();
    ///
    /// // 条件：字母
    /// let c = next_char_predicate(&mut iter, |c| c.is_alphabetic());
    /// assert_eq!(c, Some('a')); // 消费 'a'
    ///
    /// // 条件：数字
    /// let c = next_char_predicate(&mut iter, |c| c.is_ascii_digit());
    /// assert_eq!(c, None); // 下一个是 'b'，不满足条件
    /// ```
    fn next_char_predicate<F>(&mut self, predicate: F) -> Option<char>
    where
        F: Fn(&char) -> bool,
    {
        self.chars.peek().filter(|&c| predicate(c))?;
        self.chars.next()
    }

    fn next_is(&mut self, ch: char) -> bool { self.next_char_predicate(|c| ch.eq(c)).is_some() }

    fn next_map<F, T>(&mut self, map: F) -> Option<T>
    where
        F: Fn(&char) -> Option<T>,
    {
        // 获取下一个元素
        let value = self.chars.peek().copied().and_then(|c| map(&c))?;
        self.chars.next();
        Some(value)
    }

    fn scan_symbol(&mut self) -> Option<Token> {
        let mut token = self.next_map(|c| {
            Some(match c {
                '.' => Token::Period,
                ',' => Token::Comma,
                '=' => Token::Equal,
                '>' => Token::GreaterThan,
                '<' => Token::LessThan,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '/' => Token::Slash,
                '*' => Token::Asterisk,
                '%' => Token::Percent,
                '^' => Token::Caret,
                '!' => Token::Exclamation,
                '?' => Token::Question,
                ';' => Token::Semicolon,
                '(' => Token::OpenParen,
                ')' => Token::CloseParen,
                _ => return None,
            })
        })?;
        token = match token {
            Token::Exclamation if self.next_is('=') => Token::NotEqual,
            Token::GreaterThan if self.next_is('=') => Token::GreaterThanOrEqual,
            Token::LessThan if self.next_is('=') => Token::LessThanOrEqual,
            Token::LessThan if self.next_is('>') => Token::LessOrGreaterThan,
            token => token,
        };
        Some(token)
    }
    /// 扫描并返回下一个 `Token`（如果有的话）。
    ///
    /// # 行为描述
    ///
    /// 该方法会按以下步骤扫描输入流：
    ///
    /// 1. **跳过空白字符**
    ///    调用 [`skip_whitespace`](#method.skip_whitespace) 忽略空格、制表符、换行等空白。
    ///
    /// 2. **检查是否到达输入末尾**
    ///    - 使用 [`peek`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.peek) 预览下一个字符；
    ///    - 如果没有下一个字符（`None`），返回 `Ok(None)` 表示没有更多 Token。
    ///
    /// 3. **根据首字符判断 Token 类型并分派到相应的扫描方法**：
    ///    - `'\'` → 调用 [`scan_string`](#method.scan_string) 扫描字符串字面量。
    ///    - `'"'` → 调用 [`scan_ident_quoted`](#method.scan_ident_quoted) 扫描带引号的标识符。
    ///    - `'0'..='9'` → 调用 [`scan_number`](#method.scan_number) 扫描数字字面量。
    ///    - **字母字符**（`is_alphabetic()` 为真） → 调用 [`scan_ident_or_keyword`](#method.scan_ident_or_keyword) 扫描标识符或关键字。
    ///    - **其它字符** → 调用 [`scan_symbol`](#method.scan_symbol) 扫描符号（如运算符、分隔符等）。
    ///
    /// # 返回值
    /// - `Ok(Some(Token))`：成功扫描到一个 Token。
    /// - `Ok(None)`：输入已结束，没有更多 Token。
    /// - `Err(...)`：扫描过程中出现错误。
    ///
    /// # 示例
    /// ```rust,ignore
    /// let mut lexer = Lexer::new("SELECT * FROM table");
    /// while let Some(token) = lexer.scan().unwrap() {
    ///     println!("{:?}", token);
    /// }
    /// ```
    fn scan_token(&mut self) -> crate::db_error::Result<Option<Token>> {
        //1、跳过空白字符串
        self.skip_whitespace();

        //2、读取下一个元素，注意这里通过peek去借用
        // dbg!(self.chars.peek());
        let Some(c) = self.chars.peek() else {
            return Ok(None);
        };
        //3、根据元素类型，决定后续使用什么方法展开扫描
        match c {
            '\'' => self.scan_string(),
            '"' => self.scan_quoted(),
            '0'..='9' => Ok(self.scan_number()),
            c if c.is_alphabetic() => Ok(self.scan_keyword_or_identifier()),
            _ => Ok(self.scan_symbol())
        }
    }


    /// 扫描并返回下一个 **标识符 (identifier)** 或 **关键字 (keyword)**。
    ///
    /// # 行为描述
    ///
    /// 该方法按 SQL 规则解析**未加引号**的标识符，并将其转换为小写（SQL 关键字通常大小写不敏感，约定使用小写）。
    /// 如果解析结果与已定义的关键字表匹配，则返回关键字 token；否则返回普通标识符 token。
    ///
    /// ## 处理步骤
    ///
    /// 1. **扫描首字符**
    ///    - 必须是**字母字符**（`is_alphabetic()`）。
    ///    - 如果第一个字符不是字母，返回 `None` 表示不匹配标识符。
    ///    - 将首字符转换为小写并放入结果字符串 `name`。
    ///
    /// 2. **扫描剩余字符**
    ///    - 允许的字符包括：字母、数字、下划线（`_`）。
    ///    - 连续读取符合条件的字符，将它们转换为小写并追加到 `name`。
    ///
    /// 3. **关键字匹配**
    ///    - 调用 [`Keyword::try_from`] 尝试将 `name` 转换为关键字枚举。
    ///    - 如果成功，返回 `Some(Token::Keyword(keyword))`。
    ///
    /// 4. **默认返回标识符**
    ///    - 如果关键字匹配失败，返回 `Some(Token::Ident(name))`。
    ///
    /// # 返回值
    /// - `Some(Token::Keyword)`：匹配到 SQL 关键字。
    /// - `Some(Token::Ident)`：匹配到普通标识符。
    /// - `None`：首字符不是字母，不是有效的标识符。
    ///
    /// # 示例
    /// ```rust,ignore
    /// // 解析关键字
    /// let mut lexer = Lexer::new("select");
    /// assert_eq!(lexer.scan_ident_or_keyword(), Some(Token::Keyword(Keyword::Select)));
    ///
    /// // 解析普通标识符
    /// let mut lexer = Lexer::new("user_name1");
    /// assert_eq!(lexer.scan_ident_or_keyword(), Some(Token::Ident("user_name1".into())));
    /// ```
    fn scan_keyword_or_identifier(&mut self) -> Option<Token> {
        let mut result = self.next_char_predicate(|c| c.is_alphabetic())?
            .to_lowercase()
            .to_string();

        while let Some(c) = self.next_char_predicate(|c| c.is_alphanumeric() || '_'.eq(c)) {
            result.extend(c.to_lowercase());
        }

        // 判断是否能和关键字匹配上
        // 是 => 返回对应的关键字token
        // 否 => 返回普通标识符
        if let Ok(keyword) = Keyword::try_from(result.as_str()) {
            return Some(Token::Keyword(keyword));
        }
        Some(Token::Identifier(result))
    }

    fn scan_quoted(&mut self) -> crate::db_error::Result<Option<Token>> {
        //1、判断第一个字符是不是"号
        if !self.next_is('"') {
            return Ok(None);
        }
        let mut result_str = String::new();
        loop {
            match self.chars.next() {
                Some('"') if self.next_is('"') => return Ok(None),
                Some('"') => break,
                Some(c) => result_str.push(c),
                None => return errinput!("unexpected end of quoted identifier")
            }
        }
        Ok(Some(Token::Identifier(result_str)))
    }

    /// 字符串扫描规则：字符串是由两个单引号引起来的`'test'`
    ///
    /// 不断的扫描字符知道`\'`闭合跳出循环
    fn scan_string(&mut self) -> crate::db_error::Result<Option<Token>> {
        //1、判断第一个字符是不是'符号,不是则
        if !self.next_is('\'') {
            return Ok(None);
        }
        let mut result_str = String::new();
        loop {
            match self.chars.next() {
                Some('\'')  if self.next_is('\'') => result_str.push('\''),
                Some('\'') => break,
                Some(c) => result_str.push(c),
                None => return errinput!("Unexpected end of string literal"),
            }
        }
        Ok(Some(Token::String(result_str)))
    }

    /// 扫描并返回下一个 **数字字面量 Token**（如果存在）。
    ///
    /// # 解析逻辑
    ///
    /// 数字字面量的格式支持：
    /// - **整数部分**（必需，至少 1 位数字）
    /// - 可选的 **小数部分**（由 `.` 开头，后跟数字）
    /// - 可选的 **科学计数法指数部分**（`e` 或 `E`，可带正负号）
    ///
    /// ## 处理步骤
    ///
    /// 1. **扫描整数部分**
    ///    - 使用 [`next_char_predicate`](#method.next_char_predicate) 获取第一个必须为数字的字符（`0-9`）。
    ///    - 若第一个字符不是数字，直接返回 `None`，表示不匹配数字 token。
    ///    - 继续读取后续连续的数字并追加到字符串缓冲区。
    ///
    /// 2. **扫描小数部分（可选）**
    ///    - 如果下一个字符是 `.`，将其加入结果字符串。
    ///    - 再读取所有连续的数字字符追加到结果中。
    ///
    /// 3. **扫描指数部分（可选）**
    ///    - 如果下一个字符是 `e` 或 `E`，将其加入结果字符串。
    ///    - 可选地读取一个符号字符（`+` 或 `-`）追加到结果。
    ///    - 读取所有连续的数字字符并追加。
    ///
    /// 4. **构造 Token**
    ///    - 将最终得到的数字字符串封装为 [`Token::Number`] 并返回 `Some(...)`。
    ///
    /// # 返回值
    /// - `Some(Token::Number)`：成功扫描到数字字面量。
    /// - `None`：下一个字符不是数字，未扫描到数字 token。
    ///
    /// # 示例
    /// ```rust,ignore
    /// // 能识别的数字格式示例：
    /// // 整数
    /// 42
    /// // 小数
    /// 3.14
    /// // 科学计数法
    /// 6.02e23
    /// -1.23E-4
    /// ```
    fn scan_number(&mut self) -> Option<Token> {
        // 扫描整数部分
        let mut number = self.next_char_predicate(|e| e.is_ascii_digit())?.to_string();
        while let Some(c) = self.next_char_predicate(|e| e.is_ascii_digit()) {
            number.push(c);
        }
        // 扫描小数部分
        if self.next_is('.') {
            number.push('.');
            // 如果下一个字符是数字，则依次添加到number直到遇到非数值类型为止
            while let Some(n) = self.next_char_predicate(|c| c.is_ascii_digit()) {
                number.push(n);
            }
        }

        // 扫描指数类型
        if let Some(exp) = self.next_char_predicate(|c| 'e'.eq(c) || 'E'.eq(c)) {
            number.push(exp);
            if let Some(sign) = self.next_char_predicate(|c| '+'.eq(c) || '-'.eq(c)) {
                number.push(sign);
            }
            while let Some(ch) = self.next_char_predicate(|c| c.is_ascii_digit()) {
                number.push(ch);
            }
        }
        Some(Token::Number(number))
    }

    /// 消耗掉空字符串
    fn skip_whitespace(&mut self) {
        while self.next_char_predicate(|c| c.is_whitespace()).is_some() {}
    }
}

/// 判断整个字符串是否全部由标识符组成
fn is_identifier(input: &str) -> bool {
    let mut lexer = Lexer::new(input);
    let Some(Ok(Token::Identifier(_))) = lexer.next() else { return false; };
    lexer.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_lexer_create() {
        let lexer = Lexer::new("create table test;");
        for ch in lexer.chars {
            println!("{}", ch);
        }
    }

    #[test]
    fn test_peek_try() {
        let mut lexer = Lexer::new("create table test;");
        // 扫描是从头开始扫描的
        let res = lexer.chars.peek().filter(|&c| *c == 'c');
        println!("{:?}", res);
    }

    #[test]
    fn test_scan_string() -> crate::db_error::Result<()> {
        let mut lexer = Lexer::new("'hello'");
        let result = lexer.scan_token()?;
        println!("{:?}", result);
        Ok(())
    }

    #[test]
    fn test_scan_quoted() -> crate::db_error::Result<()> {
        let mut lexer = Lexer::new("\"hello\"");
        let result = lexer.scan_token()?;
        println!("{:?}", result);
        Ok(())
    }

    #[test]
    fn test_lexer() -> crate::db_error::Result<()> {
        let mut lexer = Lexer::new("select
  t.*
  from
  (
    select
      id as id,
      display_name as name,
      biz_url as url,
      type as bizType,
      2 as type,
      'null' as html,
      'null' as description,
      updated_time as publish_time
    from
      tb_ct_biz
    union
    select
      info.content_id as id,
      title as name,
      'null' as url,
      'null' as bizType,
       1  as type,
      content as html,
      tcp.cover_description as description,
      tcp.publish_time as publish_time
    from
      tb_ct_info info
      inner join  tb_ct_publish tcp on info.content_id = tcp.content_id
    where tcp.state = 5 ) t
  order by match_score desc,t.publish_time desc,t.html desc;");
        let mut tokens = Vec::<String>::new();
        while let Some(token) = lexer.scan_token()? {
            tokens.push(token.to_string());
        }
        println!("{:?}", tokens.join(" "));
        Ok(())
    }
}
