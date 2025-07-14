use serde::{Deserialize, Serialize};

use crate::cfg::Config;
/// 自定义错误信息
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// 阻塞操作，必须进行重试
    Abort,
    /// 无效数据
    InvalidData(String),
    /// 无效查询语句
    ParserError(String),
    /// 文件IO错误
    IO(String),
    /// 只读事务
    ReadOnly,
    /// 串行事务冲突：由不同writer，必须进行重试
    Serialization,
    //配置错误
    ConfigError(String),
    /// 配置监听错误
    ConfigWatcherError(String),
    /// Mutex 锁错误
    MutexError(String),
}

/// 自定义错误类型
pub type Result<T> = std::result::Result<T, Error>;

/// 实现标准库std::error::Error特征
impl std::error::Error for Error {}

/// 实现格式输出
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "opertion aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data:{msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
            Error::ParserError(msg) => write!(f, "parser error:{msg}"),
            Error::ReadOnly => write!(f, "error: readonly"),
            Error::Serialization => write!(f, "error: Serialization"),
            Error::ConfigError(msg) => write!(f,"error: config error:{msg}"),
            Error::ConfigWatcherError(msg) => write!(f,"error: config watcher error:{msg}"),
            Error::MutexError(msg) => write!(f,"error: mutex error:{msg}"),
        }
    }
}

/// 构建一个结构体实例
/// an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => {
        $crate::db_error::Error::InvalidData(format!($($args)*))
    };
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        // 根据你的错误定义转换，比如包裹成一个枚举成员或自定义错误
        Error::IO(err.to_string())
    }
}

impl From<toml::de::Error> for Error {
    fn from(err: toml::de::Error) -> Self {
        Error::ConfigError(err.to_string())
    }
}

impl From<notify::Error> for Error {
    fn from(err: notify::Error) -> Self {
        Error::ConfigWatcherError(err.to_string())
    }
}

impl From<std::sync::PoisonError<std::sync::MutexGuard<'_, Config>>> for Error {
    fn from(err: std::sync::PoisonError<std::sync::MutexGuard<'_, Config>>) -> Self {
        Error::MutexError(err.to_string())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_errdata() {
        errdata!("test");
    }
}
