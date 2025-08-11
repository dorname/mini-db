use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::string::FromUtf8Error;
use std::sync::PoisonError;
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
    /// 服务器错误
    ServerError(String),
    /// bincode 编码错误
    EncodeError(String),
    /// bincode 解码错误
    DecodeError(String),
    /// keycode 序列化类型
    SerializationError(String),
    /// TryFromIntError
    TryFromIntError(String),
    /// keycode 反序列化错误类型
    DeserializationError(String),
    /// 获取锁错误
    PoisonError(String),
    /// 输入错误
    UnExpectedInput(String),
}

/// 自定义错误类型
pub type Result<T> = std::result::Result<T, Error>;

/// 实现标准库std::error::Error特征
impl std::error::Error for Error {}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    #[doc = r" Used when a [`Serialize`] implementation encounters any error"]
    #[doc = r" while serializing a type."]
    #[doc = r""]
    #[doc = r" The message should not be capitalized and should not end with a"]
    #[doc = r" period."]
    #[doc = r""]
    #[doc = r" For example, a filesystem [`Path`] may refuse to serialize"]
    #[doc = r" itself if it contains invalid UTF-8 data."]
    #[doc = r""]
    #[doc = r" ```edition2021"]
    #[doc = r" # struct Path;"]
    #[doc = r" #"]
    #[doc = r" # impl Path {"]
    #[doc = r" #     fn to_str(&self) -> Option<&str> {"]
    #[doc = r" #         unimplemented!()"]
    #[doc = r" #     }"]
    #[doc = r" # }"]
    #[doc = r" #"]
    #[doc = r" use serde::ser::{self, Serialize, Serializer};"]
    #[doc = r""]
    #[doc = r" impl Serialize for Path {"]
    #[doc = r"     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>"]
    #[doc = r"     where"]
    #[doc = r"         S: Serializer,"]
    #[doc = r"     {"]
    #[doc = r"         match self.to_str() {"]
    #[doc = r"             Some(s) => serializer.serialize_str(s),"]
    #[doc = r#"             None => Err(ser::Error::custom("path contains invalid UTF-8 characters")),"#]
    #[doc = r"         }"]
    #[doc = r"     }"]
    #[doc = r" }"]
    #[doc = r" ```"]
    #[doc = r""]
    #[doc = r" [`Path`]: std::path::Path"]
    #[doc = r" [`Serialize`]: crate::Serialize"]
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Self::SerializationError(msg.to_string())
    }
}
/// 实现格式输出
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "opertion aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data:{msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
            Error::ParserError(msg) => write!(f, "parser error:{msg}"),
            Error::ReadOnly => write!(f, "error: readonly"),
            Error::Serialization => write!(f, "error: Serialization"),
            Error::ConfigError(msg) => write!(f, "error: config error:{msg}"),
            Error::ConfigWatcherError(msg) => write!(f, "error: config watcher error:{msg}"),
            Error::ServerError(msg) => write!(f, "error: server error:{msg}"),
            Error::EncodeError(msg) => write!(f, "error:binCoder encode error{msg}"),
            Error::DecodeError(msg) => write!(f, "error:binCoder decode error{msg}"),
            Error::SerializationError(msg) => write!(f, "error:Serialization error:{msg}"),
            Error::TryFromIntError(msg) => write!(f, "error:TryFromIntError error:{msg}"),
            Error::DeserializationError(msg) => write!(f, "error:Deserialization error:{msg}"),
            Error::PoisonError(msg) => write!(f, "error:PoisonError error:{msg}"),
            Error::UnExpectedInput(msg) => write!(f, "error:unexpectedInput:{msg}"),
        }
    }
}

/// 构建一个结构体实例
/// an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => {
        $crate::db_error::Error::InvalidData(format!($($args)*)).into()
    };
}

#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => {
        $crate::db_error::Error::UnExpectedInput(format!($($args)*)).into()
    };
}


impl<T> From<Error> for Result<T> {
    fn from(error: Error) -> Self {
        Err(error)
    }
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

impl From<axum::Error> for Error {
    fn from(err: axum::Error) -> Self {
        Error::ServerError(err.to_string())
    }
}

impl From<bincode::error::EncodeError> for Error {
    fn from(err: bincode::error::EncodeError) -> Self {
        Error::EncodeError(err.to_string())
    }
}

impl From<bincode::error::DecodeError> for Error {
    fn from(err: bincode::error::DecodeError) -> Self {
        Error::DecodeError(err.to_string())
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::TryFromIntError(err.to_string())
    }
}
impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::TryFromIntError(err.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::TryFromIntError(err.to_string())
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::PoisonError(value.to_string())
    }
}
#[cfg(test)]
mod tests {
    use crate::db_error;

    #[test]
    fn test_err_data() -> db_error::Result<()> {
        errdata!("test")
    }
}
