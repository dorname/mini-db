use itertools::Itertools;

/// 格式化打印输出
pub trait Formatter {
    /// 格式化键值
    fn key(key: &[u8]) -> String;

    /// 格式化值，需要根据键来决定值的类型
    fn value(key: &[u8], value: &[u8]) -> String;

    /// 格式化 键值对
    fn key_value(key: &[u8], value: &[u8]) -> String {
        Self::key_maybe_value(key, Some(value))
    }

    /// 格式化 键值对 运行值位空的清空
    #[allow(non_camel_case_types)]
    fn key_maybe_value(key: &[u8], value: Option<&[u8]>) -> String {
        let fmtkey = Self::key(key);
        let fmtvalue = value.map_or("None".to_string(), |v| Self::value(key, v));
        format!("{fmtkey} → {fmtvalue}")
    }
}

/// 二进制结构体：表示未经过解码的字节数组
pub struct Raw;

impl Raw {
    /// Formats raw bytes as escaped ASCII strings.
    pub fn bytes(bytes: &[u8]) -> String {
        let escaped = bytes.iter().copied().flat_map(std::ascii::escape_default).collect_vec();
        format!("\"{}\"", String::from_utf8_lossy(&escaped))
    }
}
impl Formatter for Raw {
    fn key(key: &[u8]) -> String {
        Self::bytes(key)
    }
    fn value(_key: &[u8], value: &[u8]) -> String {
        Self::bytes(value)
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::Raw;

    #[test]
    fn test_format() {
        let input = b"\x00\x7fABC";
        let input_1 = b"hello world";
        println!("{:?}", input);
        println!("{:?}", input_1);
        // let output = String::from_utf8_lossy(input).to_string();
        let output = Raw::bytes(input);
        let output_1 = Raw::bytes(input_1);
        println!("{}", output);
        println!("{}", output_1);
        assert_eq!(output, "\"\\x00\\x7fABC\"");
    }
}
