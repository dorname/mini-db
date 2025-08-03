use crate::db_error::Result;
use serde::de::DeserializeOwned;
/// bin_encoder 关于值的序列化工具
/// 因为值不需要支持前缀范围扫描，所以使用bincode库进行序列化
use serde::{Deserialize, Serialize};

/// 初始化bincode的配置
/// 默认配置小端序
const CONFIG: bincode::config::Configuration = bincode::config::standard();

/// 用于对值进行序列化
pub fn encode<T: Serialize>(value: T) -> Result<Vec<u8>> {
    Ok(bincode::serde::encode_to_vec(value, CONFIG)?)
}
/// 用于反序列化成值
pub fn decode<'de, T: Deserialize<'de>>(value: &'de [u8]) -> Result<T> {
    Ok(bincode::serde::borrow_decode_from_slice(value, CONFIG)?.0)
}

/// 用于可写入文件的序列化方法
/// 实现了Write特征的一个目标：代表一个“可写入”的目标，比如 File、Vec<u8>、TcpStream 等。
pub fn encode_into<W, T>(writer: &mut W, value: &T) -> Result<()>
where
    W: std::io::Write,
    T: Serialize,
{
    bincode::serde::encode_into_std_write(value, writer, CONFIG)?;
    Ok(())
}

/// 用于可读取文件的反序列化方法
/// 实现了Reader
pub fn decode_from<R, T>(mut reader: R) -> Result<T>
where
    R: std::io::Read,
    T: DeserializeOwned,
{
    Ok(bincode::serde::decode_from_std_read(&mut reader, CONFIG)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{Decode, Encode};
    #[test]
    #[ignore]
    fn test_bincode() {
        #[derive(Encode, Decode, Debug)]
        pub struct Person {
            name: String,
            age: u8,
        }
        let person = Person { name: "John".to_string(), age: 30 };
        // 测试bincode序列化，已经实现了Encode, Decode
        let encoded = bincode::encode_to_vec(&person, bincode::config::standard()).unwrap();
        let (decoded, _): (Person, _) = bincode::decode_from_slice(&encoded, bincode::config::standard()).unwrap();

        println!("encoded: {:?}", encoded);
        println!("decoded: {:?}", decoded);

        assert_eq!(person.name, decoded.name);
        assert_eq!(person.age, decoded.age);

        //测试u8类型
        let u8_value: u8 = 100;
        let encoded_u8 = bincode::encode_to_vec(&u8_value, bincode::config::standard()).unwrap();
        let (decoded_u8, _): (u8, _) = bincode::decode_from_slice(&encoded_u8, bincode::config::standard()).unwrap();
        println!("encoded_u8: {:?}", encoded_u8);
        assert_eq!(u8_value, decoded_u8);

        //测试u16类型
        let u16_value: u16 = 10000;
        let encoded_u16 = bincode::encode_to_vec(&u16_value, bincode::config::standard()).unwrap();
        let (decoded_u16, _): (u16, _) = bincode::decode_from_slice(&encoded_u16, bincode::config::standard()).unwrap();
        println!("encoded_u16: {:?}", encoded_u16);
        assert_eq!(u16_value, decoded_u16);
    }

    #[test]
    fn test_encode() -> Result<()> {
        println!("{:?}", encode("test")?);
        println!("{:?}", decode::<&str>(&[4, 116, 101, 115, 116])?);
        assert_eq!("test", decode::<&str>(&[4, 116, 101, 115, 116])?);
        Ok(())
    }
}
