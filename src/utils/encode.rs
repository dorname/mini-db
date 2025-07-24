use serde::Serialize;
use crate::db_error::Result;

/// encode 模块
/// 目标是把任意类型可以序列化成字节数组
/// 使用bincode库进行序列化
/// 
pub struct Encoder{
    buf: Vec<u8>,
}

impl Encoder{
    pub fn new() -> Self{
        Self{buf: Vec::new()}
    }
    

    // pub fn encode<T: Serialize>(&mut self, value: T) -> Result<Vec<u8>>{
    //     let bytes = bincode::serialize(&value)?;
    //     self.buf.extend_from_slice(&bytes);
    //     Ok(self.buf.clone())
    // }
}

#[cfg(test)]
mod tests {
    use bincode::{Encode,Decode};
    #[test]
    fn test_bincode(){
        #[derive(Encode,Decode,Debug)]
        pub struct Person{
            name: String,
            age: u8,
        }
        let person = Person{name: "John".to_string(), age: 30};
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
}
