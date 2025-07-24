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
        #[derive(Encode,Decode)]
        pub struct Person{
            name: String,
            age: u8,
        }
        let person = Person{name: "John".to_string(),age: 30};

    }
}
