
use std::io::Read;

use itertools::Either;
use serde::{ser::{SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant}, Serialize, Serializer};
use crate::db_error::Result;

/// key_encoder 关于键的自定义序列化工具
/// 目标:
/// 1、是把任意类型的键可以序列化成字节数组
/// 2、支持key的前缀范围扫描
/// 3、保证序列化后值的有序性
pub struct KeyEncoder{
    buf: Vec<u8>,
}

impl KeyEncoder{
    pub fn new() -> Self{
        Self{buf: Vec::new()}
    }
    
    /// 为 有符号整数类型 实现 序列化 符号位反转
    fn convert_first_bit<T:ToBeBytes>(&mut self,v:T)->Result<()>{
        // 1、转换为字节
        let mut bytes = v.to_be_bytes();
        let bytes = bytes.as_mut();
        // 2、反转最高位
        bytes[0] ^= 1 << 7;
        // 3、写入缓冲输出
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }
}

trait ToBeBytes {
    type Bytes: AsRef<[u8]> + AsMut<[u8]>;  // 关键：支持索引访问
    fn to_be_bytes(self) -> Self::Bytes;
}

impl ToBeBytes for i8 {
    type Bytes = [u8; 1];
    fn to_be_bytes(self) -> Self::Bytes {
        i8::to_be_bytes(self)
    }
}

impl ToBeBytes for i16 {
    type Bytes = [u8; 2];
    fn to_be_bytes(self) -> Self::Bytes {
        i16::to_be_bytes(self)
    }
}

impl ToBeBytes for i32 {
    type Bytes = [u8; 4];
    fn to_be_bytes(self) -> Self::Bytes {
        i32::to_be_bytes(self)
    }
}

impl ToBeBytes for i64 {
    type Bytes = [u8; 8];
    fn to_be_bytes(self) -> Self::Bytes {
        i64::to_be_bytes(self)
    }
}



impl Serializer for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant =Self;

    type SerializeMap = Self;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> crate::db_error::Result<()> {
        // 序列化的时候 真 序列化为 1；假序列化成0
        self.buf.push(
            match v {
                true => 1,
                _ => 0
            }
        );
        Ok(())
    }
 
    /// 序列化原理：符号位反转，可以保证序列化的结果保留原本的顺序
    fn serialize_i8(self, v: i8) ->  crate::db_error::Result<()> {
        // 1、转换为字节
        let mut bytes = v.to_be_bytes();
        // 2、反转最高位
        bytes[0] ^= 1 << 7;
        // 3、写入缓冲输出
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    /// 序列化 
    fn serialize_i16(self, v: i16) -> crate::db_error::Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_i32(self, v: i32) -> crate::db_error::Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_i64(self, v: i64) -> crate::db_error::Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_u8(self, v: u8) -> crate::db_error::Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> crate::db_error::Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> crate::db_error::Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> crate::db_error::Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, v: f32) -> crate::db_error::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            // 对于正数：只需翻转符号位，让正数排在负数后面
            false => bytes[0] ^= 1 << 7,
            // 对于负数：翻转所有位，这样可以让负数按照从小到大排序
            true => bytes.iter_mut().for_each(|b| *b = !*b),
        }
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn serialize_f64(self, v: f64) -> crate::db_error::Result<()> {
        let mut bytes = v.to_be_bytes();
        match v.is_sign_negative() {
            // 对于正数：只需翻转符号位，让正数排在负数后面
            false => bytes[0] ^= 1 << 7,
            // 对于负数：翻转所有位，这样可以让负数按照从小到大排序
            true => bytes.iter_mut().for_each(|b| *b = !*b),
        }
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> crate::db_error::Result<()> {
        let bytes = v.iter().flat_map(|&byte| match byte {
            0x00 => Either::Left([0x00, 0xff].into_iter()),
            byte => Either::Right([byte].into_iter()),
        })
        .chain([0x00, 0x00]);
        self.buf.extend(bytes);
        Ok(())
    }

    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize {
        todo!()
    }

    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> crate::db_error::Result<()> {
        self.buf.push(variant_index.try_into()?);
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
            self.serialize_unit_variant(name, variant_index, variant)?;
            value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) ->crate::db_error::Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> crate::db_error::Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> crate::db_error::Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> crate::db_error::Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> std::result::Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
    
    fn serialize_i128(self, v: i128) -> std::result::Result<Self::Ok, Self::Error> {
        let _ = v;
        Err(serde::ser::Error::custom("i128 is not supported"))
    }
    
    fn serialize_u128(self, v: u128) -> std::result::Result<Self::Ok, Self::Error> {
        let _ = v;
        Err(serde::ser::Error::custom("u128 is not supported"))
    }
}

/// 为 键 序列化器 实现 队列的序列化特征
impl SerializeSeq for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_element<T>(&mut self, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
            value.serialize(&mut **self)?;
            Ok(())
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeTuple for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_element<T>(&mut self, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
            value.serialize(&mut **self)?;
            Ok(())
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut KeyEncoder{
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_field<T>(&mut self, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
            value.serialize(&mut **self)?;
            Ok(())
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeStructVariant for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
         unimplemented!()
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeTupleVariant for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_field<T>(&mut self, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
        unimplemented!()
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeMap for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_key<T>(&mut self, key: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
        unimplemented!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
        unimplemented!()
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeStruct for &mut KeyEncoder {
    type Ok = ();

    type Error = crate::db_error::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> crate::db_error::Result<()>
    where
        T: ?Sized + Serialize {
        unimplemented!()
    }

    fn end(self) -> crate::db_error::Result<Self::Ok> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Either;


    #[test]
    #[ignore]
    fn test_ehither(){
        let str  = "abcv\x00";
        let v = str.as_bytes();
        println!("{:?}",v);
        let bytes = v
        .iter()
        .flat_map(|&byte| match byte {
            0x00 => Either::Left([0x00, 0xff].into_iter()),
            byte => Either::Right([byte].into_iter()),
        })
        .chain([0x00, 0x00]);
        let mut items: Vec<u8> = vec![];
        items.extend(bytes);
        println!("{:?}",items);
    }
}
