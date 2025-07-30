use crate::db_error::{Error, Result};
use crate::errdata;
use itertools::Either;
use serde::de::{DeserializeSeed, EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor};
use serde::ser::Impossible;
use serde::{ser::{SerializeSeq, SerializeTuple, SerializeTupleStruct, SerializeTupleVariant}, Deserializer, Serialize, Serializer};

/// key_encoder 关于键的自定义序列化工具
/// 目标:
/// 1、是把任意类型的键可以序列化成字节数组
/// 2、支持key的前缀范围扫描
/// 3、保证序列化后值的有序性
pub struct KeyEncoder {
    buf: Vec<u8>,
}

impl KeyEncoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// 为 有符号整数类型 实现 序列化 符号位反转
    fn convert_first_bit<T: ToBeBytes>(&mut self, v: T) -> Result<()> {
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

#[allow(unused)]
impl Serializer for &mut KeyEncoder {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<(), Error>;

    type SerializeTupleVariant = Self;
    type SerializeMap = Impossible<(), Error>;
    type SerializeStruct = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<()> {
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
    fn serialize_i8(self, v: i8) -> Result<()> {
        // 1、转换为字节
        let mut bytes = v.to_be_bytes();
        // 2、反转最高位
        bytes[0] ^= 1 << 7;
        // 3、写入缓冲输出
        self.buf.extend_from_slice(&bytes);
        Ok(())
    }

    /// 序列化 
    fn serialize_i16(self, v: i16) -> Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.convert_first_bit(v)
    }

    fn serialize_i128(self, v: i128) -> Result<()> {
        let _ = v;
        Err(Error::SerializationError("i128 is not supported".to_owned()))
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.buf.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, v: u128) -> std::result::Result<Self::Ok, Self::Error> {
        let _ = v;
        Err(Error::SerializationError("u128 is not supported".to_owned()))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
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

    fn serialize_f64(self, v: f64) -> Result<()> {
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
        unimplemented!()
    }

    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        self.serialize_bytes(v.as_bytes())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        let bytes = v.iter().flat_map(|&byte| match byte {
            0x00 => Either::Left([0x00, 0xff].into_iter()),
            byte => Either::Right([byte].into_iter()),
        })
            .chain([0x00, 0x00]);
        self.buf.extend(bytes);
        Ok(())
    }

    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.buf.push(variant_index.try_into()?);
        Ok(())
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        unimplemented!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, len: Option<usize>) -> std::result::Result<Self::SerializeMap, Self::Error> {
        unimplemented!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        unimplemented!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        unimplemented!()
    }
}

/// 为 键 序列化器 实现 队列的序列化特征
impl SerializeSeq for &mut KeyEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeTuple for &mut KeyEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut KeyEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}


#[allow(unused)]
impl SerializeTupleVariant for &mut KeyEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!()
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

/// 反序列化结构体
pub struct KeyDecoder<'de> {
    input: &'de [u8],
}

impl<'de> KeyDecoder<'de> {
    /// 初始化接收一个等待反序列化的字节数组引用
    pub fn new(input: &'de [u8]) -> Self {
        KeyDecoder { input }
    }

    /// 取出指定长度的字节返回
    pub fn take_bytes(&mut self, len: usize) -> Result<&'de [u8]> {
        if self.input.len() < len {
            panic!("invalid length");
        }
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        Ok(bytes)
    }

    /// 解析字符串类型的字节数组
    pub fn decode_next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut result = Vec::<u8>::new();
        let mut iter = self.input.iter().enumerate();
        let taken = loop {
            match iter.next() {
                Some((_, 0x00)) => match iter.next() {
                    Some((i, 0x00)) => break i + 1,        // terminator
                    Some((_, 0xff)) => result.push(0x00), // escaped 0x00
                    _ => return errdata!("invalid data"),
                },
                Some((_, b)) => result.push(*b),
                None => return errdata!("invalid data"),
            }
        };
        self.input = &self.input[taken..];
        Ok(result)
    }
}

#[allow(unused)]
impl<'de> Deserializer<'de> for &mut KeyDecoder<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        panic!("需要提供解码的实际类型");
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let decoded = self.take_bytes(1)?;
        visitor.visit_bool(
            match decoded[0] {
                0x00 => false,
                0x01 => true,
                _ => panic!("invalid bool value"),
            }
        )
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // 反序列化i8类型
        let mut bytes = self.take_bytes(8)?.to_vec(); // 转成Vec<u8>的原因是为了修改bytes[0]
        bytes[0] ^= 1 << 7;
        visitor.visit_i8(i8::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // 反序列化i16类型
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7;
        visitor.visit_i16(i16::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // 反序列化i32类型
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7;
        visitor.visit_i32(i32::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // 反序列化i64类型
        let mut bytes = self.take_bytes(8)?.to_vec();
        bytes[0] ^= 1 << 7;
        visitor.visit_i64(i64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        match bytes[0] >> 7 {
            1 => bytes[0] ^= 1 << 7, // 正数 反转符号位
            0 => bytes.iter_mut().for_each(|e| *e = !*e),
            _ => panic!("invalid float value"),
        }
        visitor.visit_f32(f32::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let mut bytes = self.take_bytes(8)?.to_vec();
        match bytes[0] >> 7 {
            1 => bytes[0] ^= 1 << 7, // 正数 反转符号位
            0 => bytes.iter_mut().for_each(|e| *e = !*e),
            _ => panic!("invalid float value"),
        }
        visitor.visit_f64(f64::from_be_bytes(bytes.as_slice().try_into()?))
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let decoded = self.decode_next_bytes()?;
        visitor.visit_str(&String::from_utf8(decoded)?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let decoded = self.decode_next_bytes()?;
        visitor.visit_string(String::from_utf8(decoded)?)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let decoded = self.decode_next_bytes()?;
        visitor.visit_bytes(&decoded[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let decoded = self.decode_next_bytes()?;
        visitor.visit_byte_buf(decoded)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }
}

impl<'de> SeqAccess<'de> for KeyDecoder<'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self).map(Some)
    }
}
/// Enum variants are deserialized by their index.
impl<'de> EnumAccess<'de> for &mut KeyDecoder<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> std::result::Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)?[0] as u32;
        let value: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((value?, self))
    }
}


#[allow(unused)]
impl<'de> VariantAccess<'de> for &mut KeyDecoder<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }
}
#[cfg(test)]
mod tests {
    use itertools::Either;


    #[test]
    #[ignore]
    fn test_ehither() {
        let str = "abcv\x00";
        let v = str.as_bytes();
        println!("{:?}", v);
        let bytes = v
            .iter()
            .flat_map(|&byte| match byte {
                0x00 => Either::Left([0x00, 0xff].into_iter()),
                byte => Either::Right([byte].into_iter()),
            })
            .chain([0x00, 0x00]);
        let mut items: Vec<u8> = vec![];
        items.extend(bytes);
        println!("{:?}", items);
    }
}
