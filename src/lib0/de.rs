use crate::lib0::{
    ExpectedString, TAG_ARRAY, TAG_BIGINT, TAG_BYTE_ARRAY, TAG_FALSE, TAG_FLOAT32, TAG_FLOAT64,
    TAG_INTEGER, TAG_NULL, TAG_OBJECT, TAG_STRING, TAG_TRUE, TAG_UNDEFINED,
};
use crate::read::ReadExt;
use serde::de::{DeserializeSeed, Error, Unexpected, Visitor};
use std::io::Read;

pub(super) struct Deserializer<R> {
    reader: R,
}

impl<R: Read> Deserializer<R> {
    pub fn new(reader: R) -> Self {
        Deserializer { reader }
    }

    fn deserialize_any_tagged<'de, V>(
        &'de mut self,
        tag: u8,
        visitor: V,
    ) -> Result<V::Value, super::Error>
    where
        V: Visitor<'de>,
    {
        match tag {
            TAG_UNDEFINED => visitor.visit_unit(),
            TAG_NULL => visitor.visit_none(),
            TAG_INTEGER => {
                let num: i64 = self.reader.read_var()?;
                visitor.visit_i64(num)
            }
            TAG_FLOAT32 => {
                let num: f32 = self.reader.read_f32()?;
                visitor.visit_f32(num)
            }
            TAG_FLOAT64 => {
                let num: f64 = self.reader.read_f64()?;
                visitor.visit_f64(num)
            }
            TAG_BIGINT => {
                let num: i64 = self.reader.read_i64()?;
                visitor.visit_i64(num)
            }
            TAG_TRUE => visitor.visit_bool(true),
            TAG_FALSE => visitor.visit_bool(false),
            TAG_STRING => {
                let mut buf = String::new(); // TODO: String::new_in(self.alloc)
                self.reader.read_string(&mut buf)?;
                visitor.visit_string(buf)
            }
            TAG_OBJECT => visitor.visit_map(MapAccess::new(self)?),
            TAG_ARRAY => visitor.visit_seq(SeqAccess::new(self)?),
            TAG_BYTE_ARRAY => {
                let mut buf = Vec::new(); // TODO: String::new_in(self.alloc)
                self.reader.read_bytes(&mut buf)?;
                visitor.visit_byte_buf(buf)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }
}

impl<'de, R: Read> serde::Deserializer<'de> for &'de mut Deserializer<R> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        self.deserialize_any_tagged(tag, visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_TRUE => visitor.visit_bool(true),
            TAG_FALSE => visitor.visit_bool(false),
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_INTEGER => {
                let num: i64 = self.reader.read_var()?;
                visitor.visit_i64(num)
            }
            TAG_FLOAT32 => {
                let num: f32 = self.reader.read_f32()?;
                visitor.visit_i64(num as i64)
            }
            TAG_FLOAT64 => {
                let num: f64 = self.reader.read_f64()?;
                visitor.visit_i64(num as i64)
            }
            TAG_BIGINT => {
                let num: i64 = self.reader.read_i64()?;
                visitor.visit_i64(num)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_FLOAT32 => {
                let num = self.reader.read_f32()?;
                visitor.visit_f32(num)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_FLOAT64 => {
                let num = self.reader.read_f64()?;
                visitor.visit_f64(num)
            }
            TAG_FLOAT32 => {
                let num = self.reader.read_f32()?;
                visitor.visit_f64(num as f64)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_STRING => {
                let mut buf = String::new(); // TODO: String::new_in(self.alloc)
                self.reader.read_string(&mut buf)?;
                match buf.chars().next() {
                    None => Err(super::Error::invalid_value(
                        Unexpected::Str(""),
                        &ExpectedString("character"),
                    )),
                    Some(c) => visitor.visit_char(c),
                }
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_STRING => {
                let mut buf = String::new(); // TODO: String::new_in(self.alloc)
                self.reader.read_string(&mut buf)?;
                visitor.visit_string(buf)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_BYTE_ARRAY => {
                let mut buf = Vec::new(); // TODO: Vec::new_in(self.alloc)
                self.reader.read_bytes(&mut buf)?;
                visitor.visit_byte_buf(buf)
            }
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_UNDEFINED | TAG_NULL => visitor.visit_unit(),
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_ARRAY => visitor.visit_seq(SeqAccess::new(self)?),
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.reader.read_u8()?;
        match tag {
            TAG_OBJECT => visitor.visit_map(MapAccess::new(self)?),
            tag => Err(super::Error::UnknownTag(tag)),
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

struct MapAccess<'a, R: 'a> {
    de: &'a mut Deserializer<R>,
    remaining: usize,
}

impl<'a, R: Read + 'a> MapAccess<'a, R> {
    fn new(de: &'a mut Deserializer<R>) -> Result<Self, super::Error> {
        let remaining: usize = de.reader.read_var()?;
        Ok(MapAccess { de, remaining })
    }
}

impl<'de, R: Read + 'de> serde::de::MapAccess<'de> for MapAccess<'de, R> {
    type Error = super::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            Ok(None)
        } else {
            self.remaining -= 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

struct SeqAccess<'a, R: 'a> {
    de: &'a mut Deserializer<R>,
    remaining: usize,
}

impl<'a, R: Read + 'a> SeqAccess<'a, R> {
    fn new(de: &'a mut Deserializer<R>) -> Result<Self, super::Error> {
        let remaining: usize = de.reader.read_var()?;
        Ok(SeqAccess { de, remaining })
    }
}

impl<'de, R: Read + 'de> serde::de::SeqAccess<'de> for SeqAccess<'de, R> {
    type Error = super::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.remaining == 0 {
            Ok(None)
        } else {
            self.remaining -= 1;
            seed.deserialize(&mut *self.de).map(Some)
        }
    }
}
