use crate::lib0::{
    ExpectedString, Tag, Value, TAG_ARRAY, TAG_BIGINT, TAG_BYTE_ARRAY, TAG_FALSE, TAG_FLOAT32,
    TAG_FLOAT64, TAG_INTEGER, TAG_NULL, TAG_OBJECT, TAG_STRING, TAG_TRUE, TAG_UNDEFINED,
};
use crate::read::ReadExt;
use serde::de::{DeserializeSeed, Error, MapAccess, SeqAccess, Unexpected, Visitor};
use serde::{de, Deserialize};
use smallvec::SmallVec;
use std::io::{Cursor, Read};

const DEFAULT_INLINE_STRING_SIZE: usize = 16;

pub(super) struct Deserializer<R> {
    reader: R,
    peeked_tag: Option<u8>,
}

impl<R: Read> Deserializer<R> {
    pub fn new(reader: R) -> Self {
        Deserializer {
            reader,
            peeked_tag: None,
        }
    }

    fn read_tag(&mut self) -> Result<u8, super::Error> {
        match self.peeked_tag.take() {
            Some(tag) => Ok(tag),
            None => Ok(self.reader.read_u8()?),
        }
    }

    #[inline]
    fn expect_tag(&mut self, tag: u8) -> Result<(), super::Error> {
        let actual = self.read_tag()?;
        if actual == tag {
            Ok(())
        } else {
            Err(super::Error::UnknownTag(actual))
        }
    }

    fn peek_tag(&mut self) -> Result<u8, super::Error> {
        match self.peeked_tag {
            Some(tag) => Ok(tag),
            None => {
                let tag = self.reader.read_u8()?;
                self.peeked_tag = Some(tag);
                Ok(tag)
            }
        }
    }

    fn deserialize_any_tagged<'de, V>(
        &'de mut self,
        tag: Tag,
        visitor: V,
    ) -> Result<V::Value, super::Error>
    where
        V: Visitor<'de>,
    {
        match tag {
            Tag::Undefined => visitor.visit_unit(),
            Tag::Null => visitor.visit_none(),
            Tag::VarInt => {
                let num: i64 = self.reader.read_var()?;
                visitor.visit_i64(num)
            }
            Tag::Float32 => {
                let num: f32 = self.reader.read_f32()?;
                visitor.visit_f32(num)
            }
            Tag::Float64 => {
                let num: f64 = self.reader.read_f64()?;
                visitor.visit_f64(num)
            }
            Tag::BigInt => {
                let num: i64 = self.reader.read_i64()?;
                visitor.visit_i64(num)
            }
            Tag::True => visitor.visit_bool(true),
            Tag::False => visitor.visit_bool(false),
            Tag::String => {
                let mut buf: SmallVec<[u8; DEFAULT_INLINE_STRING_SIZE]> = SmallVec::new();
                self.reader.read_string(&mut buf)?;
                let str = std::str::from_utf8(&buf)?;
                visitor.visit_str(str)
            }
            Tag::Object => visitor.visit_map(Access::new(self)?),
            Tag::Array => visitor.visit_seq(Access::new(self)?),
            Tag::ByteArray => {
                let mut buf = Vec::new(); // TODO: String::new_in(self.alloc)
                self.reader.read_bytes(&mut buf)?;
                visitor.visit_byte_buf(buf)
            }
        }
    }
}

impl<'de, R: Read> serde::Deserializer<'de> for &'de mut Deserializer<R> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag: Tag = self.read_tag()?.try_into()?;
        self.deserialize_any_tagged(tag, visitor)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.read_tag()?;
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
        let tag: Tag = self.read_tag()?.try_into()?;
        match tag {
            Tag::VarInt => {
                let num: i64 = self.reader.read_var()?;
                visitor.visit_i64(num)
            }
            Tag::Float32 => {
                let num: f32 = self.reader.read_f32()?;
                visitor.visit_i64(num as i64)
            }
            Tag::Float64 => {
                let num: f64 = self.reader.read_f64()?;
                visitor.visit_i64(num as i64)
            }
            Tag::BigInt => {
                let num: i64 = self.reader.read_i64()?;
                visitor.visit_i64(num)
            }
            tag => Err(super::Error::UnknownTag(tag as u8)),
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
        self.expect_tag(TAG_FLOAT32)?;
        let num = self.reader.read_f32()?;
        visitor.visit_f32(num)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.read_tag()?;
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
        self.expect_tag(TAG_STRING)?;
        let mut buf: SmallVec<[u8; 4]> = SmallVec::new();
        self.reader.read_string(&mut buf)?;
        let str = std::str::from_utf8(&buf)?;
        match str.chars().next() {
            None => Err(super::Error::invalid_value(
                Unexpected::Str(""),
                &ExpectedString("character"),
            )),
            Some(c) => visitor.visit_char(c),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.expect_tag(TAG_STRING)?;
        let mut buf: SmallVec<[u8; DEFAULT_INLINE_STRING_SIZE]> = SmallVec::new();
        self.reader.read_string(&mut buf)?;
        let str = std::str::from_utf8(&buf)?;
        visitor.visit_str(str)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.expect_tag(TAG_STRING)?;
        let mut buf = String::new();
        let writer = unsafe { buf.as_mut_vec() };
        self.reader.read_string(writer)?;
        visitor.visit_string(buf)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.expect_tag(TAG_BYTE_ARRAY)?;
        let mut buf = Vec::new(); // TODO: Vec::new_in(self.alloc)
        self.reader.read_bytes(&mut buf)?;
        visitor.visit_byte_buf(buf)
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
        let tag = self.peek_tag()?;
        match tag {
            TAG_UNDEFINED | TAG_NULL => {
                self.peeked_tag = None; // reset peek
                visitor.visit_none()
            }
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let tag = self.read_tag()?;
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
        self.expect_tag(TAG_ARRAY)?;
        visitor.visit_seq(Access::new(self)?)
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
        self.expect_tag(TAG_OBJECT)?;
        visitor.visit_map(Access::new(self)?)
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
        self.expect_tag(TAG_OBJECT)?;
        visitor.visit_enum(Access::new(self)?)
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

struct Access<'a, R> {
    de: &'a mut Deserializer<R>,
    len: usize,
}

impl<'a, R: Read> Access<'a, R> {
    fn new(de: &'a mut Deserializer<R>) -> Result<Self, super::Error> {
        let len = de.reader.read_var()?;
        Ok(Access { de, len })
    }
}

impl<'a, 'de, R: Read> de::SeqAccess<'de> for Access<'a, R>
where
    'a: 'de,
{
    type Error = super::Error;

    #[inline]
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.len > 0 {
            self.len -= 1;
            //FIXME: fix borrow checker error
            let de: &'a mut Deserializer<R> = unsafe { std::mem::transmute(&mut *self.de) };
            seed.deserialize(de).map(Some)
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

impl<'a, 'de, R: Read> de::MapAccess<'de> for Access<'a, R>
where
    'a: 'de,
{
    type Error = super::Error;

    #[inline]
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.len > 0 {
            self.len -= 1;
            //FIXME: fix borrow checker error
            let de: &'a mut Deserializer<R> = unsafe { std::mem::transmute(&mut *self.de) };
            seed.deserialize(MapKey { de }).map(Some)
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        //FIXME: fix borrow checker error
        let de: &'a mut Deserializer<R> = unsafe { std::mem::transmute(&mut *self.de) };
        seed.deserialize(de)
    }

    #[inline]
    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

impl<'a, 'de, R: Read> de::EnumAccess<'de> for Access<'a, R>
where
    'a: 'de,
{
    type Error = super::Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(MapKey { de: self.de }).map(|v| (v, self))
    }
}

impl<'a, 'de, R: Read> de::VariantAccess<'de> for Access<'a, R>
where
    'a: 'de,
{
    type Error = super::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        self.de.expect_tag(TAG_ARRAY)?;
        let mut access = Access::new(self.de)?;
        while let Some(_) = access.next_element::<Value>()? {
            // skip over all possible values for forward compatibility
        }
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        self.de.expect_tag(TAG_ARRAY)?;
        let mut access = Access::new(self.de)?;
        let value = match access.next_element_seed(seed)? {
            None => {
                return Err(super::Error::invalid_length(
                    0,
                    &"newtype variant with >1 element",
                ))
            }
            Some(value) => value,
        };
        while let Some(_) = access.next_element::<Value>()? {
            // skip over all possible values for forward compatibility
        }
        Ok(value)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_struct(self.de, "", fields, visitor)
    }
}

struct MapKey<'a, R> {
    de: &'a mut Deserializer<R>,
}

impl<'a, 'de, R: Read> de::Deserializer<'de> for MapKey<'a, R> {
    type Error = super::Error;

    #[inline]
    fn deserialize_any<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_bool<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_i8<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_i16<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_i32<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_i64<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_u8<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_u16<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_u32<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_u64<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_f32<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_f64<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_char<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf: SmallVec<[u8; DEFAULT_INLINE_STRING_SIZE]> = SmallVec::new();
        self.de.reader.read_string(&mut buf)?;
        let str = std::str::from_utf8(&buf)?;
        visitor.visit_str(str)
    }

    #[inline]
    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut s = String::new();
        let buf = unsafe { s.as_mut_vec() };
        self.de.reader.read_string(buf)?;
        visitor.visit_string(s)
    }

    #[inline]
    fn deserialize_bytes<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_byte_buf<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_option<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_unit<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_unit_struct<V>(self, _: &'static str, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_newtype_struct<V>(self, _: &'static str, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_seq<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_tuple<V>(self, _: usize, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        _: &'static str,
        _: usize,
        _: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_map<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_struct<V>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }

    #[inline]
    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    #[inline]
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    #[inline]
    fn deserialize_ignored_any<V>(self, _: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(super::Error::NonStringKey)
    }
}
