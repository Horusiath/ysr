use bytes::Bytes;
use serde::de::value::StringDeserializer;
use serde::de::{DeserializeSeed, Error, IntoDeserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ValueKind {
    Undefined,
    Null,
    Number,
    Bool,
    String,
    Object,
    Array,
    Bytes,
}

impl Display for ValueKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueKind::Undefined => write!(f, "undefined"),
            ValueKind::Null => write!(f, "null"),
            ValueKind::Number => write!(f, "number"),
            ValueKind::Bool => write!(f, "bool"),
            ValueKind::String => write!(f, "string"),
            ValueKind::Object => write!(f, "object"),
            ValueKind::Array => write!(f, "array"),
            ValueKind::Bytes => write!(f, "binary"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Undefined,
    Null,
    Number(Number),
    Bool(bool),
    String(String),
    Object(HashMap<String, Value>),
    Array(Vec<Value>),
    Bytes(Bytes),
}

impl Value {
    pub fn kind(&self) -> ValueKind {
        match self {
            Value::Undefined => ValueKind::Undefined,
            Value::Null => ValueKind::Null,
            Value::Number(_) => ValueKind::Number,
            Value::Bool(_) => ValueKind::Bool,
            Value::String(_) => ValueKind::String,
            Value::Object(_) => ValueKind::Object,
            Value::Array(_) => ValueKind::Array,
            Value::Bytes(_) => ValueKind::Bytes,
        }
    }

    pub fn is_undefined(&self) -> bool {
        matches!(self, Value::Undefined)
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_string_mut(&mut self) -> Option<&mut String> {
        match self {
            Value::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_slice(&self) -> Option<&[Self]> {
        match self {
            Value::Array(value) => Some(value.as_ref()),
            _ => None,
        }
    }

    pub fn as_vec_mut(&mut self) -> Option<&mut Vec<Self>> {
        match self {
            Value::Array(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut HashMap<String, Value>> {
        match self {
            Value::Object(value) => Some(value),
            _ => None,
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Undefined => serializer.serialize_unit(),
            Value::Null => serializer.serialize_none(),
            Value::Number(v) => v.serialize(serializer),
            Value::Bool(v) => serializer.serialize_bool(*v),
            Value::String(v) => serializer.serialize_str(v),
            Value::Object(v) => v.serialize(serializer),
            Value::Array(v) => v.serialize(serializer),
            Value::Bytes(v) => serializer.serialize_bytes(v),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a lib0 value")
            }

            #[inline]
            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Bool(v))
            }

            #[inline]
            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(v.into())
            }

            #[inline]
            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(v.into())
            }

            #[inline]
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(v.into())
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Null)
            }

            #[inline]
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::from(v))
            }

            #[inline]
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::from(v))
            }

            #[inline]
            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::String(v))
            }

            #[inline]
            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::String(v.into()))
            }

            #[inline]
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = match map.size_hint() {
                    None => HashMap::new(),
                    Some(len) => HashMap::with_capacity(len),
                };
                while let Some((key, value)) = map.next_entry()? {
                    values.insert(key, value);
                }
                Ok(Value::Object(values))
            }

            #[inline]
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = match seq.size_hint() {
                    None => Vec::new(),
                    Some(len) => Vec::with_capacity(len),
                };
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Value::Array(values))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Undefined => write!(f, "undefined"),
            Value::Null => write!(f, "null"),
            Value::Number(n) => write!(f, "{}", n),
            Value::Bool(v) => Display::fmt(v, f),
            Value::String(v) => write!(f, "\"{}\"", v),
            Value::Object(v) => {
                let mut i = v.iter();
                write!(f, "{{")?;
                if let Some((k, v)) = i.next() {
                    write!(f, "\"{}\": {}", k, v)?;
                }
                for (k, v) in i {
                    write!(f, ", \"{}\": {}", k, v)?;
                }
                write!(f, "}}")
            }
            Value::Array(v) => {
                let mut i = v.iter();
                write!(f, "[")?;
                if let Some(v) = i.next() {
                    write!(f, "{}", v)?;
                }
                for v in i {
                    write!(f, ", {}", v)?;
                }
                write!(f, "]")
            }
            Value::Bytes(v) => {
                let base64 = simple_base64::encode(v);
                write!(f, "{}", base64)
            }
        }
    }
}

impl<'de> Deserializer<'de> for Value {
    type Error = super::Error;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Undefined => visitor.visit_unit(),
            Value::Null => visitor.visit_unit(),
            Value::Bool(value) => visitor.visit_bool(value),
            Value::String(value) => visitor.visit_string(value),
            Value::Object(mut value) => visitor.visit_map(MapDeserializer::new(value.drain())),
            Value::Array(value) => visit_array(value, visitor),
            Value::Bytes(value) => visitor.visit_byte_buf(value.into()),
            Value::Number(value) => match value.as_i64() {
                None => match value.as_f64() {
                    None => Err(Self::Error::InvalidType(ValueKind::Number)),
                    Some(v) => visitor.visit_f64(v),
                },
                Some(v) => visitor.visit_i64(v),
            },
        }
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Bool(value) => visitor.visit_bool(value),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    #[inline]
    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_i64(visitor)
    }

    #[inline]
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
        match self {
            Value::Number(value) => match value.as_i64() {
                Some(value) => visitor.visit_i64(value),
                None => Err(super::Error::InvalidType(ValueKind::Number)),
            },
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_u64(visitor)
    }

    #[inline]
    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_u64(visitor)
    }

    #[inline]
    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_u64(visitor)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Number(value) => match value.as_i64() {
                Some(value) if value >= 0 => visitor.visit_u64(value as u64),
                _ => Err(super::Error::InvalidType(ValueKind::Number)),
            },
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_f64(visitor)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Number(value) => match value.as_f64() {
                Some(value) => visitor.visit_f64(value),
                None => Err(super::Error::InvalidType(ValueKind::Number)),
            },
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    #[inline]
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::String(value) => visitor.visit_string(value),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_byte_buf(visitor)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Bytes(value) => visitor.visit_byte_buf(value.into()),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Undefined | Value::Null => visitor.visit_none(),
            other => visitor.visit_some(other),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Undefined | Value::Null => visitor.visit_none(),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
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

    #[inline]
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
        match self {
            Value::Array(array) => visit_array(array, visitor),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    #[inline]
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Object(map) => visitor.visit_map(MapDeserializer::new(map.into_iter())),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
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
        match self {
            Value::String(str) => visitor.visit_enum(str.into_deserializer()),
            other => Err(super::Error::InvalidType(other.kind())),
        }
    }

    #[inline]
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    #[inline]
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }
}

fn visit_array<'de, V>(values: Vec<Value>, visitor: V) -> Result<V::Value, super::Error>
where
    V: Visitor<'de>,
{
    let mut deserializer = SeqDeserializer::new(values.into_iter());
    visitor.visit_seq(&mut deserializer)
}

#[repr(transparent)]
struct SeqDeserializer<I> {
    iter: I,
}

impl<I> SeqDeserializer<I> {
    fn new(iter: I) -> Self {
        SeqDeserializer { iter }
    }
}

impl<'de, I: Iterator<Item = Value> + ExactSizeIterator> SeqAccess<'de> for SeqDeserializer<I> {
    type Error = super::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            None => Ok(None),
            Some(value) => seed.deserialize(value).map(Some),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

struct MapDeserializer<I> {
    iter: I,
    current: Option<Value>,
}

impl<I> MapDeserializer<I> {
    fn new(iter: I) -> Self {
        MapDeserializer {
            iter,
            current: None,
        }
    }
}

impl<'de, I: Iterator<Item = (String, Value)> + ExactSizeIterator> MapAccess<'de>
    for MapDeserializer<I>
{
    type Error = super::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            None => Ok(None),
            Some((key, value)) => {
                self.current = Some(value);
                seed.deserialize(StringDeserializer::new(key)).map(Some)
            }
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        match self.current.take() {
            None => Err(super::Error::Custom("value is missing".into())),
            Some(value) => seed.deserialize(value),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    Int(i64),
    Float(f64),
}

impl Number {
    pub const I64_MAX_SAFE_INTEGER: i64 = i64::pow(2, 53) - 1;
    pub const I64_MIN_SAFE_INTEGER: i64 = -Self::I64_MAX_SAFE_INTEGER;
    pub const F64_MAX_SAFE_INTEGER: f64 = Self::I64_MAX_SAFE_INTEGER as f64;
    pub const F64_MIN_SAFE_INTEGER: f64 = -Self::F64_MAX_SAFE_INTEGER;

    pub fn as_i64(self) -> Option<i64> {
        match self {
            Number::Int(value) => Some(value),
            Number::Float(value) => {
                // check if conversion is lossless
                let converted = value as i64;
                if converted as f64 == value {
                    Some(converted)
                } else {
                    None
                }
            }
        }
    }

    pub fn as_f64(self) -> Option<f64> {
        match self {
            Number::Int(value) => {
                if value >= Self::I64_MIN_SAFE_INTEGER && value <= Self::I64_MAX_SAFE_INTEGER {
                    Some(value as f64)
                } else {
                    None
                }
            }
            Number::Float(value) => Some(value),
        }
    }
}

impl Serialize for Number {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::Error;
        match self.as_f64() {
            Some(v) => serializer.serialize_f64(v),
            None => match self.as_i64() {
                Some(v) => serializer.serialize_i64(v),
                None => Err(S::Error::custom("cannot serialize number")),
            },
        }
    }
}

impl<'de> Deserialize<'de> for Number {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct NumberVisitor;
        impl<'de> Visitor<'de> for NumberVisitor {
            type Value = Number;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                write!(formatter, "number")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Number, E> {
                Ok(Number::Int(value))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if v <= Number::F64_MAX_SAFE_INTEGER
                    && v > Number::F64_MIN_SAFE_INTEGER
                    && v.trunc() == v
                {
                    Ok(Number::Int(v as i64))
                } else {
                    Ok(Number::Float(v))
                }
            }
        }

        deserializer.deserialize_any(NumberVisitor)
    }
}

impl Display for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Number::Int(value) => write!(f, "{}", value),
            Number::Float(value) => write!(f, "{}", value),
        }
    }
}
