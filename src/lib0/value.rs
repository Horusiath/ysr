use crate::block::InsertBlockData;
use crate::prelim::Prelim;
use crate::Transaction;
use bytes::{Bytes, BytesMut};
use serde::de::{Error, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Undefined,
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Object(HashMap<String, Value>),
    Array(Vec<Value>),
    ByteArray(Bytes),
}

impl Value {
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
            Value::Int(v) => serializer.serialize_i64(*v),
            Value::Float(v) => serializer.serialize_f64(*v),
            Value::Bool(v) => serializer.serialize_bool(*v),
            Value::String(v) => serializer.serialize_str(&*v),
            Value::Object(v) => v.serialize(serializer),
            Value::Array(v) => v.serialize(serializer),
            Value::ByteArray(v) => serializer.serialize_bytes(&*v),
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
                Ok(Value::Int(v))
            }

            #[inline]
            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Float(v as f64))
            }

            #[inline]
            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Float(v))
            }

            #[inline]
            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Value::Undefined)
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
            Value::Int(v) => Display::fmt(v, f),
            Value::Float(v) => Display::fmt(v, f),
            Value::Bool(v) => Display::fmt(v, f),
            Value::String(v) => write!(f, "\"{}\"", v),
            Value::Object(v) => {
                let mut i = v.iter();
                write!(f, "{{")?;
                if let Some((k, v)) = i.next() {
                    write!(f, "\"{}\": {}", k, v)?;
                }
                while let Some((k, v)) = i.next() {
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
                while let Some(v) = i.next() {
                    write!(f, ", {}", v)?;
                }
                write!(f, "]")
            }
            Value::ByteArray(v) => {
                let base64 = simple_base64::encode(v);
                write!(f, "{}", base64)
            }
        }
    }
}

impl Prelim for Value {
    type Return = ();

    fn prepare(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return> {
        todo!()
    }
}
