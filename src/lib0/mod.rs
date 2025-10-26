use serde::de::{DeserializeOwned, Expected};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{Read, Write};
use std::str::Utf8Error;

mod copy;
mod de;
mod macros;
mod ser;
#[cfg(test)]
mod test;
mod value;

pub const TAG_UNDEFINED: u8 = 127;
pub const TAG_NULL: u8 = 126;
pub const TAG_INTEGER: u8 = 125;
pub const TAG_FLOAT32: u8 = 124;
pub const TAG_FLOAT64: u8 = 123;
pub const TAG_BIGINT: u8 = 122;
pub const TAG_FALSE: u8 = 121;
pub const TAG_TRUE: u8 = 120;
pub const TAG_STRING: u8 = 119;
pub const TAG_OBJECT: u8 = 118;
pub const TAG_ARRAY: u8 = 117;
pub const TAG_BYTE_ARRAY: u8 = 116;

#[repr(u8)]
#[derive(Debug, Copy, Clone)]
pub enum Tag {
    Undefined = TAG_UNDEFINED,
    Null = TAG_NULL,
    VarInt = TAG_INTEGER,
    Float32 = TAG_FLOAT32,
    Float64 = TAG_FLOAT64,
    BigInt = TAG_BIGINT,
    False = TAG_FALSE,
    True = TAG_TRUE,
    String = TAG_STRING,
    Object = TAG_OBJECT,
    Array = TAG_ARRAY,
    ByteArray = TAG_BYTE_ARRAY,
}

impl TryFrom<u8> for Tag {
    type Error = Error;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            TAG_UNDEFINED => Ok(Self::Undefined),
            TAG_NULL => Ok(Self::Null),
            TAG_INTEGER => Ok(Self::VarInt),
            TAG_FLOAT32 => Ok(Self::Float32),
            TAG_FLOAT64 => Ok(Self::Float64),
            TAG_BIGINT => Ok(Self::BigInt),
            TAG_FALSE => Ok(Self::False),
            TAG_TRUE => Ok(Self::True),
            TAG_STRING => Ok(Self::String),
            TAG_OBJECT => Ok(Self::Object),
            TAG_ARRAY => Ok(Self::Array),
            TAG_BYTE_ARRAY => Ok(Self::ByteArray),
            _ => Err(Error::UnknownTag(value)),
        }
    }
}

pub const F64_MAX_SAFE_INTEGER: i64 = (i64::pow(2, 53) - 1);
pub const F64_MIN_SAFE_INTEGER: i64 = -F64_MAX_SAFE_INTEGER;

pub use copy::copy;
pub use value::Value;

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>, Error>
where
    T: ?Sized + Serialize,
{
    let mut buf = Vec::new();
    let mut serializer = ser::Serializer::new(&mut buf);
    value.serialize(&mut serializer)?;
    Ok(buf)
}

pub fn from_slice<T>(buf: &[u8]) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    let mut deserializer = de::Deserializer::new(buf);
    T::deserialize(&mut deserializer)
}

pub fn to_writer<W, T>(writer: W, value: &T) -> Result<(), Error>
where
    W: Write,
    T: ?Sized + Serialize,
{
    let mut serializer = ser::Serializer::new(writer);
    value.serialize(&mut serializer)
}

pub fn from_reader<R, T>(reader: R) -> Result<T, Error>
where
    R: Read,
    T: DeserializeOwned,
{
    let mut deserializer = de::Deserializer::new(reader);
    T::deserialize(&mut deserializer)
}

pub(crate) fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    todo!()
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("cannot serialize/deserialize collection of unknown length")]
    UnknownLength,
    #[error("tried to serialize/deserialize map with non-string keys")]
    NonStringKey,
    #[error("cannot deserialize payload - unknown type tag: {0}")]
    UnknownTag(u8),
    #[error("invalid UTF8 string: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("lib0 error: {0}")]
    Custom(String),
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Custom(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Custom(msg.to_string())
    }
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        Self::Lib0(Box::new(err))
    }
}

struct ExpectedString(&'static str);
impl Expected for ExpectedString {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
