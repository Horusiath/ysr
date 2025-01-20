use serde::de::{DeserializeOwned, Expected};
use serde::{Deserialize, Serialize};
use std::alloc::Allocator;
use std::fmt::Display;
use std::io::{Read, Write};

mod copy;
mod de;
mod ser;
#[cfg(test)]
mod test;

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

pub const F64_MAX_SAFE_INTEGER: i64 = (i64::pow(2, 53) - 1);
pub const F64_MIN_SAFE_INTEGER: i64 = -F64_MAX_SAFE_INTEGER;

pub use copy::copy;

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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IO(#[from] std::io::Error),
    #[error("cannot serialize/deserialize collection of unknown length")]
    UnknownLength,
    #[error("tried to serialize/deserialize map with non-string keys")]
    NonStringKey,
    #[error("cannot deserialize payload - unknown tag: {0}")]
    UnknownTag(u8),
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
