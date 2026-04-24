use serde::Serialize;
use serde::de::{DeserializeOwned, Expected};
use std::fmt::{Display, Formatter};
use std::io::{Read, Write};
use std::ops::Range;
use std::str::Utf8Error;

mod copy;
pub mod de;
mod macros;
pub mod ser;
#[cfg(test)]
mod test;
pub mod v1;
pub mod v2;
mod value;
mod varint;

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

use crate::lib0::v1::{DecoderV1, EncoderV1};
use crate::lib0::v2::{DecoderV2, EncoderV2};
use crate::lib0::varint::{Signed, SignedVarInt, VarInt};
use crate::{ClientID, Clock, ID};
pub use copy::copy;
pub use value::{Number, Value, ValueKind};

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

pub(crate) fn from_value<T>(value: Value) -> Result<T, Error>
where
    T: DeserializeOwned,
{
    T::deserialize(value)
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
    #[error("invalid type: {0}")]
    InvalidType(ValueKind),
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

/// Which version of lib0 encoding should be used.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum Encoding {
    /// (Default) V1 encoding, works faster for small payloads (like incremental updates) and has
    /// less intermediate allocations. For bigger payloads it can produce a bigger size footprint.
    #[default]
    V1 = 1,

    /// V2 encoding, it's better at compressing updates with a high number of changes (like initial
    /// document state). It can be slower, and update size can be a bit bigger for smaller updates,
    /// but for big updates it can often lead to massive size savings.
    V2 = 2,
}

pub trait Encoder: Write {
    /// Reset the state of currently encoded [DeleteSet].
    fn reset_ds_cur_val(&mut self);

    /// Write a clock value of currently encoded [DeleteSet] client.
    fn write_ds_clock(&mut self, clock: Clock) -> crate::Result<usize>;

    /// Write a number of client entries used by currently encoded [DeleteSet].
    fn write_ds_len(&mut self, len: Clock) -> crate::Result<usize>;

    /// Write unique identifier of a currently encoded [Block]'s left origin.
    fn write_left_id(&mut self, id: &ID) -> crate::Result<()>;

    /// Write unique identifier of a currently encoded [Block]'s right origin.
    fn write_right_id(&mut self, id: &ID) -> crate::Result<()>;

    /// Write currently encoded client identifier.
    fn write_client(&mut self, client: ClientID) -> crate::Result<usize>;

    /// Write currently encoded [Block]'s info flags. These contain information about which fields
    /// have been provided and which should be skipped during decoding process as well as a type of
    /// block currently encoded.
    fn write_info(&mut self, info: u8) -> crate::Result<()>;

    /// Write info flag about currently encoded [Block]'s parent. Is is another block or root type.
    fn write_parent_info(&mut self, is_y_key: bool) -> crate::Result<()>;

    /// Writes type ref data of currently encoded [Block]'s parent.
    fn write_type_ref(&mut self, info: u8) -> crate::Result<()>;

    /// Write length parameter.
    fn write_len(&mut self, len: Clock) -> crate::Result<usize>;

    /// Write a string key.
    fn write_key(&mut self, string: &str) -> crate::Result<usize>;

    /// Encode JSON-like data type. This is a complex structure which is an extension to JavaScript
    /// Object Notation with some extra cases.
    #[allow(unused)]
    fn write_any<S: Serialize>(&mut self, any: &S) -> crate::Result<()>;

    /// Encode JSON-like data type as nested JSON string. This is a complex structure which is an
    /// extension to JavaScript Object Notation with some extra cases.
    fn write_json<S: Serialize>(&mut self, any: &S) -> crate::Result<()>;
}

pub trait Encode {
    fn encode(&self, version: Encoding) -> crate::Result<Vec<u8>> {
        match version {
            Encoding::V1 => {
                let mut encoder = EncoderV1::new(Vec::new());
                self.encode_with(&mut encoder)?;
                Ok(encoder.into_inner())
            }
            Encoding::V2 => {
                let mut encoder = EncoderV2::new(Vec::new());
                self.encode_with(&mut encoder)?;
                Ok(encoder.into_inner()?)
            }
        }
    }

    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()>;
}

impl Encode for Range<Clock> {
    fn encode_with<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        encoder.write_ds_clock(self.start)?;
        encoder.write_ds_len(self.end - self.start)?;
        Ok(())
    }
}

pub trait WriteExt: Write + Sized {
    /// Write an unsigned integer (16bit)
    fn write_u8(&mut self, num: u8) -> std::io::Result<()> {
        self.write_all(&[num])
    }

    /// Write an unsigned integer (16bit)
    #[allow(unused)]
    fn write_u16(&mut self, num: u16) -> std::io::Result<()> {
        self.write_all(&[num as u8, (num >> 8) as u8])
    }

    /// Write an unsigned integer (32bit)
    #[allow(unused)]
    fn write_u32(&mut self, num: u32) -> std::io::Result<()> {
        self.write_all(&[
            num as u8,
            (num >> 8) as u8,
            (num >> 16) as u8,
            (num >> 24) as u8,
        ])
    }

    /// Write an unsigned integer (32bit) in big endian order (most significant byte first)
    #[allow(unused)]
    fn write_u32_be(&mut self, num: u32) -> std::io::Result<()> {
        self.write_all(&[
            (num >> 24) as u8,
            (num >> 16) as u8,
            (num >> 8) as u8,
            num as u8,
        ])
    }

    /// Write a variable length integer or unsigned integer.
    ///
    /// We don't use zig-zag encoding because we want to keep the option open
    /// to use the same function for BigInt and 53bit integers.
    ///
    /// We use the 7th bit instead for signaling that this is a negative number.
    #[inline]
    fn write_var<T: VarInt>(&mut self, num: T) -> std::io::Result<usize> {
        num.write(self)
    }

    /// Write a variable length integer or unsigned integer.
    ///
    /// We don't use zig-zag encoding because we want to keep the option open
    /// to use the same function for BigInt and 53bit integers.
    ///
    /// We use the 7th bit instead for signaling that this is a negative number.
    #[allow(unused)]
    #[inline]
    fn write_var_signed<T: SignedVarInt>(&mut self, num: &Signed<T>) -> std::io::Result<()> {
        T::write_signed(num, self)
    }

    /// Write variable length buffer (binary content).
    fn write_bytes<B: AsRef<[u8]>>(&mut self, buf: B) -> std::io::Result<usize> {
        let buf = buf.as_ref();
        let n = buf.len() + self.write_var(buf.len())?;
        self.write_all(buf)?;
        Ok(n)
    }

    /// Write variable-length utf8 string
    #[inline]
    fn write_string(&mut self, str: &str) -> std::io::Result<usize> {
        self.write_bytes(str)
    }

    /// Write floating point number in 4 bytes
    #[inline]
    fn write_f32(&mut self, num: f32) -> std::io::Result<()> {
        self.write_all(&num.to_be_bytes())
    }

    /// Write floating point number in 8 bytes
    #[inline]
    fn write_f64(&mut self, num: f64) -> std::io::Result<()> {
        self.write_all(&num.to_be_bytes())
    }

    /// Write BigInt in 8 bytes in big endian order.
    #[inline]
    fn write_i64(&mut self, num: i64) -> std::io::Result<()> {
        self.write_all(&num.to_be_bytes())
    }

    /// Write BigUInt in 8 bytes in big endian order.
    #[allow(unused)]
    #[inline]
    fn write_u64(&mut self, num: u64) -> std::io::Result<()> {
        self.write_all(&num.to_be_bytes())
    }
}

impl<W: Write> WriteExt for W {}

#[derive(Debug, Copy, Clone)]
pub struct BufferReservationError;
impl std::error::Error for BufferReservationError {}

impl Display for BufferReservationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "decoded buffer size would cause out of memory panic")
    }
}

pub trait Decoder: Read {
    /// Reset the value of current delete set state.
    fn reset_ds_cur_val(&mut self);

    /// Read next [DeleteSet] clock value.
    fn read_ds_clock(&mut self) -> crate::Result<Clock>;

    /// Read the number of clients stored in encoded [DeleteSet].
    fn read_ds_len(&mut self) -> crate::Result<Clock>;

    /// Read left origin of a currently decoded [Block].
    fn read_left_id(&mut self) -> crate::Result<ID>;

    /// Read right origin of a currently decoded [Block].
    fn read_right_id(&mut self) -> crate::Result<ID>;

    /// Read currently decoded client identifier.
    fn read_client(&mut self) -> crate::Result<ClientID>;

    /// Read info bit flags of a currently decoded [Block].
    fn read_info(&mut self) -> crate::Result<u8>;

    /// Read bit flags determining type of parent of a currently decoded [Block].
    fn read_parent_info(&mut self) -> crate::Result<bool>;

    /// Read type ref info of a currently decoded [Block] parent.
    fn read_type_ref(&mut self) -> crate::Result<u8>;

    /// Read length parameter.
    fn read_len(&mut self) -> crate::Result<Clock>;

    /// Read key string.
    fn read_key<W: Write>(&mut self, w: &mut W) -> crate::Result<u64>;

    /// Decode a JSON-like data type. It's a complex type which is an extension of native JavaScript
    /// Object Notation.
    fn read_any<D: DeserializeOwned>(&mut self) -> crate::Result<D>;

    /// Decode an embedded JSON string into [Any] struct. It's a complex type which is an extension
    /// of native JavaScript Object Notation.
    fn read_json<D: DeserializeOwned>(&mut self) -> crate::Result<D>;
}

pub trait Decode: Sized {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self>;

    fn decode(data: &[u8], version: Encoding) -> crate::Result<Self> {
        match version {
            Encoding::V1 => {
                let mut decoder = DecoderV1::from_slice(data);
                Self::decode_with(&mut decoder)
            }
            Encoding::V2 => {
                let mut decoder = DecoderV2::from_slice(data)?;
                Self::decode_with(&mut decoder)
            }
        }
    }
}

impl Decode for Range<Clock> {
    fn decode_with<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
        let clock = decoder.read_ds_clock()?;
        let len = decoder.read_ds_len()?;
        Ok(clock..(clock + len))
    }
}

pub trait ReadExt: Read + Sized {
    /// Read unsigned integer with variable length.
    /// * numbers < 2^7 are stored in one byte
    /// * numbers < 2^14 are stored in two bytes
    #[inline]
    fn read_var<T: VarInt>(&mut self) -> std::io::Result<T> {
        T::read(self)
    }

    /// Read unsigned integer with variable length.
    /// * numbers < 2^7 are stored in one byte
    /// * numbers < 2^14 are stored in two bytes
    #[inline]
    fn read_var_signed<T: SignedVarInt>(&mut self) -> std::io::Result<Signed<T>> {
        T::read_signed(self)
    }

    /// Read a variable length buffer.
    fn read_bytes<W: Write>(&mut self, w: &mut W) -> std::io::Result<u64> {
        let len: u64 = self.read_var()?;
        std::io::copy(&mut self.take(len), w)
    }

    /// Read string of variable length.
    fn read_string<W: Write>(&mut self, w: &mut W) -> std::io::Result<u64> {
        self.read_bytes(w)
    }

    /// Read float32 in big endian order
    fn read_f32(&mut self) -> std::io::Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }

    /// Read float64 in big endian order
    // @todo there must be a more elegant way to convert a slice to a fixed-length buffer.
    fn read_f64(&mut self) -> std::io::Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    /// Read BigInt64 in big endian order
    fn read_i64(&mut self) -> std::io::Result<i64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// read BigUInt64 in big endian order
    fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// read BigUInt64 in big endian order
    fn read_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl<T: Read> ReadExt for T {}
