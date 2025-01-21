use crate::block::ID;
use crate::varint::{Signed, SignedVarInt, VarInt};
use crate::{lib0, ClientID, Clock, U64};
use serde::de::DeserializeOwned;
use std::alloc::{Allocator, Global, GlobalAlloc};
use std::fmt::{Debug, Display, Formatter};
use std::io::{ErrorKind, Read};
use std::ops::Range;

#[derive(Copy, Clone)]
pub struct BufferReservationError;
impl std::error::Error for BufferReservationError {}

impl Debug for BufferReservationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

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
    fn read_ds_len(&mut self) -> crate::Result<U64>;

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
    fn read_len(&mut self) -> crate::Result<U64>;

    /// Read key string.
    fn read_key(&mut self, buf: &mut String) -> crate::Result<()>;

    /// Decode a JSON-like data type. It's a complex type which is an extension of native JavaScript
    /// Object Notation.
    fn read_any<D: DeserializeOwned>(&mut self) -> crate::Result<D>;

    /// Decode an embedded JSON string into [Any] struct. It's a complex type which is an extension
    /// of native JavaScript Object Notation.
    fn read_json<D: DeserializeOwned>(&mut self) -> crate::Result<D>;
}

pub trait Decode: Sized {
    fn decode<D: Decoder>(decoder: &mut D) -> crate::Result<Self>;
}

impl Decode for Range<Clock> {
    fn decode<D: Decoder>(decoder: &mut D) -> crate::Result<Self> {
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
    fn read_bytes<A: Allocator>(&mut self, buf: &mut Vec<u8, A>) -> std::io::Result<()> {
        let len: u64 = self.read_var()?;
        if buf.try_reserve(len as usize).is_err() {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                BufferReservationError,
            ));
        }
        let len = buf.len() + len as usize;
        let slice: &mut [u8] = unsafe { std::mem::transmute(buf.spare_capacity_mut()) };
        self.read_exact(&mut slice[0..len])?;
        unsafe {
            buf.set_len(len);
        }
        Ok(())
    }

    /// Read string of variable length.
    fn read_string(&mut self, str: &mut String) -> std::io::Result<()> {
        self.read_bytes(unsafe { str.as_mut_vec() })
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

pub struct DecoderV1<R, A: Allocator = Global> {
    reader: R,
    alloc: A,
}

impl<R: Read> DecoderV1<R, Global> {
    #[inline]
    pub fn new(reader: R) -> Self {
        DecoderV1 {
            reader,
            alloc: Global,
        }
    }
}

impl<R: Read, A: Allocator> DecoderV1<R, A> {
    #[inline]
    pub fn new_in(reader: R, alloc: A) -> Self {
        DecoderV1 { reader, alloc }
    }

    fn read_id(&mut self) -> crate::Result<ID> {
        let client: ClientID = self.reader.read_var()?;
        let clock: Clock = self.reader.read_var()?;
        Ok(ID::new(client, clock))
    }
}

impl<R: Read> From<R> for DecoderV1<R, Global> {
    #[inline]
    fn from(reader: R) -> Self {
        Self::new(reader)
    }
}

impl<R: Read> Read for DecoderV1<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<R: Read> Decoder for DecoderV1<R> {
    #[inline]
    fn reset_ds_cur_val(&mut self) {}

    #[inline]
    fn read_ds_clock(&mut self) -> crate::Result<Clock> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_ds_len(&mut self) -> crate::Result<U64> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_left_id(&mut self) -> crate::Result<ID> {
        self.read_id()
    }

    #[inline]
    fn read_right_id(&mut self) -> crate::Result<ID> {
        self.read_id()
    }

    #[inline]
    fn read_client(&mut self) -> crate::Result<ClientID> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_info(&mut self) -> crate::Result<u8> {
        Ok(self.reader.read_u8()?)
    }

    fn read_parent_info(&mut self) -> crate::Result<bool> {
        let flag: usize = self.reader.read_var()?;
        Ok(flag == 1)
    }

    #[inline]
    fn read_type_ref(&mut self) -> crate::Result<u8> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_len(&mut self) -> crate::Result<U64> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_key(&mut self, buf: &mut String) -> crate::Result<()> {
        Ok(self.read_string(buf)?)
    }

    fn read_any<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        Ok(lib0::from_reader(&mut self.reader)?)
    }

    fn read_json<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        let mut buf = Vec::new_in(self.alloc);
        self.read_bytes(&mut buf)?;
        let data = serde_json::from_slice(&buf)?;
        Ok(data)
    }
}
