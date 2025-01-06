use crate::block::ID;
use crate::varint::{Signed, SignedVarInt, VarInt};
use crate::{ClientID, Clock, U64};
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

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
    fn read_key(&mut self) -> crate::Result<Arc<str>>;

    /// Decode a JSON-like data type. It's a complex type which is an extension of native JavaScript
    /// Object Notation.
    fn read_any<A>(&mut self) -> crate::Result<A>;

    /// Decode an embedded JSON string into [Any] struct. It's a complex type which is an extension
    /// of native JavaScript Object Notation.
    fn read_json<A>(&mut self) -> crate::Result<A>;
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
    fn read_var<T: VarInt>(&mut self) -> crate::Result<T> {
        T::read(self)
    }

    /// Read unsigned integer with variable length.
    /// * numbers < 2^7 are stored in one byte
    /// * numbers < 2^14 are stored in two bytes
    #[inline]
    fn read_var_signed<T: SignedVarInt>(&mut self) -> crate::Result<Signed<T>> {
        T::read_signed(self)
    }

    /// Read a variable length buffer.
    fn read_buf(&mut self, buf: &mut Vec<u8>) -> crate::Result<()> {
        let len: u64 = self.read_var()?;
        if buf.try_reserve(len as usize).is_err() {
            return Err(crate::Error::ValueOutOfRange);
        }
        let len = buf.len() + len as usize;
        self.read_exact(unsafe { std::mem::transmute(buf.spare_capacity_mut()) })?;
        unsafe {
            buf.set_len(len);
        }
        Ok(())
    }

    /// Read string of variable length.
    fn read_string(&mut self, str: &mut String) -> crate::Result<()> {
        self.read_buf(unsafe { str.as_mut_vec() })
    }

    /// Read float32 in big endian order
    fn read_f32(&mut self) -> crate::Result<f32> {
        let mut buf = [0; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }

    /// Read float64 in big endian order
    // @todo there must be a more elegant way to convert a slice to a fixed-length buffer.
    fn read_f64(&mut self) -> crate::Result<f64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    /// Read BigInt64 in big endian order
    fn read_i64(&mut self) -> crate::Result<i64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// read BigUInt64 in big endian order
    fn read_u8(&mut self) -> crate::Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// read BigUInt64 in big endian order
    fn read_u64(&mut self) -> crate::Result<u64> {
        let mut buf = [0; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_be_bytes(buf))
    }
}

impl<T: Read> ReadExt for T {}
