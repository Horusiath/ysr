use crate::block::ID;
use crate::varint::{Signed, SignedVarInt, VarInt};
use crate::{lib0, ClientID, Clock, U64};
use serde::Serialize;
use std::io::Write;
use std::ops::Range;

pub trait Encoder: Write {
    /// Reset the state of currently encoded [DeleteSet].
    fn reset_ds_cur_val(&mut self);

    /// Write a clock value of currently encoded [DeleteSet] client.
    fn write_ds_clock(&mut self, clock: Clock) -> crate::Result<usize>;

    /// Write a number of client entries used by currently encoded [DeleteSet].
    fn write_ds_len(&mut self, len: U64) -> crate::Result<usize>;

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
    fn write_len(&mut self, len: U64) -> crate::Result<usize>;

    /// Write a string key.
    fn write_key(&mut self, string: &str) -> crate::Result<usize>;

    /// Encode JSON-like data type. This is a complex structure which is an extension to JavaScript
    /// Object Notation with some extra cases.
    fn write_any<S: Serialize>(&mut self, any: &S) -> crate::Result<()>;

    /// Encode JSON-like data type as nested JSON string. This is a complex structure which is an
    /// extension to JavaScript Object Notation with some extra cases.
    fn write_json<S: Serialize>(&mut self, any: &S) -> crate::Result<()>;
}

pub trait Encode {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()>;
}

impl Encode for Range<Clock> {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> crate::Result<()> {
        encoder.write_ds_clock(self.start)?;
        encoder.write_ds_len(self.end - self.start)?;
        Ok(())
    }
}

pub trait WriteExt: Write + Sized {
    /// Write an unsigned integer (16bit)
    fn write_u8(&mut self, num: u8) -> std::io::Result<()> {
        Ok(self.write_all(&[num])?)
    }

    /// Write an unsigned integer (16bit)
    fn write_u16(&mut self, num: u16) -> std::io::Result<()> {
        Ok(self.write_all(&[num as u8, (num >> 8) as u8])?)
    }

    /// Write an unsigned integer (32bit)
    fn write_u32(&mut self, num: u32) -> std::io::Result<()> {
        Ok(self.write_all(&[
            num as u8,
            (num >> 8) as u8,
            (num >> 16) as u8,
            (num >> 24) as u8,
        ])?)
    }

    /// Write an unsigned integer (32bit) in big endian order (most significant byte first)
    fn write_u32_be(&mut self, num: u32) -> std::io::Result<()> {
        Ok(self.write_all(&[
            (num >> 24) as u8,
            (num >> 16) as u8,
            (num >> 8) as u8,
            num as u8,
        ])?)
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
        Ok(self.write_all(&num.to_be_bytes())?)
    }

    /// Write floating point number in 8 bytes
    #[inline]
    fn write_f64(&mut self, num: f64) -> std::io::Result<()> {
        Ok(self.write_all(&num.to_be_bytes())?)
    }

    /// Write BigInt in 8 bytes in big endian order.
    #[inline]
    fn write_i64(&mut self, num: i64) -> std::io::Result<()> {
        Ok(self.write_all(&num.to_be_bytes())?)
    }

    /// Write BigUInt in 8 bytes in big endian order.
    #[inline]
    fn write_u64(&mut self, num: u64) -> std::io::Result<()> {
        Ok(self.write_all(&num.to_be_bytes())?)
    }
}

impl<W: Write> WriteExt for W {}

#[repr(transparent)]
pub struct EncoderV1<W> {
    writer: W,
}

impl<W: Write> EncoderV1<W> {
    #[inline]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    fn write_id(&mut self, id: &ID) -> crate::Result<()> {
        self.write_var(id.client)?;
        self.write_var(id.clock)?;
        Ok(())
    }
}

impl<W: Write> From<W> for EncoderV1<W> {
    #[inline]
    fn from(writer: W) -> Self {
        Self::new(writer)
    }
}

impl<W: Write> Write for EncoderV1<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write> Encoder for EncoderV1<W> {
    #[inline]
    fn reset_ds_cur_val(&mut self) {}

    #[inline]
    fn write_ds_clock(&mut self, clock: Clock) -> crate::Result<usize> {
        Ok(self.writer.write_var(clock)?)
    }

    #[inline]
    fn write_ds_len(&mut self, len: U64) -> crate::Result<usize> {
        Ok(self.writer.write_var(len)?)
    }

    #[inline]
    fn write_left_id(&mut self, id: &ID) -> crate::Result<()> {
        self.write_id(id)
    }

    #[inline]
    fn write_right_id(&mut self, id: &ID) -> crate::Result<()> {
        self.write_id(id)
    }

    #[inline]
    fn write_client(&mut self, client: ClientID) -> crate::Result<usize> {
        Ok(self.writer.write_var(client)?)
    }

    #[inline]
    fn write_info(&mut self, info: u8) -> crate::Result<()> {
        Ok(self.writer.write_u8(info)?)
    }

    #[inline]
    fn write_parent_info(&mut self, is_y_key: bool) -> crate::Result<()> {
        self.write_var(if is_y_key { 1u8 } else { 0u8 })?;
        Ok(())
    }

    #[inline]
    fn write_type_ref(&mut self, info: u8) -> crate::Result<()> {
        self.write_var(info)?;
        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: U64) -> crate::Result<usize> {
        Ok(self.write_var(len)?)
    }

    #[inline]
    fn write_key(&mut self, string: &str) -> crate::Result<usize> {
        Ok(self.write_string(string)?)
    }

    fn write_any<S: Serialize>(&mut self, any: &S) -> crate::Result<()> {
        lib0::to_writer(&mut self.writer, any)?;
        Ok(())
    }

    fn write_json<S: Serialize>(&mut self, any: &S) -> crate::Result<()> {
        serde_json::to_writer(&mut self.writer, any)?;
        Ok(())
    }
}
