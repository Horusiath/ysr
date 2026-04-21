use crate::lib0::varint::Signed;
use crate::lib0::{Decoder, Encoder, ReadExt, WriteExt};
use crate::{ClientID, Clock, ID};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;
use zerocopy::{FromZeros, U32};

pub struct EncoderV2<W> {
    writer: W,
    key_table: HashMap<String, u32>,
    rest: Vec<u8>,
    ds_curr_val: Clock,
    seqeuncer: u32,
    key_clock_encoder: IntDiffOptRleEncoder,
    client_encoder: UIntOptRleEncoder,
    left_clock_encoder: IntDiffOptRleEncoder,
    right_clock_encoder: IntDiffOptRleEncoder,
    info_encoder: RleEncoder,
    string_encoder: StringEncoder,
    parent_info_encoder: RleEncoder,
    type_ref_encoder: UIntOptRleEncoder,
    len_encoder: UIntOptRleEncoder,
}

impl<W: Write> EncoderV2<W> {
    pub fn new(writer: W) -> Self {
        EncoderV2 {
            writer,
            key_table: HashMap::new(),
            rest: Vec::new(),
            seqeuncer: 0,
            ds_curr_val: Clock::new(0),
            key_clock_encoder: IntDiffOptRleEncoder::new(),
            client_encoder: UIntOptRleEncoder::new(),
            left_clock_encoder: IntDiffOptRleEncoder::new(),
            right_clock_encoder: IntDiffOptRleEncoder::new(),
            info_encoder: RleEncoder::new(),
            string_encoder: StringEncoder::new(),
            parent_info_encoder: RleEncoder::new(),
            type_ref_encoder: UIntOptRleEncoder::new(),
            len_encoder: UIntOptRleEncoder::new(),
        }
    }

    pub fn into_inner(mut self) -> crate::Result<W> {
        self.flush()?;
        Ok(self.writer)
    }
}

impl<W: Write> Write for EncoderV2<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.rest.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let key_clock = self.key_clock_encoder.finish()?;
        let client = self.client_encoder.finish()?;
        let left_clock = self.left_clock_encoder.finish()?;
        let right_clock = self.right_clock_encoder.finish()?;
        let info = self.info_encoder.finish();
        let string = self.string_encoder.finish()?;
        let parent_info = self.parent_info_encoder.finish();
        let type_ref = self.type_ref_encoder.finish()?;
        let len = self.len_encoder.finish()?;
        let rest = &self.rest;
        let mut buf = Vec::new();
        buf.write_u8(0)?; // this is a feature flag that we might use in the future
        buf.write_bytes(key_clock)?;
        buf.write_bytes(client)?;
        buf.write_bytes(left_clock)?;
        buf.write_bytes(right_clock)?;
        buf.write_bytes(info)?;
        buf.write_bytes(string)?;
        buf.write_bytes(parent_info)?;
        buf.write_bytes(type_ref)?;
        buf.write_bytes(len)?;
        buf.write_all(rest)?;
        Ok(())
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.rest.extend_from_slice(buf);
        Ok(())
    }
}

impl<W: Write> Encoder for EncoderV2<W> {
    #[inline]
    fn reset_ds_cur_val(&mut self) {
        self.ds_curr_val = Clock::new(0);
    }

    fn write_ds_clock(&mut self, clock: Clock) -> crate::Result<usize> {
        let diff = clock - self.ds_curr_val;
        self.ds_curr_val = clock;
        Ok(self.rest.write_var(diff)?)
    }

    fn write_ds_len(&mut self, len: Clock) -> crate::Result<usize> {
        debug_assert!(len != 0);
        let n = self.rest.write_var(len - 1)?;
        self.ds_curr_val += len;
        Ok(n)
    }

    fn write_left_id(&mut self, id: &ID) -> crate::Result<()> {
        self.client_encoder.write_u64(id.client.0.get() as u64);
        self.left_clock_encoder.write_u32(id.clock.get());
        Ok(())
    }

    fn write_right_id(&mut self, id: &ID) -> crate::Result<()> {
        self.client_encoder.write_u64(id.client.0.get() as u64);
        self.right_clock_encoder.write_u32(id.clock.get());
        Ok(())
    }

    #[inline]
    fn write_client(&mut self, client: ClientID) -> crate::Result<usize> {
        self.client_encoder.write_u64(client.0.get() as u64);
        Ok(0)
    }

    #[inline]
    fn write_info(&mut self, info: u8) -> crate::Result<()> {
        self.info_encoder.write_u8(info);
        Ok(())
    }

    #[inline]
    fn write_parent_info(&mut self, is_y_key: bool) -> crate::Result<()> {
        self.parent_info_encoder
            .write_u8(if is_y_key { 1 } else { 0 });
        Ok(())
    }

    #[inline]
    fn write_type_ref(&mut self, info: u8) -> crate::Result<()> {
        self.type_ref_encoder.write_u64(info as u64);
        Ok(())
    }

    #[inline]
    fn write_len(&mut self, len: Clock) -> crate::Result<usize> {
        self.len_encoder.write_u64(len.get() as u64);
        Ok(0)
    }

    fn write_key(&mut self, key: &str) -> crate::Result<usize> {
        //TODO: this is wrong (key_table is never updated), but this behavior matches Yjs
        self.key_clock_encoder.write_u32(self.seqeuncer);
        self.seqeuncer += 1;
        if self.key_table.get(key).is_none() {
            self.string_encoder.write(key);
        }
        Ok(0)
    }

    #[inline]
    fn write_any<S: Serialize>(&mut self, value: &S) -> crate::Result<()> {
        super::to_writer(&mut self.rest, value)?;
        Ok(())
    }

    #[inline]
    fn write_json<S: Serialize>(&mut self, any: &S) -> crate::Result<()> {
        self.write_any(any)
    }
}

/// A combination of the IntDiffEncoder and the UintOptRleEncoder.
///
/// The count approach is similar to the UintDiffOptRleEncoder, but instead of using the negative bitflag, it encodes
/// in the LSB whether a count is to be read. Therefore this Encoder only supports 31 bit integers!
///
/// Encodes [1, 2, 3, 2] as [3, 1, 6, -1] (more specifically [(1 << 1) | 1, (3 << 0) | 0, -1])
///
/// Internally uses variable length encoding. Contrary to normal UintVar encoding, the first byte contains:
/// * 1 bit that denotes whether the next value is a count (LSB)
/// * 1 bit that denotes whether this value is negative (MSB - 1)
/// * 1 bit that denotes whether to continue reading the variable length integer (MSB)
///
/// Therefore, only five bits remain to encode diff ranges.
///
/// Use this Encoder only when appropriate. In most cases, this is probably a bad idea.
struct IntDiffOptRleEncoder {
    buf: Vec<u8>,
    last: u32,
    count: u32,
    diff: i32,
}

impl IntDiffOptRleEncoder {
    fn new() -> Self {
        IntDiffOptRleEncoder {
            buf: Vec::new(),
            last: 0,
            count: 0,
            diff: 0,
        }
    }

    fn write_u32(&mut self, value: u32) {
        let diff = value as i32 - self.last as i32;
        if self.diff == diff {
            self.last = value;
            self.count += 1;
        } else {
            self.flush();
            self.count = 1;
            self.diff = diff;
            self.last = value;
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.count > 0 {
            // 31 bit making up the diff | wether to write the counter
            let encode_diff = self.diff << 1 | (if self.count == 1 { 0 } else { 1 });
            // flush counter, unless this is the first value (count = 0)
            // case 1: just a single value. set first bit to positive
            // case 2: write several values. set first bit to negative to indicate that there is a length coming
            self.buf.write_var(encode_diff as i64)?;
            if self.count > 1 {
                self.buf.write_var(self.count - 2)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<&[u8]> {
        self.flush()?;
        Ok(&self.buf)
    }
}

/// Optimized Rle encoder that does not suffer from the mentioned problem of the basic Rle encoder.
///
/// Internally uses VarInt encoder to write unsigned integers. If the input occurs multiple times, we write
/// write it as a negative number. The UintOptRleDecoder then understands that it needs to read a count.
///
/// Encodes [1,2,3,3,3] as [1,2,-3,3] (once 1, once 2, three times 3)
struct UIntOptRleEncoder {
    buf: Vec<u8>,
    last: u64,
    count: u32,
}

impl UIntOptRleEncoder {
    fn new() -> Self {
        UIntOptRleEncoder {
            buf: Vec::new(),
            last: 0,
            count: 0,
        }
    }

    fn write_u64(&mut self, value: u64) {
        if self.last == value {
            self.count += 1;
        } else {
            self.flush();
            self.count = 1;
            self.last = value;
        }
    }

    fn finish(&mut self) -> std::io::Result<&[u8]> {
        self.flush()?;
        Ok(&self.buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.count > 0 {
            // flush counter, unless this is the first value (count = 0)
            // case 1: just a single value. set sign to positive
            // case 2: write several values. set sign to negative to indicate that there is a length coming
            if self.count == 1 {
                self.buf.write_var(self.last as i64)?;
            } else {
                let value = Signed::new(-(self.last as i64), true);
                self.buf.write_var_signed(&value)?;
                self.buf.write_var(self.count - 2)?;
            }
        }
        Ok(())
    }
}

/// Basic Run Length Encoder - a basic compression implementation.
///
/// Encodes [1,1,1,7] to [1,3,7,1] (3 times 1, 1 time 7). This encoder might do more harm than good if there are a lot of values that are not repeated.
///
/// It was originally used for image compression. Cool .. article http://csbruce.com/cbm/transactor/pdfs/trans_v7_i06.pdf
struct RleEncoder {
    buf: Vec<u8>,
    last: Option<u8>,
    count: u32,
}

impl RleEncoder {
    fn new() -> Self {
        RleEncoder {
            buf: Vec::new(),
            last: None,
            count: 0,
        }
    }

    fn write_u8(&mut self, value: u8) {
        if self.last == Some(value) {
            self.count += 1;
        } else {
            if self.count > 0 {
                // flush counter, unless this is the first value (count = 0)
                self.buf.write_var(self.count - 1);
            }
            self.count = 1;
            self.buf.write_u8(value);
            self.last = Some(value);
        }
    }

    fn finish(&self) -> &[u8] {
        &self.buf
    }
}

/// Optimized String Encoder.
///
/// Encoding many small strings in a simple Encoder is not very efficient. The function call to decode a string takes some time and creates references that must be eventually deleted.
/// In practice, when decoding several million small strings, the GC will kick in more and more often to collect orphaned string objects (or maybe there is another reason?).
///
/// This string encoder solves the above problem. All strings are concatenated and written as a single string using a single encoding call.
///
/// The lengths are encoded using a UintOptRleEncoder.
struct StringEncoder {
    buf: String,
    len_encoder: UIntOptRleEncoder,
}

impl StringEncoder {
    fn new() -> Self {
        StringEncoder {
            buf: String::new(),
            len_encoder: UIntOptRleEncoder::new(),
        }
    }

    fn write(&mut self, str: &str) {
        let utf16_len = str.encode_utf16().count(); // Yjs encodes offsets using utf-16
        self.buf.push_str(str);
        self.len_encoder.write_u64(utf16_len as u64);
    }

    fn finish(&mut self) -> std::io::Result<Vec<u8>> {
        let lengths = self.len_encoder.finish()?;
        let mut buf = Vec::with_capacity(self.buf.len() + lengths.len());
        buf.write_string(&self.buf)?;
        buf.write_all(lengths)?;
        Ok(buf)
    }
}

/// Version 2 of lib0 decoder.
pub struct DecoderV2<R> {
    reader: R,
    keys: Vec<String>,
    ds_curr_val: Clock,
    key_clock_decoder: IntDiffOptRleDecoder,
    client_decoder: UIntOptRleDecoder,
    left_clock_decoder: IntDiffOptRleDecoder,
    right_clock_decoder: IntDiffOptRleDecoder,
    info_decoder: RleDecoder,
    string_decoder: StringDecoder,
    parent_info_decoder: RleDecoder,
    type_ref_decoder: UIntOptRleDecoder,
    len_decoder: UIntOptRleDecoder,
}

impl<'a> DecoderV2<Cursor<&'a [u8]>> {
    pub fn from_slice(buf: &'a [u8]) -> crate::Result<Self> {
        Self::new(Cursor::new(buf))
    }
}

impl<R: Read> DecoderV2<R> {
    pub fn new(mut reader: R) -> crate::Result<Self> {
        // read feature flag - currently unused
        let _: u8 = reader.read_u8()?;

        let key_clock_buf = Self::read_buf(&mut reader)?;
        let client_buf = Self::read_buf(&mut reader)?;
        let left_clock_buf = Self::read_buf(&mut reader)?;
        let right_clock_buf = Self::read_buf(&mut reader)?;
        let info_buf = Self::read_buf(&mut reader)?;
        let string_buf = Self::read_buf(&mut reader)?;
        let parent_info_buf = Self::read_buf(&mut reader)?;
        let type_ref_buf = Self::read_buf(&mut reader)?;
        let len_buf = Self::read_buf(&mut reader)?;
        Ok(DecoderV2 {
            reader,
            ds_curr_val: Clock::new(0),
            keys: Vec::new(),
            key_clock_decoder: IntDiffOptRleDecoder::new(Cursor::new(key_clock_buf)),
            client_decoder: UIntOptRleDecoder::new(Cursor::new(client_buf)),
            left_clock_decoder: IntDiffOptRleDecoder::new(Cursor::new(left_clock_buf)),
            right_clock_decoder: IntDiffOptRleDecoder::new(Cursor::new(right_clock_buf)),
            info_decoder: RleDecoder::new(Cursor::new(info_buf)),
            string_decoder: StringDecoder::new(Cursor::new(string_buf))?,
            parent_info_decoder: RleDecoder::new(Cursor::new(parent_info_buf)),
            type_ref_decoder: UIntOptRleDecoder::new(Cursor::new(type_ref_buf)),
            len_decoder: UIntOptRleDecoder::new(Cursor::new(len_buf)),
        })
    }

    fn read_buf(reader: &mut R) -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        reader.read_bytes(&mut buf)?;
        Ok(buf)
    }
}

impl<R: Read> Read for DecoderV2<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

impl<R: Read> Decoder for DecoderV2<R> {
    fn reset_ds_cur_val(&mut self) {
        self.ds_curr_val = Clock::new(0);
    }

    fn read_ds_clock(&mut self) -> crate::Result<Clock> {
        self.ds_curr_val += self.reader.read_var::<u32>()?;
        Ok(self.ds_curr_val)
    }

    fn read_ds_len(&mut self) -> crate::Result<Clock> {
        let diff: Clock = self.reader.read_var::<Clock>()? + 1;
        self.ds_curr_val += diff;
        Ok(diff)
    }

    fn read_left_id(&mut self) -> crate::Result<ID> {
        let client_id = self.client_decoder.read_client()?;
        Ok(ID::new(
            client_id,
            self.left_clock_decoder.read_u32()?.into(),
        ))
    }

    fn read_right_id(&mut self) -> crate::Result<ID> {
        let client_id = self.client_decoder.read_client()?;
        Ok(ID::new(
            client_id,
            self.right_clock_decoder.read_u32()?.into(),
        ))
    }

    fn read_client(&mut self) -> crate::Result<ClientID> {
        self.client_decoder.read_client()
    }

    fn read_info(&mut self) -> crate::Result<u8> {
        self.info_decoder.read_u8()
    }

    fn read_parent_info(&mut self) -> crate::Result<bool> {
        Ok(self.parent_info_decoder.read_u8()? == 1)
    }

    fn read_type_ref(&mut self) -> crate::Result<u8> {
        Ok(self.type_ref_decoder.read_u64()? as u8)
    }

    fn read_len(&mut self) -> crate::Result<Clock> {
        Ok(Clock::new(self.len_decoder.read_u64()? as u32))
    }

    fn read_any<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        Ok(super::from_reader(&mut self.reader)?)
    }

    fn read_json<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        Self::read_any(self)
    }

    fn read_key<W: Write>(&mut self, w: &mut W) -> crate::Result<u64> {
        let key_clock = self.key_clock_decoder.read_u32()?;
        if let Some(key) = self.keys.get(key_clock as usize) {
            w.write_all(key.as_bytes())?;
            Ok(key.len() as u64)
        } else {
            let key = self.string_decoder.read_str()?;
            self.keys.push(key.into());
            w.write_all(key.as_bytes())?;
            Ok(key.len() as u64)
        }
    }
}

struct IntDiffOptRleDecoder {
    cursor: Cursor<Vec<u8>>,
    last: u32,
    count: u32,
    diff: i32,
}

impl<'a> IntDiffOptRleDecoder {
    fn new(cursor: Cursor<Vec<u8>>) -> Self {
        IntDiffOptRleDecoder {
            cursor,
            last: 0,
            count: 0,
            diff: 0,
        }
    }

    fn read_u32(&mut self) -> crate::Result<u32> {
        if self.count == 0 {
            let diff = self.cursor.read_var::<i32>()?;
            // if the first bit is set, we read more data
            let has_count = diff & 1;
            self.diff = (diff >> 1) as i32;
            self.count = if has_count != 0 {
                self.cursor.read_var::<u32>()? + 2
            } else {
                1
            };
        }
        self.last = ((self.last as i32) + self.diff) as u32;
        self.count -= 1;
        Ok(self.last)
    }
}

struct UIntOptRleDecoder {
    cursor: Cursor<Vec<u8>>,
    last: u64,
    count: u32,
}

impl<'a> UIntOptRleDecoder {
    fn new(cursor: Cursor<Vec<u8>>) -> Self {
        UIntOptRleDecoder {
            cursor,
            last: 0,
            count: 0,
        }
    }

    fn read_u64(&mut self) -> crate::Result<u64> {
        if self.count == 0 {
            let s = self.cursor.read_var_signed::<i64>()?;
            // if the sign is negative, we read the count too, otherwise count is 1
            let is_negative = s.is_negative();
            let value = if is_negative {
                self.count = self.cursor.read_var::<u32>()? + 2;
                (-s.value()) as u64
            } else {
                self.count = 1;
                s.value() as u64
            };
            self.last = value;
        }
        self.count -= 1;
        Ok(self.last)
    }

    fn read_client(&mut self) -> crate::Result<ClientID> {
        let client_id = self.read_u64()?;
        if client_id > u32::MAX as u64 || client_id == 0 {
            return Err(crate::Error::ClientIDOutOfRange);
        }
        Ok(unsafe { ClientID::new_unchecked(client_id as u32) })
    }
}

struct RleDecoder {
    cursor: Cursor<Vec<u8>>,
    last: u8,
    count: i32,
}

impl<'a> RleDecoder {
    fn new(cursor: Cursor<Vec<u8>>) -> Self {
        RleDecoder {
            cursor,
            last: 0,
            count: 0,
        }
    }

    fn read_u8(&mut self) -> crate::Result<u8> {
        if self.count == 0 {
            self.last = self.cursor.read_u8()?;
            if self.cursor.position() < self.cursor.get_ref().len() as u64 {
                self.count = (self.cursor.read_var::<u32>()? as i32) + 1; // see encoder implementation for the reason why this is incremented
            } else {
                self.count = -1; // read the current value forever
            }
        }
        self.count -= 1;
        Ok(self.last)
    }
}

struct StringDecoder {
    buf: String,
    len_decoder: UIntOptRleDecoder,
    pos: usize,
}

impl StringDecoder {
    fn new(mut cursor: Cursor<Vec<u8>>) -> crate::Result<Self> {
        let str_bin = DecoderV2::read_buf(&mut cursor)?;
        let str = unsafe { String::from_utf8_unchecked(str_bin) };
        let len_decoder = UIntOptRleDecoder::new(cursor);
        Ok(StringDecoder {
            pos: 0,
            buf: str,
            len_decoder,
        })
    }

    fn read_str(&mut self) -> crate::Result<&str> {
        let mut remaining = self.len_decoder.read_u64()? as usize;
        let mut i = 0;
        let start = &self.buf[self.pos..];
        for c in start.chars() {
            if remaining == 0 {
                break;
            }
            i += c.len_utf8(); // rust uses offsets as utf-8 bytes
            remaining -= c.len_utf16(); // but yjs provides them as utf-16
        }
        let result = &start[..i];
        self.pos += i;
        Ok(result)
    }
}
