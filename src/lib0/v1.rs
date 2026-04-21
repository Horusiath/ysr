use crate::lib0::{Decoder, Encoder, ReadExt, WriteExt};
use crate::{ClientID, Clock, ID, lib0};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::{Cursor, Read, Write};

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

    pub fn into_inner(self) -> W {
        self.writer
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
    fn write_ds_len(&mut self, len: Clock) -> crate::Result<usize> {
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
    fn write_len(&mut self, len: Clock) -> crate::Result<usize> {
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
        let str = serde_json::to_string(any)?;
        self.write_string(str.as_str())?;
        Ok(())
    }
}

pub struct DecoderV1<R> {
    reader: R,
}

impl<R: Read> DecoderV1<R> {
    #[inline]
    pub fn new(reader: R) -> Self {
        DecoderV1 { reader }
    }
}

impl<'a> DecoderV1<Cursor<&'a [u8]>> {
    pub fn from_slice<T>(slice: &'a T) -> Self
    where
        T: AsRef<[u8]> + ?Sized,
    {
        DecoderV1::new(Cursor::new(slice.as_ref()))
    }
}

impl<R: Read> DecoderV1<R> {
    fn read_id(&mut self) -> crate::Result<ID> {
        let client: ClientID = self.reader.read_var()?;
        let clock: Clock = self.reader.read_var()?;
        Ok(ID::new(client, clock))
    }
}

impl<R: Read> From<R> for DecoderV1<R> {
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
    fn read_ds_len(&mut self) -> crate::Result<Clock> {
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
    fn read_len(&mut self) -> crate::Result<Clock> {
        Ok(self.reader.read_var()?)
    }

    #[inline]
    fn read_key<W: Write>(&mut self, w: &mut W) -> crate::Result<u64> {
        Ok(self.read_string(w)?)
    }

    fn read_any<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        Ok(lib0::from_reader(&mut self.reader)?)
    }

    fn read_json<D: DeserializeOwned>(&mut self) -> crate::Result<D> {
        let mut buf = Vec::new();
        self.read_bytes(&mut buf)?;
        let data = serde_json::from_slice(&buf)?;
        Ok(data)
    }
}
