use crate::read::ReadExt;
use crate::write::WriteExt;
use crate::{ClientID, U64};
use std::convert::Infallible;
use std::fmt::{Display, Formatter};
use std::io::{ErrorKind, Read, Write};

#[derive(Debug, Clone, Copy)]
pub struct VarIntOutOfRangeError;
impl std::error::Error for VarIntOutOfRangeError {}
impl Display for VarIntOutOfRangeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "variable length integer is out of range")
    }
}

fn out_of_range<T>() -> std::io::Result<T> {
    Err(std::io::Error::new(
        ErrorKind::Other,
        Box::new(VarIntOutOfRangeError),
    ))
}

pub trait VarInt: Sized + Copy {
    /// Write current number into given writer using variable size integer encoding.
    /// Returns a number of bytes written.
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize>;
    /// Read a number from given reader using variable size integer encoding.
    fn read<R: Read>(r: &mut R) -> std::io::Result<Self>;
}

impl VarInt for ClientID {
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u64((*self).into(), w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_u64(r)?;
        match ClientID::try_from(U64::new(value)) {
            Ok(id) => Ok(id),
            Err(_) => out_of_range(),
        }
    }
}

impl VarInt for U64 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u64(self.get(), w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        Ok(read_var_u64(r)?.into())
    }
}

impl VarInt for usize {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u64(*self as u64, w)
    }

    #[inline]
    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        Ok(read_var_u64(r)? as Self)
    }
}

impl VarInt for u128 {
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        let mut n = 1;
        let mut value = *self;
        while value >= 0b10000000 {
            let b = ((value & 0b01111111) as u8) | 0b10000000;
            w.write_u8(b)?;
            n += 1;
            value = value >> 7;
        }
        w.write_u8((value & 0b01111111) as u8)?;
        Ok(n)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let mut num = 0u128;
        let mut len: usize = 0;
        loop {
            let r = r.read_u8()?;
            num |= u128::wrapping_shl((r & 0b01111111) as u128, len as u32);
            len += 7;
            if r < 0b10000000 {
                return Ok(num);
            }
            if len > 180 {
                return out_of_range();
            }
        }
    }
}

impl VarInt for u64 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u64(*self, w)
    }

    #[inline]
    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        read_var_u64(r)
    }
}

impl VarInt for u32 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u32(*self, w)
    }

    #[inline]
    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        read_var_u32(r)
    }
}

impl VarInt for u16 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u32(*self as u32, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_u32(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

impl VarInt for u8 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_u32(*self as u32, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_u32(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

impl VarInt for isize {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_i64(*self as i64, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_i64(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

impl VarInt for i64 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_i64(*self, w)
    }

    #[inline]
    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        read_var_i64(r)
    }
}

impl VarInt for i32 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_i64(*self as i64, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_i64(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

impl VarInt for i16 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_i64(*self as i64, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_i64(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

impl VarInt for i8 {
    #[inline]
    fn write<W: Write>(&self, w: &mut W) -> std::io::Result<usize> {
        write_var_i64(*self as i64, w)
    }

    fn read<R: Read>(r: &mut R) -> std::io::Result<Self> {
        let value = read_var_i64(r)?;
        if let Ok(value) = value.try_into() {
            Ok(value)
        } else {
            out_of_range()
        }
    }
}

fn write_var_u32<W: Write>(mut value: u32, w: &mut W) -> std::io::Result<usize> {
    let mut n = 1;
    while value >= 0b10000000 {
        let b = ((value & 0b01111111) as u8) | 0b10000000;
        w.write_u8(b)?;
        n += 1;
        value = value >> 7;
    }
    w.write_u8((value & 0b01111111) as u8)?;
    Ok(n)
}

fn write_var_u64<W: Write>(mut value: u64, w: &mut W) -> std::io::Result<usize> {
    let mut n = 1;
    while value >= 0b10000000 {
        let b = ((value & 0b01111111) as u8) | 0b10000000;
        w.write_u8(b)?;
        n += 1;
        value = value >> 7;
    }
    w.write_u8((value & 0b01111111) as u8)?;
    Ok(n)
}

fn write_var_i64<W: Write>(mut value: i64, w: &mut W) -> std::io::Result<usize> {
    let mut n = 1;
    let is_negative = value < 0;
    value = if is_negative { -value } else { value };
    w.write_u8(
        // whether to continue reading
        (if value > 0b00111111i64 { 0b10000000u8 } else { 0 })
            // whether number is negative
            | (if is_negative { 0b01000000u8 } else { 0 })
            // number
            | (0b00111111i64 & value) as u8,
    )?;
    value >>= 6;
    while value > 0 {
        w.write_u8(
            if value > 0b01111111i64 {
                0b10000000u8
            } else {
                0
            } | (0b01111111i64 & value) as u8,
        )?;
        n += 1;
        value >>= 7;
    }
    Ok(n)
}

fn read_var_u64<R: Read>(r: &mut R) -> std::io::Result<u64> {
    let mut num = 0;
    let mut len: usize = 0;
    loop {
        let r = r.read_u8()?;
        num |= u64::wrapping_shl((r & 0b01111111) as u64, len as u32);
        len += 7;
        if r < 0b10000000 {
            return Ok(num);
        }
        if len > 70 {
            return out_of_range();
        }
    }
}

pub(crate) fn var_u64_from_slice(r: &[u8]) -> (u64, usize) {
    let mut num = 0;
    let mut len = 0;
    for &r in r {
        num |= u64::wrapping_shl((r & 0b01111111) as u64, len as u32);
        len += 7;
        if r < 0b10000000 {
            return (num, len / 7);
        }
        if len > 70 {
            break;
        }
    }
    (0, 0)
}

fn read_var_u32<R: Read>(r: &mut R) -> std::io::Result<u32> {
    let mut num = 0;
    let mut len: usize = 0;
    loop {
        let r = r.read_u8()?;
        num |= u32::wrapping_shl((r & 0b01111111) as u32, len as u32);
        len += 7;
        if r < 0b10000000 {
            return Ok(num);
        }
        if len > 70 {
            // a proper setting for 32bit int would be 35 bits, however for Yjs compatibility
            // we allow wrap up up to 64bit ints (with int overflow wrap)
            return out_of_range();
        }
    }
}

fn read_var_i64<R: Read>(reader: &mut R) -> std::io::Result<i64> {
    let mut r = reader.read_u8()?;
    let mut num = (r & 0b00111111u8) as i64;
    let mut len: u32 = 6;
    let is_negative = r & 0b01000000u8 > 0;
    if r & 0b10000000u8 == 0 {
        return Ok(if is_negative { -num } else { num });
    }
    loop {
        r = reader.read_u8()?;
        num |= (r as i64 & 0b01111111i64) << len;
        len += 7;
        if r < 0b10000000u8 {
            return Ok(if is_negative { -num } else { num });
        }
        if len > 70 {
            return out_of_range();
        }
    }
}

pub trait SignedVarInt: Sized + Copy {
    fn write_signed<W: Write>(value: &Signed<Self>, w: &mut W) -> std::io::Result<()>;
    fn read_signed<R: Read>(r: &mut R) -> std::io::Result<Signed<Self>>;
}

/// Struct which recognizes signed integer values. This special case has been build for Yjs encoding
/// compatibility, which recognizes differences between `0` and `-0`, which is used in some
/// cases (eg. `UIntOptRleDecoder`).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Signed<T: Sized + Copy> {
    value: T,
    is_negative: bool,
}

impl<T: Sized + Copy> Signed<T> {
    pub fn new(value: T, is_negative: bool) -> Self {
        Signed { value, is_negative }
    }

    /// Returns true is stored number is a positive value.
    pub fn is_positive(&self) -> bool {
        !self.is_negative
    }

    /// Returns true is stored number is a negative value (including `-0` as a special case).
    pub fn is_negative(&self) -> bool {
        self.is_negative
    }

    /// Returns a stored value.
    pub fn value(&self) -> T {
        self.value
    }

    /// Maps contents of a [Signed] value container to a new data type, retaining the sign
    /// information.
    pub fn map<F, U>(&self, f: F) -> Signed<U>
    where
        F: FnOnce(T) -> U,
        U: Sized + Copy,
    {
        let mapped = f(self.value);
        Signed::new(mapped, self.is_negative)
    }
}

impl SignedVarInt for i64 {
    fn write_signed<W: Write>(s: &Signed<Self>, w: &mut W) -> std::io::Result<()> {
        let mut value = s.value;
        let is_negative = s.is_negative;
        value = if is_negative { -value } else { value };
        w.write_u8(
            // whether to continue reading
            (if value > 0b00111111i64 { 0b10000000u8 } else { 0 })
                // whether number is negative
                | (if is_negative { 0b01000000u8 } else { 0 })
                // number
                | (0b00111111i64 & value) as u8,
        )?;
        value >>= 6;
        while value > 0 {
            w.write_u8(
                if value > 0b01111111i64 {
                    0b10000000u8
                } else {
                    0
                } | (0b01111111i64 & value) as u8,
            )?;
            value >>= 7;
        }
        Ok(())
    }

    fn read_signed<R: Read>(reader: &mut R) -> std::io::Result<Signed<Self>> {
        let mut r = reader.read_u8()?;
        let mut num = (r & 0b00111111u8) as i64;
        let mut len: u32 = 6;
        let is_negative = r & 0b01000000u8 > 0;
        if r & 0b10000000u8 == 0 {
            let num = if is_negative { -num } else { num };
            return Ok(Signed::new(num, is_negative));
        }
        loop {
            r = reader.read_u8()?;
            num |= (r as i64 & 0b01111111i64) << len;
            len += 7;
            if r < 0b10000000u8 {
                let num = if is_negative { -num } else { num };
                return Ok(Signed::new(num, is_negative));
            }
            if len > 70 {
                return out_of_range();
            }
        }
    }
}

impl SignedVarInt for isize {
    fn write_signed<W: Write>(value: &Signed<Self>, w: &mut W) -> std::io::Result<()> {
        let value = value.map(|v| v as i64);
        i64::write_signed(&value, w)
    }

    fn read_signed<R: Read>(r: &mut R) -> std::io::Result<Signed<Self>> {
        let result = i64::read_signed(r)?;
        match result.value.try_into() {
            Ok(i) => Ok(Signed::new(i, result.is_negative)),
            Err(_) => out_of_range(),
        }
    }
}

impl SignedVarInt for i32 {
    fn write_signed<W: Write>(value: &Signed<Self>, w: &mut W) -> std::io::Result<()> {
        let value = value.map(|v| v as i64);
        i64::write_signed(&value, w)
    }

    fn read_signed<R: Read>(r: &mut R) -> std::io::Result<Signed<Self>> {
        let result = i64::read_signed(r)?;
        match result.value.try_into() {
            Ok(i) => Ok(Signed::new(i, result.is_negative)),
            Err(_) => out_of_range(),
        }
    }
}

impl SignedVarInt for i16 {
    fn write_signed<W: Write>(value: &Signed<Self>, w: &mut W) -> std::io::Result<()> {
        let value = value.map(|v| v as i64);
        i64::write_signed(&value, w)
    }

    fn read_signed<R: Read>(r: &mut R) -> std::io::Result<Signed<Self>> {
        let result = i64::read_signed(r)?;
        match result.value.try_into() {
            Ok(i) => Ok(Signed::new(i, result.is_negative)),
            Err(_) => out_of_range(),
        }
    }
}

impl SignedVarInt for i8 {
    fn write_signed<W: Write>(value: &Signed<Self>, w: &mut W) -> std::io::Result<()> {
        let value = value.map(|v| v as i64);
        i64::write_signed(&value, w)
    }

    fn read_signed<R: Read>(r: &mut R) -> std::io::Result<Signed<Self>> {
        let result = i64::read_signed(r)?;
        match result.value.try_into() {
            Ok(i) => Ok(Signed::new(i, result.is_negative)),
            Err(_) => out_of_range(),
        }
    }
}
