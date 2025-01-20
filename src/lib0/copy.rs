use crate::lib0::{
    TAG_ARRAY, TAG_BIGINT, TAG_BYTE_ARRAY, TAG_FALSE, TAG_FLOAT32, TAG_FLOAT64, TAG_INTEGER,
    TAG_NULL, TAG_OBJECT, TAG_STRING, TAG_TRUE, TAG_UNDEFINED,
};
use crate::read::ReadExt;
use crate::write::WriteExt;
use std::io::{Read, Write};

/// Copies the next object stored in lib0 any binary format from a given `src` to a `dst`.
/// Returns a number of bytes copied this way.
pub fn copy<R: Read, W: Write>(src: &mut R, dst: &mut W) -> Result<usize, super::Error> {
    let mut n = 0;
    copy_any(src, dst, &mut n)?;
    Ok(n)
}

fn copy_any<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    n: &mut usize,
) -> Result<(), super::Error> {
    let tag = src.read_u8()?;
    if tag < 116 || tag > 127 {
        return Err(super::Error::UnknownTag(tag));
    }
    dst.write_u8(tag)?;
    *n += 1;
    match tag {
        TAG_UNDEFINED | TAG_NULL | TAG_TRUE | TAG_FALSE => { /* do nothing */ }
        TAG_INTEGER => {
            let num: i64 = src.read_var()?;
            *n += dst.write_var(num)?;
        }
        TAG_FLOAT32 => {
            let num: f32 = src.read_f32()?;
            dst.write_f32(num)?;
            *n += 4;
        }
        TAG_FLOAT64 => {
            let num: f64 = src.read_f64()?;
            dst.write_f64(num)?;
            *n += 8;
        }
        TAG_BIGINT => {
            let num: i64 = src.read_var()?;
            *n += dst.write_var(num)?;
        }
        TAG_STRING => copy_string(src, dst, n)?,
        TAG_OBJECT => copy_object(src, dst, n)?,
        TAG_ARRAY => copy_array(src, dst, n)?,
        TAG_BYTE_ARRAY => {
            let mut buf = Vec::new();
            src.read_bytes(&mut buf)?;
            *n += dst.write_bytes(&buf)?;
        }
        tag => return Err(super::Error::UnknownTag(tag)),
    }
    Ok(())
}

fn copy_string<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    n: &mut usize,
) -> Result<(), super::Error> {
    let mut buf = String::new();
    src.read_string(&mut buf)?;
    *n += dst.write_string(&buf)?;
    Ok(())
}

fn copy_object<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    n: &mut usize,
) -> Result<(), super::Error> {
    let len: usize = src.read_var()?;
    *n += dst.write_var(len)?;
    for _ in 0..len {
        copy_string(src, dst, n)?;
        copy_any(src, dst, n)?;
    }
    Ok(())
}

fn copy_array<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    n: &mut usize,
) -> Result<(), super::Error> {
    let len: usize = src.read_var()?;
    *n += dst.write_var(len)?;
    for _ in 0..len {
        copy_any(src, dst, n)?;
    }
    Ok(())
}
