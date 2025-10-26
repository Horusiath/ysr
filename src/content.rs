use crate::block::{
    Block, CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::lib0::Value;
use crate::{lib0, Unmounted};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::marker::PhantomData;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub(crate) enum ContentType {
    Deleted = CONTENT_TYPE_DELETED,
    Json = CONTENT_TYPE_JSON,
    Binary = CONTENT_TYPE_BINARY,
    String = CONTENT_TYPE_STRING,
    Embed = CONTENT_TYPE_EMBED,
    Format = CONTENT_TYPE_FORMAT,
    Node = CONTENT_TYPE_NODE,
    Atom = CONTENT_TYPE_ATOM,
    Doc = CONTENT_TYPE_DOC,
}

impl ContentType {
    pub fn is_empty(&self) -> bool {
        match self {
            ContentType::Node | ContentType::Deleted => true,
            _ => false,
        }
    }

    pub fn is_countable(self) -> bool {
        match self {
            ContentType::Atom => true,
            ContentType::Binary => true,
            ContentType::Doc => true,
            ContentType::Json => true,
            ContentType::Embed => true,
            ContentType::String => true,
            ContentType::Node => true,
            ContentType::Deleted => false,
            ContentType::Format => false,
            //ContentType::Move => false,
        }
    }

    #[inline]
    pub fn is_mergeable(self) -> bool {
        match self {
            ContentType::Atom => true,
            ContentType::Json => true,
            ContentType::String => true,
            ContentType::Deleted => true,
            _ => false,
        }
    }
}

impl Display for ContentType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentType::Deleted => write!(f, "deleted"),
            ContentType::Json => write!(f, "json"),
            ContentType::Binary => write!(f, "binary"),
            ContentType::String => write!(f, "string"),
            ContentType::Embed => write!(f, "embed"),
            ContentType::Format => write!(f, "format"),
            ContentType::Node => write!(f, "node"),
            ContentType::Atom => write!(f, "atom"),
            ContentType::Doc => write!(f, "doc"),
        }
    }
}

impl TryFrom<u8> for ContentType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            CONTENT_TYPE_DELETED => Ok(ContentType::Deleted),
            CONTENT_TYPE_JSON => Ok(ContentType::Json),
            CONTENT_TYPE_BINARY => Ok(ContentType::Binary),
            CONTENT_TYPE_STRING => Ok(ContentType::String),
            CONTENT_TYPE_EMBED => Ok(ContentType::Embed),
            CONTENT_TYPE_FORMAT => Ok(ContentType::Format),
            CONTENT_TYPE_NODE => Ok(ContentType::Node),
            CONTENT_TYPE_ATOM => Ok(ContentType::Atom),
            CONTENT_TYPE_DOC => Ok(ContentType::Doc),
            _ => Err(crate::Error::UnsupportedContent(value)),
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub(crate) enum BlockContent<'a> {
    Deleted = CONTENT_TYPE_DELETED,
    Json(ContentRef<'a, JsonEncoding>) = CONTENT_TYPE_JSON,
    Atom(ContentRef<'a, AtomEncoding>) = CONTENT_TYPE_ATOM,
    Binary(&'a [u8]) = CONTENT_TYPE_BINARY,
    Embed(&'a [u8]) = CONTENT_TYPE_EMBED,
    Text(&'a str) = CONTENT_TYPE_STRING,
    Node = CONTENT_TYPE_NODE,
    Format(ContentFormat<'a>) = CONTENT_TYPE_FORMAT,
    Doc(&'a [u8]) = CONTENT_TYPE_DOC,
    // to be supported..
    // Move(&'a Move) = CONTENT_TYPE_MOVE,
}

impl<'a> BlockContent<'a> {
    pub fn new(content_type: ContentType, data: &'a [u8]) -> crate::Result<Self> {
        Ok(match content_type {
            ContentType::Deleted => BlockContent::Deleted,
            ContentType::Json => BlockContent::Json(ContentRef::new(data)),
            ContentType::Binary => BlockContent::Binary(data),
            ContentType::String => BlockContent::Text(unsafe { str::from_utf8_unchecked(data) }),
            ContentType::Embed => BlockContent::Embed(data),
            ContentType::Format => BlockContent::Format(ContentFormat::new(data)?),
            ContentType::Node => BlockContent::Node,
            ContentType::Atom => BlockContent::Atom(ContentRef::new(data)),
            ContentType::Doc => BlockContent::Doc(data),
        })
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Deleted => ContentType::Deleted,
            Self::Atom(_) => ContentType::Atom,
            Self::Binary(_) => ContentType::Binary,
            Self::Doc(_) => ContentType::Doc,
            Self::Embed(_) => ContentType::Embed,
            Self::Format(_) => ContentType::Format,
            Self::Node => ContentType::Node,
            Self::Text(_) => ContentType::String,
            Self::Json(_) => ContentType::Json,
        }
    }

    pub fn body(&self) -> &[u8] {
        match self {
            Self::Deleted => &[],
            Self::Json(jsons) => jsons.inner.body(),
            Self::Atom(atoms) => atoms.inner.body(),
            Self::Binary(bin) => bin,
            Self::Embed(bin) => bin,
            Self::Text(text) => text.as_bytes(),
            Self::Node => &[],
            Self::Format(format) => format.body(),
            Self::Doc(doc) => doc,
        }
    }
}

impl<'a> Display for BlockContent<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deleted => write!(f, "deleted"),
            Self::Json(jsons) => write!(f, "{}", jsons),
            Self::Atom(atoms) => write!(f, "{}", atoms),
            Self::Binary(bin) => write!(f, "binary({})", simple_base64::encode(bin)),
            Self::Embed(bin) => write!(f, "embed({})", simple_base64::encode(bin)),
            Self::Text(text) => write!(f, "'{}'", text),
            Self::Node => write!(f, "node"),
            Self::Format(format) => write!(f, "{}", format),
            Self::Doc(doc) => todo!("Display::fmt(doc)"),
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub(crate) enum BlockContentMut<'a> {
    Deleted = CONTENT_TYPE_DELETED,
    Json(ContentRef<'a, JsonEncoding>) = CONTENT_TYPE_JSON,
    Atom(ContentRef<'a, AtomEncoding>) = CONTENT_TYPE_ATOM,
    Binary(&'a [u8]) = CONTENT_TYPE_BINARY,
    Embed(&'a [u8]) = CONTENT_TYPE_EMBED,
    Text(&'a str) = CONTENT_TYPE_STRING,
    Node = CONTENT_TYPE_NODE,
    Format(ContentFormat<'a>) = CONTENT_TYPE_FORMAT,
    Doc(&'a [u8]) = CONTENT_TYPE_DOC,
    // to be supported..
    // Move(&'a Move) = CONTENT_TYPE_MOVE,
}

impl<'a> BlockContentMut<'a> {
    #[inline]
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Deleted => ContentType::Deleted,
            Self::Atom(_) => ContentType::Atom,
            Self::Binary(_) => ContentType::Binary,
            Self::Doc(_) => ContentType::Doc,
            Self::Embed(_) => ContentType::Embed,
            Self::Format(_) => ContentType::Format,
            Self::Node => ContentType::Node,
            Self::Text(_) => ContentType::String,
            Self::Json(_) => ContentType::Json,
        }
    }
}

impl<'a> Display for BlockContentMut<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deleted => write!(f, "deleted"),
            Self::Json(jsons) => write!(f, "{}", jsons),
            Self::Atom(atoms) => write!(f, "{}", atoms),
            Self::Binary(bin) => write!(f, "binary({})", simple_base64::encode(bin)),
            Self::Embed(bin) => write!(f, "embed({})", simple_base64::encode(bin)),
            Self::Text(text) => write!(f, "'{}'", text),
            Self::Node => write!(f, "node"),
            Self::Format(format) => write!(f, "{}", format),
            Self::Doc(doc) => todo!("Display::fmt(doc)"),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct ContentIter<'a> {
    data: &'a [u8],
}

impl<'a> ContentIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    fn body(&self) -> &'a [u8] {
        self.data
    }

    pub fn get(&self, mut index: usize) -> Option<&'a [u8]> {
        let mut iter = self.clone();
        while index > 0 {
            iter.next()?;
            index -= 1;
        }
        iter.next()
    }

    pub fn slice(&self, mut index: usize) -> Option<&'a [u8]> {
        let mut iter = self.clone();
        while index > 0 {
            iter.next()?;
            index -= 1;
        }
        Some(iter.data)
    }
}

impl<'a> Iterator for ContentIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }

        let len =
            u32::from_be_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;
        let slice = &self.data[4..4 + len];
        self.data = &self.data[4 + len..];

        Some(slice)
    }
}

pub trait Encoding {
    fn serialize<W, T>(writer: &mut W, value: &T) -> crate::Result<()>
    where
        W: std::io::Write,
        T: serde::Serialize;

    fn deserialize<T>(data: &[u8]) -> crate::Result<T>
    where
        T: DeserializeOwned;

    fn fmt(data: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result;
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct JsonEncoding;
impl Encoding for JsonEncoding {
    fn serialize<W, T>(writer: &mut W, value: &T) -> crate::Result<()>
    where
        W: std::io::Write,
        T: serde::Serialize,
    {
        serde_json::to_writer(writer, value).map_err(crate::Error::from)
    }

    fn deserialize<T>(data: &[u8]) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(data).map_err(crate::Error::from)
    }

    fn fmt(data: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result {
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(value) => write!(f, "{}", value),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct AtomEncoding;
impl Encoding for AtomEncoding {
    fn serialize<W, T>(writer: &mut W, value: &T) -> crate::Result<()>
    where
        W: std::io::Write,
        T: serde::Serialize,
    {
        lib0::to_writer(writer, value).map_err(crate::Error::from)
    }

    fn deserialize<T>(data: &[u8]) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        let cursor = Cursor::new(data);
        lib0::from_reader(cursor).map_err(crate::Error::from)
    }

    fn fmt(data: &[u8], f: &mut Formatter<'_>) -> std::fmt::Result {
        let cursor = Cursor::new(data);
        match lib0::from_reader::<_, lib0::Value>(cursor) {
            Ok(value) => write!(f, "{}", value),
            Err(_) => Err(std::fmt::Error),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct ContentRef<'a, E> {
    inner: ContentIter<'a>,
    _encoding: PhantomData<E>,
}

impl<'a, E> ContentRef<'a, E> {
    pub fn new(slice: &'a [u8]) -> Self {
        let inner = ContentIter::new(slice);
        Self {
            inner,
            _encoding: PhantomData::default(),
        }
    }

    pub fn iter<D>(&self) -> Iter<'a, E, D>
    where
        D: Deserialize<'a>,
    {
        Iter {
            inner: self.inner.clone(),
            _deserializer: PhantomData::default(),
            _target_type: PhantomData::default(),
        }
    }
}

impl<'a, E> Debug for ContentRef<'a, E>
where
    E: Encoding,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl<'a, E> Display for ContentRef<'a, E>
where
    E: Encoding,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut i = self.inner.clone().into_iter();
        write!(f, "[")?;
        if let Some(res) = i.next() {
            E::fmt(res, f)?;
        }
        while let Some(res) = i.next() {
            E::fmt(res, f)?;
        }

        write!(f, "]")
    }
}

pub struct Iter<'a, E, D> {
    inner: ContentIter<'a>,
    _deserializer: PhantomData<E>,
    _target_type: PhantomData<D>,
}

impl<'a, E, D> Iterator for Iter<'a, E, D>
where
    E: Encoding,
    D: DeserializeOwned,
{
    type Item = crate::Result<D>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.inner.next()?;

        match E::deserialize(slice) {
            Ok(data) => Some(Ok(data)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(PartialEq)]
pub struct ContentFormat<'a> {
    data: &'a [u8],
}

impl<'a> ContentFormat<'a> {
    pub fn new(data: &'a [u8]) -> crate::Result<Self> {
        if data.len() < 2 {
            return Err(crate::Error::EndOfBuffer);
        }
        let key_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + key_len {
            return Err(crate::Error::EndOfBuffer);
        }

        Ok(Self { data })
    }

    fn body(&self) -> &'a [u8] {
        self.data
    }

    fn key_len(&self) -> usize {
        u16::from_be_bytes([self.data[0], self.data[1]]) as usize
    }

    pub fn key(&self) -> &'a str {
        let key_bytes = &self.data[2..2 + self.key_len()];
        unsafe { std::str::from_utf8_unchecked(key_bytes) }
    }

    pub fn value(&self) -> &'a [u8] {
        let key_len = self.key_len();
        &self.data[2 + key_len..]
    }
}

impl<'a> Debug for ContentFormat<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl<'a> Display for ContentFormat<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\"={:?}", self.key(), self.value())
    }
}

pub trait TryFromContent: Sized {
    fn try_from_content(block: Block<'_>, content: BlockContent<'_>) -> crate::Result<Self>;
}

impl TryFromContent for lib0::Value {
    fn try_from_content(block: Block<'_>, content: BlockContent<'_>) -> crate::Result<Self> {
        match content {
            BlockContent::Deleted => Err(crate::Error::NotFound),
            BlockContent::Json(value) => {
                let value = value
                    .iter::<Self>()
                    .next()
                    .ok_or(crate::Error::NotFound)??;
                Ok(value)
            }
            BlockContent::Atom(value) => {
                let value = value
                    .iter::<Self>()
                    .next()
                    .ok_or(crate::Error::NotFound)??;
                Ok(value)
            }
            BlockContent::Binary(value) | BlockContent::Embed(value) => {
                Ok(Value::ByteArray(Bytes::copy_from_slice(value)))
            }
            BlockContent::Text(value) => Ok(Value::String(value.into())),
            _ => Err(crate::Error::InvalidMapping("Value")),
        }
    }
}

impl TryFromContent for String {
    fn try_from_content(block: Block<'_>, content: BlockContent<'_>) -> crate::Result<Self> {
        match content {
            BlockContent::Text(str) => Ok(str.into()),
            BlockContent::Deleted => Err(crate::Error::NotFound),
            BlockContent::Json(value) => {
                let value = value
                    .iter::<Self>()
                    .next()
                    .ok_or(crate::Error::NotFound)??;
                Ok(value)
            }
            BlockContent::Atom(value) => {
                let value = value
                    .iter::<Self>()
                    .next()
                    .ok_or(crate::Error::NotFound)??;
                Ok(value)
            }
            BlockContent::Binary(value) | BlockContent::Embed(value) => {
                let str = std::str::from_utf8(value)
                    .map_err(|e| crate::Error::InvalidMapping("String"))?;
                Ok(str.to_string())
            }
            _ => Err(crate::Error::InvalidMapping("String")),
        }
    }
}

impl<T> TryFromContent for Unmounted<T> {
    fn try_from_content(block: Block<'_>, _content: BlockContent<'_>) -> crate::Result<Self> {
        if block.is_deleted() {
            return Err(crate::Error::NotFound);
        } else if block.content_type() == ContentType::Node {
            Ok(Unmounted::nested(*block.id()))
        } else {
            Err(crate::Error::InvalidMapping("Unmounted"))
        }
    }
}

#[cfg(test)]
mod test {
    use crate::content::{ContentRef, JsonEncoding};
    use bytes::{BufMut, BytesMut};
    use serde_json::json;

    #[test]
    fn iter() {
        let alice = json!({"name": "Alice"}).to_string();
        let bob = json!({"name": "Bob"}).to_string();
        let mut data = BytesMut::new();
        data.put_u32_le(alice.len() as u32);
        data.put_slice(alice.as_bytes());
        data.put_u32_le(bob.len() as u32);
        data.put_slice(bob.as_bytes());

        let content: ContentRef<'_, JsonEncoding> = ContentRef::new(&data);
        let mut iter = content.iter::<serde_json::Value>();
        assert_eq!(iter.next().unwrap().unwrap(), json!({"name": "Alice"}));
        assert_eq!(iter.next().unwrap().unwrap(), json!({"name": "Bob"}));
        assert!(iter.next().is_none());
    }
}
