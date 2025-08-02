use crate::block::{
    CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_GC, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::lib0::{Error, Value};
use crate::node::NodeHeader;
use crate::varint::var_u64_from_slice;
use crate::{lib0, Clock, U64};
use serde::de::DeserializeOwned;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::marker::PhantomData;
use std::ops::Deref;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

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
    Deleted(Clock) = CONTENT_TYPE_DELETED,
    Json(ContentRef<'a, JsonEncoding>) = CONTENT_TYPE_JSON,
    Atom(ContentRef<'a, AtomEncoding>) = CONTENT_TYPE_ATOM,
    Binary(&'a [u8]) = CONTENT_TYPE_BINARY,
    Embed(&'a [u8]) = CONTENT_TYPE_EMBED,
    Text(&'a str) = CONTENT_TYPE_STRING,
    Node(ContentNode<'a>) = CONTENT_TYPE_NODE,
    Format(ContentFormat<'a>) = CONTENT_TYPE_FORMAT,
    Doc(&'a [u8]) = CONTENT_TYPE_DOC,
    // to be supported..
    // Move(&'a Move) = CONTENT_TYPE_MOVE,
}

impl<'a> BlockContent<'a> {
    pub fn content_type(&self) -> ContentType {
        match self {
            BlockContent::Deleted(_) => ContentType::Deleted,
            BlockContent::Atom(_) => ContentType::Atom,
            BlockContent::Binary(_) => ContentType::Binary,
            BlockContent::Doc(_) => ContentType::Doc,
            BlockContent::Embed(_) => ContentType::Embed,
            BlockContent::Format(_) => ContentType::Format,
            BlockContent::Node(_) => ContentType::Node,
            BlockContent::Text(_) => ContentType::String,
            BlockContent::Json(_) => ContentType::Json,
        }
    }

    pub fn body(&self) -> &[u8] {
        match self {
            BlockContent::Deleted(_) => &[],
            BlockContent::Json(jsons) => jsons.inner.body(),
            BlockContent::Atom(atoms) => atoms.inner.body(),
            BlockContent::Binary(bin) => bin,
            BlockContent::Embed(bin) => bin,
            BlockContent::Text(text) => text.as_bytes(),
            BlockContent::Node(node) => node.as_bytes(),
            BlockContent::Format(format) => format.body(),
            BlockContent::Doc(doc) => doc,
        }
    }
}

impl<'a> Display for BlockContent<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockContent::Deleted(len) => write!(f, "deleted({})", len),
            BlockContent::Json(jsons) => write!(f, "{}", jsons),
            BlockContent::Atom(atoms) => write!(f, "{}", atoms),
            BlockContent::Binary(bin) => write!(f, "binary({})", simple_base64::encode(bin)),
            BlockContent::Embed(bin) => write!(f, "embed({})", simple_base64::encode(bin)),
            BlockContent::Text(text) => write!(f, "'{}'", text),
            BlockContent::Node(node) => write!(f, "{}", node),
            BlockContent::Format(format) => write!(f, "{}", format),
            BlockContent::Doc(doc) => todo!("Display::fmt(doc)"),
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
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;
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
        D: DeserializeOwned,
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
pub struct ContentNode<'a> {
    data: &'a [u8],
}

impl<'a> ContentNode<'a> {
    pub fn new(data: &'a [u8]) -> crate::Result<Self> {
        if NodeHeader::try_ref_from_prefix(data).is_err() {
            return Err(crate::Error::InvalidMapping("NodeHeader"));
        }

        Ok(Self { data })
    }

    pub fn as_bytes(&self) -> &'a [u8] {
        self.data
    }

    pub fn header(&self) -> &'a NodeHeader {
        let (header, _) = NodeHeader::ref_from_prefix(self.data).unwrap();
        header
    }

    pub fn name(&self) -> &'a str {
        let (_, suffix) = NodeHeader::ref_from_prefix(self.data).unwrap();
        unsafe { std::str::from_utf8_unchecked(suffix) }
    }
}

impl<'a> Deref for ContentNode<'a> {
    type Target = NodeHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

impl<'a> Debug for ContentNode<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.header(), f)
    }
}

impl<'a> Display for ContentNode<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (header, suffix) = NodeHeader::ref_from_prefix(self.data).unwrap();
        write!(f, "{:?}", header.node_type())?;
        if !suffix.is_empty() {
            write!(f, "::'{}'", unsafe {
                std::str::from_utf8_unchecked(suffix)
            })
        } else {
            Ok(())
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
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + key_len {
            return Err(crate::Error::EndOfBuffer);
        }

        Ok(Self { data })
    }

    fn body(&self) -> &'a [u8] {
        self.data
    }

    fn key_len(&self) -> usize {
        u16::from_le_bytes([self.data[0], self.data[1]]) as usize
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
