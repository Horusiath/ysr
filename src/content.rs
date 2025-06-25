use crate::block::{
    CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_GC, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::node::NodeHeader;
use crate::varint::var_u64_from_slice;
use crate::{lib0, U64};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::marker::PhantomData;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
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
pub(crate) enum BlockContent<'a> {
    Deleted(U64) = CONTENT_TYPE_DELETED,
    Json(ContentJson<'a>) = CONTENT_TYPE_JSON,
    Atom(ContentAtom<'a>) = CONTENT_TYPE_ATOM,
    Binary(&'a [u8]) = CONTENT_TYPE_BINARY,
    Embed(&'a [u8]) = CONTENT_TYPE_EMBED,
    Text(&'a str) = CONTENT_TYPE_STRING,
    Node(&'a NodeHeader) = CONTENT_TYPE_NODE,
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

pub struct ContentIter<'a> {
    data: &'a [u8],
}

impl<'a> ContentIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for ContentIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let (data_len, read) = var_u64_from_slice(self.data);
        if read == 0 {
            None
        } else {
            self.data = &self.data[..read];
            Some(&self.data[..data_len as usize])
        }
    }
}

#[repr(transparent)]
pub struct ContentJson<'a> {
    data: &'a [u8],
}

impl<'a> ContentJson<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn iter<D>(&self) -> ContentJsonIter<'a, D>
    where
        D: Deserialize<'a>,
    {
        ContentJsonIter {
            inner: ContentIter::new(self.data),
            _marker: PhantomData::default(),
        }
    }
}

impl<'a> Display for ContentJson<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut i = self.iter::<serde_json::Value>();
        write!(f, "[")?;
        if let Some(res) = i.next() {
            let v = res.map_err(|e| std::fmt::Error)?;
            write!(f, "{}", v)?;
        }
        while let Some(res) = i.next() {
            let v = res.map_err(|e| std::fmt::Error)?;
            write!(f, ", {}", v)?;
        }

        write!(f, "]")
    }
}

pub struct ContentJsonIter<'a, D> {
    inner: ContentIter<'a>,
    _marker: PhantomData<D>,
}

impl<'a, D> Iterator for ContentJsonIter<'a, D>
where
    D: Deserialize<'a>,
{
    type Item = Result<D, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.inner.next()?;
        match serde_json::from_slice(slice) {
            Ok(data) => Some(Ok(data)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[repr(transparent)]
pub struct ContentAtom<'a> {
    data: &'a [u8],
}

impl<'a> ContentAtom<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn iter<D>(&self) -> ContentAtomIter<'a, D>
    where
        D: DeserializeOwned,
    {
        ContentAtomIter {
            inner: ContentIter::new(self.data),
            _marker: PhantomData::default(),
        }
    }
}

impl<'a> Display for ContentAtom<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut i = self.iter::<lib0::Value>();
        write!(f, "[")?;
        if let Some(res) = i.next() {
            let v = res.map_err(|e| std::fmt::Error)?;
            write!(f, "{}", v)?;
        }
        while let Some(res) = i.next() {
            let v = res.map_err(|e| std::fmt::Error)?;
            write!(f, ", {}", v)?;
        }

        write!(f, "]")
    }
}

pub struct ContentAtomIter<'a, D> {
    inner: ContentIter<'a>,
    _marker: PhantomData<D>,
}

impl<'a, D> Iterator for ContentAtomIter<'a, D>
where
    D: DeserializeOwned,
{
    type Item = Result<D, lib0::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.inner.next()?;
        match lib0::from_reader(Cursor::new(slice)) {
            Ok(data) => Some(Ok(data)),
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct ContentFormat<'a> {
    key: &'a str,
    value: &'a [u8],
}

impl<'a> ContentFormat<'a> {
    pub fn new(data: &'a [u8]) -> crate::Result<Self> {
        let mut iter = ContentIter::new(data);
        let key = iter.next().ok_or(crate::Error::EndOfBuffer)?;
        let key = std::str::from_utf8(key).map_err(|e| lib0::Error::Utf8(e))?;
        let value = iter.next().ok_or(crate::Error::EndOfBuffer)?;
        Ok(Self { key, value })
    }

    pub fn key(&self) -> &'a str {
        self.key
    }

    pub fn value(&self) -> &'a [u8] {
        self.value
    }
}

impl<'a> Display for ContentFormat<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "\"{}\"={:?}", self.key, self.value)
    }
}
