use crate::block::{
    Block, CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::lib0::Value;
use crate::write::WriteExt;
use crate::{lib0, Unmounted};
use bytes::Bytes;
use lmdb_rs_m::{MdbValue, ToMdbValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, ExtendFromSlice, SmallVec};
use std::ffi::c_void;
use std::fmt::{Debug, Display, Formatter};
use std::io::{Cursor, Write};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
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

    #[inline]
    pub fn has_content(self) -> bool {
        match self {
            ContentType::Deleted => false,
            ContentType::Json => true,
            ContentType::Binary => true,
            ContentType::String => true,
            ContentType::Embed => true,
            ContentType::Format => true,
            ContentType::Node => false,
            ContentType::Atom => true,
            ContentType::Doc => true,
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

pub type InlineBytes = SmallVec<[u8; 16]>;

#[derive(Clone, PartialEq)]
pub struct BlockContent {
    data: InlineBytes,
}

impl BlockContent {
    pub fn from_bytes<D: Into<InlineBytes>>(data: D) -> crate::Result<Self> {
        let data = data.into();
        let _ = ContentType::try_from(data[0])?;
        Ok(BlockContent { data })
    }

    pub fn new(content_type: ContentType) -> Self {
        BlockContent {
            data: smallvec![content_type as u8],
        }
    }

    pub fn deleted() -> Self {
        BlockContentRef::DELETED.to_owned()
    }

    pub fn node() -> Self {
        BlockContentRef::NODE.to_owned()
    }

    pub fn atom<S>(value: &S) -> crate::Result<Self>
    where
        S: Serialize,
    {
        let mut content = BlockContent::new(ContentType::Atom);
        lib0::to_writer(&mut content, value)?;
        Ok(content)
    }

    pub fn json<S>(value: &S) -> crate::Result<Self>
    where
        S: Serialize,
    {
        let mut content = BlockContent::new(ContentType::Json);
        serde_json::to_writer(&mut content, value)?;
        Ok(content)
    }

    pub fn binary<A: AsRef<[u8]>>(value: A) -> Self {
        let mut content = BlockContent::new(ContentType::Binary);
        content.data.extend_from_slice(value.as_ref());
        content
    }

    pub fn embed<A: AsRef<[u8]>>(value: A) -> Self {
        let mut content = BlockContent::new(ContentType::Embed);
        content.data.extend_from_slice(value.as_ref());
        content
    }

    pub fn string<S: AsRef<str>>(value: S) -> Self {
        let mut content = BlockContent::new(ContentType::String);
        content.data.extend_from_slice(value.as_ref().as_bytes());
        content
    }

    pub fn format<K, V>(key: K, value: V) -> Self
    where
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        let mut content = BlockContent::new(ContentType::Format);
        content.write_var(key.len()).unwrap();
        content.write_string(key).unwrap();
        content.write_var(value.len()).unwrap();
        content.write_all(value).unwrap();
        content
    }

    pub fn as_ref(&self) -> BlockContentRef<'_> {
        BlockContentRef { data: &self.data }
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        ContentType::try_from(self.data[0]).unwrap()
    }

    #[inline]
    pub fn body(&self) -> &[u8] {
        &self.data[1..]
    }

    pub fn as_text(&self) -> Option<&str> {
        if self.content_type() == ContentType::String {
            Some(unsafe { std::str::from_utf8_unchecked(self.body()) })
        } else {
            None
        }
    }

    pub fn as_format(&self) -> Option<ContentFormat<'_>> {
        if self.content_type() == ContentType::Format {
            ContentFormat::new(self.body()).ok()
        } else {
            None
        }
    }

    pub fn as_json(&self) -> Option<ContentRef<'_, JsonEncoding>> {
        if self.content_type() == ContentType::Json {
            Some(ContentRef::new(self.body()))
        } else {
            None
        }
    }

    pub fn as_atom(&self) -> Option<ContentRef<'_, AtomEncoding>> {
        if self.content_type() == ContentType::Atom {
            Some(ContentRef::new(self.body()))
        } else {
            None
        }
    }
}

impl Deref for BlockContent {
    type Target = InlineBytes;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for BlockContent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl std::io::Write for BlockContent {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.data.extend_from_slice(buf);
        Ok(())
    }
}

impl ToMdbValue for BlockContent {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let data = self.data.as_ptr() as *const c_void;
        let len = self.data.len();
        unsafe { MdbValue::new(data, len) }
    }
}

impl Debug for BlockContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Display for BlockContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.as_ref(), f)
    }
}

impl<'a> From<BlockContentRef<'a>> for BlockContent {
    #[inline]
    fn from(b: BlockContentRef<'a>) -> Self {
        b.to_owned()
    }
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BlockContentRef<'a> {
    data: &'a [u8],
}

impl BlockContentRef<'static> {
    pub const DELETED: Self = BlockContentRef {
        data: &[ContentType::Deleted as u8],
    };
    pub const NODE: Self = BlockContentRef {
        data: &[ContentType::Node as u8],
    };
}

impl<'a> BlockContentRef<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, crate::Error> {
        let _ = ContentType::try_from(data[0])?;
        Ok(BlockContentRef { data })
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        ContentType::try_from(self.data[0]).unwrap()
    }

    #[inline]
    pub fn body(&self) -> &'a [u8] {
        &self.data[1..]
    }

    pub fn to_owned(self) -> BlockContent {
        BlockContent {
            data: self.data.into(),
        }
    }

    pub fn as_text(&self) -> Option<&'a str> {
        if self.content_type() == ContentType::String {
            Some(unsafe { std::str::from_utf8_unchecked(self.body()) })
        } else {
            None
        }
    }

    pub fn as_format(&self) -> Option<ContentFormat<'a>> {
        if self.content_type() == ContentType::Format {
            ContentFormat::new(self.body()).ok()
        } else {
            None
        }
    }

    pub fn as_json(&self) -> Option<ContentRef<'a, JsonEncoding>> {
        if self.content_type() == ContentType::Json {
            Some(ContentRef::new(self.body()))
        } else {
            None
        }
    }

    pub fn as_atom(&self) -> Option<ContentRef<'a, AtomEncoding>> {
        if self.content_type() == ContentType::Atom {
            Some(ContentRef::new(self.body()))
        } else {
            None
        }
    }
}

impl<'a> Display for BlockContentRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let body = self.body();
        match self.content_type() {
            ContentType::Deleted => write!(f, "deleted"),
            ContentType::Json => write!(f, "{}", ContentRef::<'_, JsonEncoding>::new(body)),
            ContentType::Atom => write!(f, "{}", ContentRef::<'_, AtomEncoding>::new(body)),
            ContentType::Binary => write!(f, "binary({})", simple_base64::encode(body)),
            ContentType::Embed => write!(f, "embed({})", simple_base64::encode(body)),
            ContentType::String => {
                write!(f, "'{}'", unsafe { std::str::from_utf8_unchecked(body) })
            }
            ContentType::Node => write!(f, "node"),
            ContentType::Format => write!(f, "{}", ContentFormat::new(body).unwrap()),
            ContentType::Doc => todo!("Display::fmt(doc)"),
        }
    }
}

impl<'a> ToMdbValue for BlockContentRef<'a> {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let data = self.data.as_ptr() as *const c_void;
        let len = self.data.len();
        unsafe { MdbValue::new(data, len) }
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
    inner: &'a [u8],
    _encoding: PhantomData<E>,
}

impl<'a, E> ContentRef<'a, E> {
    pub fn new(inner: &'a [u8]) -> Self {
        Self {
            inner,
            _encoding: PhantomData::default(),
        }
    }
}

impl<'a, E: Encoding> ContentRef<'a, E> {
    pub fn value<T>(&self) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        E::deserialize(self.inner)
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
        E::fmt(self.inner, f)
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
    fn try_from_content(block: Block<'_>, content: BlockContentRef<'_>) -> crate::Result<Self>;
}

impl TryFromContent for lib0::Value {
    fn try_from_content(block: Block<'_>, content: BlockContentRef<'_>) -> crate::Result<Self> {
        if let Some(atom) = content.as_atom() {
            atom.value()
        } else if let Some(json) = content.as_json() {
            json.value()
        } else {
            Err(crate::Error::InvalidMapping("Value"))
        }
    }
}

impl TryFromContent for String {
    fn try_from_content(block: Block<'_>, content: BlockContentRef<'_>) -> crate::Result<Self> {
        let str = content
            .as_text()
            .ok_or(crate::Error::InvalidMapping("String"))?;
        Ok(str.into())
    }
}

impl<T> TryFromContent for Unmounted<T> {
    fn try_from_content(block: Block<'_>, _content: BlockContentRef<'_>) -> crate::Result<Self> {
        if block.is_deleted() {
            return Err(crate::Error::NotFound);
        } else if block.content_type() == ContentType::Node {
            Ok(Unmounted::nested(*block.id()))
        } else {
            Err(crate::Error::InvalidMapping("Unmounted"))
        }
    }
}
