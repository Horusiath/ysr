use crate::block::{
    CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::node::NodeID;
use crate::store::lmdb::store::SplitResult;
use crate::write::WriteExt;
use crate::{Clock, lib0};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub enum ContentType {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Content<'a> {
    content_type: ContentType,
    data: Cow<'a, [u8]>,
}

impl Content<'static> {
    pub const DELETED: Self = Content {
        content_type: ContentType::Deleted,
        data: Cow::Borrowed(&[]),
    };

    pub fn json<T: Serialize>(data: &T) -> crate::Result<Self> {
        let json = serde_json::to_vec(data)?;
        Ok(Self::new(ContentType::Json, Cow::Owned(json)))
    }

    pub fn atom<T: Serialize>(data: &T) -> crate::Result<Self> {
        let json = lib0::to_vec(data)?;
        Ok(Self::new(ContentType::Atom, Cow::Owned(json)))
    }

    pub fn multipart<T: Serialize>(data: &[T]) -> crate::Result<Vec<Self>> {
        let mut buf = Vec::with_capacity(data.len());
        for value in data {
            buf.push(Self::atom(value)?);
        }
        Ok(buf)
    }

    pub fn format<'a, T: Serialize>(key: &'a str, value: &'a T) -> crate::Result<Self> {
        let attr = FormatAttribute::compose(key, value)?;
        Ok(Self::new(ContentType::Format, Cow::Owned(attr)))
    }
}

impl<'a> Content<'a> {
    pub fn new(content_type: ContentType, data: Cow<'a, [u8]>) -> Self {
        Self { content_type, data }
    }

    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.data.as_ref()
    }

    pub fn to_owned(&self) -> Content<'static> {
        Self::new(self.content_type, self.data.to_owned())
    }

    pub fn binary<B: AsRef<[u8]>>(data: &'a B) -> Self {
        Self::new(ContentType::Binary, Cow::Borrowed(data.as_ref()))
    }

    pub fn str<S: AsRef<str>>(data: &'a S) -> Self {
        let str = data.as_ref();
        Self::new(ContentType::Binary, Cow::Borrowed(str.as_bytes()))
    }

    pub fn node(node_id: &'a NodeID) -> Self {
        let bytes = node_id.as_bytes();
        Self::new(ContentType::Node, Cow::Borrowed(bytes))
    }

    pub fn doc(doc_id: &'a str) -> Self {
        Self::new(ContentType::Doc, Cow::Borrowed(doc_id.as_bytes()))
    }

    pub fn as_json<T: Deserialize<'a>>(&self) -> crate::Result<T> {
        if self.content_type != ContentType::Json {
            return Err(crate::Error::InvalidMapping("json"));
        }
        let json: T = serde_json::from_slice(self.data.as_ref())?;
        Ok(json)
    }

    pub fn as_atom<T: Deserialize<'a>>(&self) -> crate::Result<T> {
        if self.content_type != ContentType::Json {
            return Err(crate::Error::InvalidMapping("atom"));
        }
        let atom: T = lib0::from_slice(self.data.as_ref())?;
        Ok(atom)
    }

    pub fn as_str(&self) -> crate::Result<&str> {
        if self.content_type != ContentType::String {
            return Err(crate::Error::InvalidMapping("string"));
        }
        match std::str::from_utf8(self.data.as_ref()) {
            Ok(str) => Ok(str),
            Err(_) => Err(crate::Error::InvalidMapping("string")),
        }
    }

    pub fn as_binary(&self) -> crate::Result<&[u8]> {
        if self.content_type != ContentType::Binary {
            return Err(crate::Error::InvalidMapping("binary"));
        }
        Ok(self.data.as_ref())
    }

    pub fn as_node(&self) -> crate::Result<&NodeID> {
        if self.content_type != ContentType::Node {
            return Err(crate::Error::InvalidMapping("node"));
        }
        let node_id = NodeID::ref_from_bytes(self.data.as_ref())?;
        Ok(node_id)
    }

    pub fn as_doc(&self) -> crate::Result<&str> {
        if self.content_type != ContentType::Doc {
            return Err(crate::Error::InvalidMapping("document id"));
        }
        match std::str::from_utf8(self.data.as_ref()) {
            Ok(str) => Ok(str),
            Err(_) => Err(crate::Error::InvalidMapping("document id")),
        }
    }

    pub fn as_format(&self) -> crate::Result<FormatAttribute<'a>> {
        if self.content_type != ContentType::Json {
            return Err(crate::Error::InvalidMapping("format attribute"));
        }
        match FormatAttribute::new(self.data.as_ref()) {
            Some(attr) => Ok(attr),
            None => Err(crate::Error::InvalidMapping("format attribute")),
        }
    }

    pub fn split(&self, utf16_offset: usize) -> Option<(Self, Self)> {
        /// Map offset given as UTF16 code points to byte offset in UTF8 encoded string.
        fn map_offset(str: &str, utf16: usize) -> Option<usize> {
            let mut offset = 0;
            for ch in str.encode_utf16() {
                if offset == utf16 {
                    break;
                }
                offset += 1;
            }

            if offset == utf16 { Some(offset) } else { None }
        }

        if self.content_type != ContentType::String {
            return None; // only strings can be split. JSON and atoms are multipart.
        }

        let str: &'a str = unsafe { std::str::from_utf8_unchecked(&self.data) };
        let offset = map_offset(str, utf16_offset)?;
        let (left, right) = self.data.split_at(offset);
        let left = Self::new(self.content_type, left.into());
        let right = Self::new(self.content_type, right.into());
        Some((left, right))
    }
}

impl<'a> Display for Content<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.content_type {
            ContentType::Deleted => write!(f, "deleted"),
            ContentType::Json => {
                let json: serde_json::Value =
                    serde_json::from_slice(&self.data).map_err(|_| std::fmt::Error)?;
                write!(f, "{}", json)
            }
            ContentType::Binary => {
                for byte in self.data.iter() {
                    write!(f, "{:02x}", byte)?;
                }
                Ok(())
            }
            ContentType::String => {
                let str = std::str::from_utf8(&self.data).map_err(|_| std::fmt::Error)?;
                write!(f, "{}", str)
            }
            ContentType::Embed => {
                write!(f, "embed")
            }
            ContentType::Format => {
                let attr = FormatAttribute::new(&self.data).ok_or(std::fmt::Error)?;
                write!(f, "{}", attr)
            }
            ContentType::Node => {
                let node_id = NodeID::ref_from_bytes(&self.data).map_err(|_| std::fmt::Error)?;
                write!(f, "{}", node_id)
            }
            ContentType::Atom => {
                let atom: lib0::Value =
                    lib0::from_slice(&self.data).map_err(|_| std::fmt::Error)?;
                write!(f, "{}", atom)
            }
            ContentType::Doc => {
                let doc_id = std::str::from_utf8(&self.data).map_err(|_| std::fmt::Error)?;
                write!(f, "{}", doc_id)
            }
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormatAttribute<'a> {
    data: &'a [u8],
}

impl FormatAttribute<'static> {
    pub fn compose<T: Serialize>(key: &str, value: &T) -> crate::Result<Vec<u8>> {
        if key.len() >= u8::MAX as usize {
            return Err(crate::Error::KeyTooLong);
        }

        let mut buf = Vec::with_capacity(key.len() + 1);
        buf.write_u8(key.len() as u8)?;
        buf.extend_from_slice(key.as_bytes());
        lib0::to_writer(&mut buf, value)?;
        Ok(buf)
    }
}

impl<'a> FormatAttribute<'a> {
    pub fn new(data: &'a [u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let len = data[0] as usize;
        if data.len() < len + 1 {
            return None;
        }
        Some(Self { data })
    }

    pub fn key(&self) -> &'a str {
        let len = self.data[0] as usize;
        let key: &'a [u8] = &self.data[1..(len + 1)];
        unsafe { std::str::from_utf8_unchecked(key) }
    }

    pub fn value(&self) -> &'a [u8] {
        let len = self.data[0] as usize;
        &self.data[(len + 1)..]
    }
}

impl<'a> Display for FormatAttribute<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let key = self.key();
        let value: lib0::Value = match lib0::from_slice(self.value()) {
            Ok(v) => v,
            Err(_) => return Err(std::fmt::Error),
        };
        write!(f, "\"{}\"={}", key, value)
    }
}

/*
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub struct InlineContent {
    pub len: u8,
    pub content_type: ContentType,
    pub data: [u8; 8],
}

impl InlineContent {
    pub fn new(content_type: ContentType) -> Self {
        InlineContent {
            len: 0,
            content_type,
            data: [0; 8],
        }
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }

    pub fn copy_from(&mut self, content: BlockContentRef) -> bool {
        if content.data.len() > 9 {
            return false;
        }
        let data = &content.data[1..];
        self.len = data.len() as u8;
        self.data[0..data.len()].copy_from_slice(data);
        let content_type = ContentType::try_from(content.data[0]).unwrap();
        self.content_type = content_type;
        true
    }

    pub fn set_data(&mut self, data: &[u8]) -> Result<(), crate::Error> {
        if data.len() > 8 {
            return Err(crate::Error::ValueTooLarge);
        }
        self.data[..data.len()].copy_from_slice(data);
        Ok(())
    }

    pub fn as_ref(&self) -> BlockContentRef<'_> {
        let data = self.data();
        BlockContentRef::new(self.content_type, data)
    }
}
pub type InlineBytes = SmallVec<[u8; 16]>;

#[derive(Clone, PartialEq)]
pub struct BlockContent {
    /// A list of buffers to be written as data content.
    /// - For most cases it's a single buffer with first byte marking [ContentType] and remaining
    ///   bytes having a content bytes.
    /// - In some cases a single block can contain multiple elements i.e. in
    ///   [ContentType::Atom]/[ContentType::Json] cases. In such case this field may contain more
    ///   than one element. Each element starting with [ContentType] followed by content data.
    /// - In case when content is empty it's just a single byte marking the [ContentType].
    data: SmallVec<[InlineBytes; 1]>,
}

impl BlockContent {
    pub fn multipart(data: SmallVec<[InlineBytes; 1]>) -> Self {
        Self { data }
    }

    pub fn new(content_type: ContentType, data: &[u8]) -> Self {
        let mut buf = SmallVec::with_capacity(data.len() + 1);
        buf.push(content_type as u8);
        buf.extend_from_slice(data);
        Self {
            data: smallvec![buf],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn items(&self) -> &[InlineBytes] {
        &self.data
    }

    pub fn deleted() -> Self {
        BlockContentRef::DELETED.to_owned()
    }

    pub fn node(node_id: &NodeID) -> Self {
        Self::new(ContentType::Node, node_id.as_bytes())
    }

    pub fn binary<A: AsRef<[u8]>>(value: A) -> Self {
        Self::new(ContentType::Binary, value.as_ref())
    }

    pub fn embed<A: AsRef<[u8]>>(value: A) -> Self {
        Self::new(ContentType::Embed, value.as_ref())
    }

    pub fn string<S: AsRef<str>>(value: S) -> Self {
        Self::new(ContentType::String, value.as_ref().as_bytes())
    }

    pub fn atom<'a, I, S>(atoms: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = &'a S>,
        S: Serialize + 'a,
    {
        let mut data = SmallVec::new();
        for value in atoms {
            let mut buf = smallvec![ContentType::Atom as u8];
            lib0::to_writer(&mut buf, value)?;
            data.push(buf);
        }

        Ok(Self { data })
    }

    pub fn json<'a, I, S>(atoms: I) -> crate::Result<Self>
    where
        I: IntoIterator<Item = &'a S>,
        S: Serialize + 'a,
    {
        let mut data = SmallVec::new();
        for value in atoms {
            let mut buf = smallvec![ContentType::Atom as u8];
            serde_json::to_writer(&mut buf, value)?;
            data.push(buf);
        }

        Ok(Self { data })
    }

    pub fn format<K, V>(key: K, value: V) -> Self
    where
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();
        let mut content = SmallVec::with_capacity(key.len() + value.len() + 3);
        content.write_var(key.len()).unwrap();
        content.write_string(key).unwrap();
        content.write_var(value.len()).unwrap();
        content.write_all(value).unwrap();
        Self {
            data: smallvec![content],
        }
    }

    pub fn as_ref(&self) -> BlockContentRef<'_> {
        let head = &self.data[0];
        let content_type = ContentType::try_from(head[0]).unwrap();
        BlockContentRef {
            content_type,
            data: &head[1..],
        }
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        let head = &self.data[0];
        ContentType::try_from(head[0]).unwrap()
    }

    #[inline]
    pub fn body(&self) -> Option<&[u8]> {
        if self.data.len() == 1 {
            Some(&self.data[0][1..])
        } else {
            None
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        if self.content_type() == ContentType::String {
            let body = self.body()?;
            Some(unsafe { std::str::from_utf8_unchecked(body) })
        } else {
            None
        }
    }

    pub fn as_format(&self) -> Option<ContentFormat<'_>> {
        if self.content_type() == ContentType::Format {
            let body = self.body()?;
            ContentFormat::new(body).ok()
        } else {
            None
        }
    }

    pub fn as_json(&self) -> ContentIter<'_, JsonEncoding> {
        if self.content_type() == ContentType::Json {
            ContentIter::new(&self.data)
        } else {
            ContentIter::new(&[])
        }
    }

    pub fn as_atom(&self) -> ContentIter<'_, AtomEncoding> {
        if self.content_type() == ContentType::Json {
            ContentIter::new(&self.data)
        } else {
            ContentIter::new(&[])
        }
    }

    pub fn merge(&mut self, other: BlockContent) -> bool {
        if self.content_type() != other.content_type() {
            return false;
        }
        match other.content_type() {
            ContentType::Atom | ContentType::Json => {
                self.data.extend(other.data);
            }
            ContentType::String => self.data[0].extend_from_slice(&other.data[0][1..]),
            _ => { /* not used */ }
        }
        true
    }

    pub fn split(&mut self, mut offset: usize) -> Option<BlockContent> {
        match self.content_type() {
            ContentType::String => {
                let content = &self.data[0][1..];
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                let mut byte_offset = 0;
                for c in str.chars() {
                    if offset == 0 {
                        break;
                    }
                    offset -= 1;
                    let utf8_len = c.len_utf8();
                    byte_offset += utf8_len;
                }
                let new_content = BlockContent::string(&str[byte_offset..]);
                self.data[0].drain((1 + byte_offset)..);
                Some(new_content)
            }
            ContentType::Atom | ContentType::Json => {
                let data = self.data.drain(offset..).collect();
                Some(BlockContent { data })
            }
            _ => None,
        }
    }

    #[inline]
    pub fn head_mut(&mut self) -> &mut InlineBytes {
        &mut self.data[0]
    }
}

impl Default for BlockContent {
    fn default() -> Self {
        Self {
            data: SmallVec::new(),
        }
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

pub struct BlockWriter {
    buf: InlineBytes,
}

impl BlockWriter {
    pub fn new(content_type: ContentType) -> Self {
        BlockWriter {
            buf: smallvec![content_type as u8],
        }
    }

    #[inline]
    pub fn into_inner(self) -> InlineBytes {
        self.buf
    }
}

impl std::io::Write for BlockWriter {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        self.buf.flush()
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.buf.write_all(buf)
    }
}

impl From<BlockWriter> for BlockContent {
    fn from(value: BlockWriter) -> Self {
        BlockContent {
            data: smallvec![value.buf],
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BlockContentRef<'a> {
    content_type: ContentType,
    data: &'a [u8],
}

impl BlockContentRef<'static> {
    pub const DELETED: Self = BlockContentRef {
        content_type: ContentType::Deleted,
        data: &[],
    };
    pub const NODE: Self = BlockContentRef {
        content_type: ContentType::Node,
        data: &[],
    };
}

impl<'a> BlockContentRef<'a> {
    pub fn from_slice(data: &'a [u8]) -> crate::Result<Self> {
        let content_type = ContentType::try_from(data[0])?;
        Ok(BlockContentRef {
            content_type,
            data: &data[1..],
        })
    }

    pub fn new(content_type: ContentType, data: &'a [u8]) -> Self {
        BlockContentRef { content_type, data }
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    #[inline]
    pub fn body(&self) -> &'a [u8] {
        &self.data
    }

    pub fn to_owned(self) -> BlockContent {
        let mut buf = SmallVec::with_capacity(1 + self.data.len());
        buf.push(self.content_type as u8);
        buf.extend_from_slice(self.data);
        BlockContent {
            data: smallvec![buf],
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

    #[inline]
    pub fn can_inline(&self) -> bool {
        self.data.len() <= 8
    }

    pub fn split(&self, offset: usize) -> (BlockContentRef<'a>, BlockContentRef<'a>) {
        let content_type = self.content_type();
        let (left, right) = self.data.split_at(offset);
        (
            BlockContentRef::new(content_type, left),
            BlockContentRef::new(content_type, right),
        )
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

pub struct ContentIter<'a, E> {
    data: &'a [InlineBytes],
    encoding: PhantomData<E>,
}

impl<'a, E> ContentIter<'a, E> {
    pub fn new(data: &'a [InlineBytes]) -> Self {
        Self {
            data,
            encoding: PhantomData,
        }
    }
}

impl<'a, E> Iterator for ContentIter<'a, E> {
    type Item = ContentRef<'a, E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            None
        } else {
            let elem = ContentRef::new(&self.data[0][1..]);
            self.data = &self.data[1..];
            Some(elem)
        }
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

#[cfg(test)]
mod test {
    use crate::content::{BlockContent, ContentType, InlineContent};

    #[test]
    fn inline_content_to_block_content() {
        let content = BlockContent::string("hello");
        let mut inline_content = InlineContent::new(ContentType::Deleted);
        assert!(inline_content.copy_from(content.as_ref()));
        let content2 = inline_content.as_ref();

        assert_eq!(content.as_ref(), content2);
    }
}
*/
