use crate::block::{
    CONTENT_TYPE_ATOM, CONTENT_TYPE_BINARY, CONTENT_TYPE_DELETED, CONTENT_TYPE_DOC,
    CONTENT_TYPE_EMBED, CONTENT_TYPE_FORMAT, CONTENT_TYPE_JSON, CONTENT_TYPE_NODE,
    CONTENT_TYPE_STRING,
};
use crate::lib0::{Decoder, Value, WriteExt};
use crate::node::{Named, Node, NodeID};
use crate::{Out, Unmounted, lib0};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
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
    pub(crate) data: Cow<'a, [u8]>,
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
        let atom = lib0::to_vec(data)?;
        Ok(Self::new(ContentType::Atom, Cow::Owned(atom)))
    }

    pub fn embed<T: Serialize>(data: &T) -> crate::Result<Self> {
        let atom = lib0::to_vec(data)?;
        Ok(Self::new(ContentType::Embed, Cow::Owned(atom)))
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

    pub fn string<S: Into<String>>(value: S) -> Self {
        Self::new(ContentType::String, Cow::Owned(value.into().into_bytes()))
    }
}

impl<'a> Content<'a> {
    pub fn new(content_type: ContentType, data: Cow<'a, [u8]>) -> Self {
        Self { content_type, data }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes().is_empty()
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    pub fn bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn to_owned(&self) -> Content<'static> {
        let owned: Cow<'static, [u8]> = Cow::Owned(self.data.to_vec());
        Content::new(self.content_type, owned)
    }

    pub fn binary<B: AsRef<[u8]>>(data: &'a B) -> Self {
        Self::new(ContentType::Binary, Cow::Borrowed(data.as_ref()))
    }

    pub fn str<S: AsRef<str>>(data: &'a S) -> Self {
        let str = data.as_ref();
        Self::new(ContentType::String, Cow::Borrowed(str.as_bytes()))
    }

    pub fn node(node_id: &'a NodeID) -> Self {
        let bytes = node_id.as_bytes();
        Self::new(ContentType::Node, Cow::Borrowed(bytes))
    }

    pub fn doc(doc_id: &'a str) -> Self {
        Self::new(ContentType::Doc, Cow::Borrowed(doc_id.as_bytes()))
    }

    pub fn as_json<T>(&self) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        if self.content_type != ContentType::Json {
            return Err(crate::Error::InvalidMapping("json"));
        }
        let json: T = serde_json::from_slice(self.data.as_ref())?;
        Ok(json)
    }

    pub fn as_atom<T>(&self) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        if self.content_type != ContentType::Atom {
            return Err(crate::Error::InvalidMapping("atom"));
        }
        let atom: T = lib0::from_slice(self.data.as_ref())?;
        Ok(atom)
    }

    pub fn as_embed<T>(&self) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        if self.content_type != ContentType::Embed {
            return Err(crate::Error::InvalidMapping("embed"));
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
        let node_id = NodeID::ref_from_bytes(self.data.as_ref())
            .map_err(|_| crate::Error::InvalidMapping("NodeID"))?;
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

    pub fn as_format(&self) -> crate::Result<FormatAttribute<'_>> {
        if self.content_type != ContentType::Format {
            return Err(crate::Error::InvalidMapping("format attribute"));
        }
        match FormatAttribute::new(self.data.as_ref()) {
            Some(attr) => Ok(attr),
            None => Err(crate::Error::InvalidMapping("format attribute")),
        }
    }

    pub fn split<'b>(&'b self, utf16_offset: usize) -> Option<(Content<'b>, Content<'b>)> {
        if self.content_type != ContentType::String {
            return None; // only strings can be split. JSON and atoms are multipart.
        }

        let str: &str = unsafe { std::str::from_utf8_unchecked(self.data.as_ref()) };
        let offset = utf16_to_utf8(str, utf16_offset)?;
        let (left, right) = self.data.split_at(offset);
        let left: Content<'b> = Content::new(self.content_type, left.into());
        let right: Content<'b> = Content::new(self.content_type, right.into());
        Some((left, right))
    }
}

/// Convert a UTF-16 code-unit offset within `str` into a UTF-8 byte offset.
/// Returns `None` if the offset is not at a valid UTF-16 boundary (e.g. it would split a
/// surrogate pair) or if it lies past the end of the string.
pub(crate) fn utf16_to_utf8(str: &str, utf16: usize) -> Option<usize> {
    let mut utf16_count = 0;
    for (byte_offset, ch) in str.char_indices() {
        if utf16_count == utf16 {
            return Some(byte_offset);
        }
        if utf16_count > utf16 {
            // We overshot, meaning `utf16` lands in the middle of a surrogate pair.
            return None;
        }
        utf16_count += ch.len_utf16();
    }
    if utf16_count == utf16 {
        Some(str.len())
    } else {
        None
    }
}

/// Count the number of UTF-16 code units needed to represent valid UTF-8 bytes.
/// Equivalent to `str::encode_utf16().count()` but avoids iterator overhead
/// by inspecting only the leading bytes of each character.
pub(crate) fn utf8_to_utf16_len(bytes: &[u8]) -> u32 {
    let mut len: u32 = 0;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b < 0x80 {
            len += 1;
            i += 1;
        } else if b < 0xE0 {
            len += 1;
            i += 2;
        } else if b < 0xF0 {
            len += 1;
            i += 3;
        } else {
            len += 2;
            i += 4;
        }
    }
    len
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

impl<'a> TryFrom<Content<'a>> for Value {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Atom => Ok(lib0::from_slice(value.bytes())?),
            _ => Err(crate::Error::InvalidMapping("atom")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for serde_json::Value {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Json => Ok(serde_json::from_slice(value.bytes())?),
            _ => Err(crate::Error::InvalidMapping("json")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for String {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::String => {
                let str = String::from_utf8(value.data.to_vec())
                    .map_err(|_| crate::Error::InvalidMapping("string"))?;
                Ok(str)
            }
            _ => Err(crate::Error::InvalidMapping("string")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for Vec<u8> {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Binary => Ok(value.data.to_vec()),
            _ => Err(crate::Error::InvalidMapping("binary")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for NodeID {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Node => {
                let node_id = *NodeID::ref_from_bytes(value.data.as_ref())
                    .map_err(|_| crate::Error::InvalidMapping("NodeID"))?;
                Ok(node_id)
            }
            _ => Err(crate::Error::InvalidMapping("node")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for Node<'static> {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        let node_id = NodeID::try_from(value)?;
        Ok(if node_id.is_root() {
            Node::Root(Named::Hash(node_id))
        } else {
            Node::Nested(node_id)
        })
    }
}

impl<'a, Cap> TryFrom<Content<'a>> for Unmounted<Cap> {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        let node = Node::try_from(value)?;
        Ok(Unmounted::new(node))
    }
}

impl<'a> TryFrom<Content<'a>> for crate::Out {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Json => {
                let value: lib0::Value = serde_json::from_slice(value.data.as_ref())?;
                Ok(Out::Value(value))
            }
            ContentType::Atom => {
                let value: lib0::Value = lib0::from_slice(value.data.as_ref())?;
                Ok(Out::Value(value))
            }
            ContentType::Node => Ok(Out::Node(NodeID::try_from(value)?)),
            ContentType::String => {
                let str = String::from_utf8(value.data.to_vec())
                    .map_err(|_| crate::Error::InvalidMapping("string"))?;
                Ok(Out::Value(Value::String(str)))
            }
            ContentType::Binary => match value.data {
                Cow::Borrowed(bytes) => Ok(Out::Value(Value::Bytes(Bytes::copy_from_slice(bytes)))),
                Cow::Owned(bytes) => Ok(Out::Value(Value::Bytes(bytes.into()))),
            },
            _ => Err(crate::Error::InvalidMapping("Out")),
        }
    }
}

impl<'a> TryFrom<Content<'a>> for FormatAttribute<'a> {
    type Error = crate::Error;

    fn try_from(value: Content<'a>) -> Result<Self, Self::Error> {
        match value.content_type() {
            ContentType::Format => Ok(FormatAttribute { data: value.data }),
            _ => Err(crate::Error::InvalidMapping("format attribute")),
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FormatAttribute<'a> {
    data: Cow<'a, [u8]>,
}

impl FormatAttribute<'static> {
    pub fn decode<D: Decoder>(decoder: &mut D) -> crate::Result<Vec<u8>> {
        let mut buf = vec![0u8];
        decoder.read_key(&mut buf)?;
        if buf.len() >= u8::MAX as usize {
            return Err(crate::Error::KeyTooLong);
        }
        buf[0] = (buf.len() - 1) as u8;
        let value: lib0::Value = decoder.read_json()?;
        lib0::to_writer(&mut buf, &value)?;
        Ok(buf)
    }

    pub fn compose<T: Serialize>(key: &str, value: &T) -> crate::Result<Vec<u8>> {
        if key.len() >= u8::MAX as usize {
            return Err(crate::Error::KeyTooLong);
        }

        let mut buf = Vec::with_capacity(key.len() + 8);
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
        Some(Self {
            data: Cow::Borrowed(data),
        })
    }

    pub fn key(&self) -> &str {
        let len = self.data[0] as usize;
        let key: &[u8] = &self.data[1..(len + 1)];
        unsafe { std::str::from_utf8_unchecked(key) }
    }

    pub fn value<T: DeserializeOwned>(&self) -> crate::Result<T> {
        let len = self.data[0] as usize;
        let data = &self.data[(len + 1)..];
        let value = lib0::from_slice::<T>(data)?;
        Ok(value)
    }
}

impl<'a> Display for FormatAttribute<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let key = self.key();
        let value: lib0::Value = self.value().map_err(|_| std::fmt::Error)?;
        write!(f, "\"{}\"={}", key, value)
    }
}
