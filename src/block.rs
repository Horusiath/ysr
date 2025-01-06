use crate::content::BlockContent;
use crate::node::NodeID;
use crate::{ClientID, Clock, U32};
use crate::{Error, Result};
use bytes::{BufMut, BytesMut};
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};
use zerocopy::{CastError, FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(C)]
#[derive(PartialEq, Eq, Copy, Clone, FromBytes, KnownLayout, Immutable, IntoBytes)]
pub struct ID {
    pub client: ClientID,
    pub clock: Clock,
}
impl Debug for ID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for ID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}:{}>", self.client, self.clock)
    }
}

#[repr(C)]
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
pub struct BlockHeader {
    clock_len: U32,
    flags: BlockFlags,
    content_type: u8,
    content_offset: u8,
    left: ID,
    right: ID,
    origin_left: ID,
    origin_right: ID,
    parent: NodeID,
}

impl BlockHeader {
    pub const SIZE: usize = size_of::<BlockHeader>();

    pub fn parse(data: &[u8]) -> Result<(&BlockHeader, &[u8]), ParseError> {
        let (header, body) = data.split_at(Self::SIZE);
        let header = Self::ref_from_bytes(header)?;
        Ok((header, body))
    }

    pub fn parse_mut(data: &mut [u8]) -> Result<(&mut BlockHeader, &mut [u8]), ParseMutError> {
        let (header, body) = data.split_at_mut(Self::SIZE);
        let header = Self::mut_from_bytes(header)?;
        Ok((header, body))
    }

    pub fn left(&self) -> Option<&ID> {
        if self.flags.has(BlockFlags::LEFT) {
            Some(&self.left)
        } else {
            None
        }
    }

    pub fn set_left(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                self.flags.set(BlockFlags::LEFT);
                self.left = *id;
            }
            None => {
                self.flags.clear(BlockFlags::LEFT);
            }
        }
    }

    pub fn right(&self) -> Option<&ID> {
        if self.flags.has(BlockFlags::RIGHT) {
            Some(&self.right)
        } else {
            None
        }
    }

    pub fn set_right(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                self.flags.set(BlockFlags::RIGHT);
                self.right = *id;
            }
            None => {
                self.flags.clear(BlockFlags::RIGHT);
            }
        }
    }

    pub fn origin_left(&self) -> Option<&ID> {
        if self.flags.has(BlockFlags::ORIGIN_LEFT) {
            Some(&self.origin_left)
        } else {
            None
        }
    }

    pub fn origin_right(&self) -> Option<&ID> {
        if self.flags.has(BlockFlags::ORIGIN_RIGHT) {
            Some(&self.origin_right)
        } else {
            None
        }
    }

    pub fn parent_sub<'a>(&self, body: &'a [u8]) -> Option<&'a [u8]> {
        let content_offset = self.content_offset as usize;
        if content_offset > 0 {
            Some(&body[..content_offset])
        } else {
            None
        }
    }

    pub fn content<'a>(&self, body: &'a [u8]) -> Result<BlockContent<'a>> {
        let content_offset = self.content_offset as usize;
        let content = &body[content_offset..];
        match self.content_type {
            CONTENT_TYPE_ATOM => Ok(BlockContent::Atom(content)),
            CONTENT_TYPE_BINARY => Ok(BlockContent::Binary(content)),
            CONTENT_TYPE_DELETED => {
                let len: [u8; 8] = content.try_into().map_err(|_| Error::EndOfBuffer)?;
                Ok(BlockContent::Deleted(u64::from_le_bytes(len)))
            }
            CONTENT_TYPE_DOC => Ok(BlockContent::Doc(content)),
            CONTENT_TYPE_EMBED => Ok(BlockContent::Embed(content)),
            CONTENT_TYPE_FORMAT => {
                let len = content[0] as usize;
                let content = &content[1..];
                let (format, content) = content.split_at(len);
                let format = unsafe { std::str::from_utf8_unchecked(format) };
                Ok(BlockContent::Format(format, content))
            }
            CONTENT_TYPE_NODE => {
                let node_id: [u8; 8] = content.try_into().map_err(|_| Error::EndOfBuffer)?;
                Ok(BlockContent::Node(node_id.into()))
            }
            CONTENT_TYPE_STRING => {
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                Ok(BlockContent::Text(str))
            }
            _ => Err(Error::UnsupportedContent(self.content_type)),
        }
    }

    pub fn set_content(&mut self, content: BlockContent, body: &mut BytesMut) {
        self.content_type = content.content_type();
        match content {
            BlockContent::Atom(content) => {
                body.extend_from_slice(content);
            }
            BlockContent::Binary(content) => {
                body.extend_from_slice(content);
            }
            BlockContent::Deleted(len) => {
                body.put_u64_le(len);
            }
            BlockContent::Doc(content) => {
                body.extend_from_slice(content);
            }
            BlockContent::Embed(content) => {
                body.extend_from_slice(content);
            }
            BlockContent::Format(format, content) => {
                body.reserve(format.len() + content.len() + 1);
                body.put_u8(format.len() as u8);
                body.extend_from_slice(format.as_bytes());
                body.extend_from_slice(content);
            }
            BlockContent::Node(node_id) => {
                body.put_u64_le(node_id.into());
            }
            BlockContent::Text(content) => {
                body.extend_from_slice(content.as_bytes());
            }
        }
    }
}

pub struct BlockMut {
    id: ID,
    body: BytesMut,
}

impl BlockMut {
    pub fn new(id: ID, body: BytesMut) -> Result<Self> {
        if let Err(_) = BlockHeader::parse(&*body) {
            Err(Error::MalformedBlock(id))
        } else {
            Ok(Self { id, body })
        }
    }

    pub fn parent_sub(&self) -> Option<&[u8]> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.parent_sub(body)
    }

    pub fn content(&self) -> Result<BlockContent> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.content(body)
    }
}

impl Deref for BlockMut {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        BlockHeader::ref_from_bytes(&self.body).unwrap()
    }
}

impl DerefMut for BlockMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        BlockHeader::mut_from_bytes(&mut self.body).unwrap()
    }
}

pub type ParseError<'a> = CastError<&'a [u8], BlockHeader>;
pub type ParseMutError<'a> = CastError<&'a mut [u8], BlockHeader>;

#[repr(transparent)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
pub struct BlockFlags(u8);

impl BlockFlags {
    /// Bit flag (1st bit) used for an item which should be kept - not used atm.
    const KEEP: u8 = 0b0000_0001;
    /// Bit flag (2nd bit) for an item, which contents are considered countable.
    const COUNTABLE: u8 = 0b0000_0010;
    /// Bit flag (3rd bit) for a tombstoned (deleted) item.
    const DELETED: u8 = 0b0000_0100;
    /// Bit flag (4th bit) for a marked item - not used atm.
    const MARKED: u8 = 0b0000_1000;
    /// Bit flag (5th bit) marking if block has defined right origin.
    const RIGHT: u8 = 0b0001_0000;
    /// Bit flag (6th bit) marking if block has defined right origin.
    const LEFT: u8 = 0b0010_0000;
    /// Bit flag (7th bit) marking if block has defined right origin.
    const ORIGIN_RIGHT: u8 = 0b0100_0000;
    /// Bit flag (8th bit) marking if block has defined right origin.
    const ORIGIN_LEFT: u8 = 0b1000_0000;

    #[inline]
    pub fn new(source: u8) -> Self {
        BlockFlags(source)
    }

    #[inline]
    fn has(&self, value: u8) -> bool {
        self.0 & value == value
    }

    #[inline]
    fn set(&mut self, value: u8) {
        self.0 |= value
    }

    #[inline]
    fn clear(&mut self, value: u8) {
        self.0 &= !value
    }
}

// Bit flag (9st bit) for item that is linked by Weak Link references
//const LINKED: u8 = 0b0001_0000_0000;

pub const CONTENT_TYPE_GC: u8 = 0;
pub const CONTENT_TYPE_DELETED: u8 = 1;
pub const CONTENT_TYPE_JSON: u8 = 2;
pub const CONTENT_TYPE_BINARY: u8 = 3;
pub const CONTENT_TYPE_STRING: u8 = 4;
pub const CONTENT_TYPE_EMBED: u8 = 5;
pub const CONTENT_TYPE_FORMAT: u8 = 6;
pub const CONTENT_TYPE_NODE: u8 = 7;
pub const CONTENT_TYPE_ATOM: u8 = 8;
pub const CONTENT_TYPE_DOC: u8 = 9;
pub const CONTENT_TYPE_SKIP: u8 = 10;
pub const CONTENT_TYPE_MOVE: u8 = 11;
