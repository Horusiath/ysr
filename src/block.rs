use crate::content::{BlockContent, ContentAtom, ContentFormat, ContentJson, ContentType};
use crate::node::{NodeHeader, NodeID};
use crate::{ClientID, Clock};
use crate::{Error, Result};
use bitflags::bitflags;
use bytes::BytesMut;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::ops::{Deref, DerefMut};
use zerocopy::{CastError, FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(PartialEq, Eq, Copy, Clone, FromBytes, KnownLayout, Immutable, IntoBytes, Default)]
pub struct ID {
    pub client: ClientID,
    pub clock: Clock,
}

impl ID {
    #[inline]
    pub fn new(client: ClientID, clock: Clock) -> Self {
        Self { client, clock }
    }
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
#[derive(FromBytes, KnownLayout, Immutable, IntoBytes, Default)]
pub struct BlockHeader {
    /// Yjs-compatible length of the block. Counted as a number of countable elements or
    /// UTF-16 characters.
    clock_len: Clock,
    /// Flags that define the block's state, including presence/absence of other fields like
    /// neighbor blocks or origins.
    flags: BlockFlags,
    /// Flags that define the block's content type.
    content_type: u8,
    /// ID of the left neighbor block (if such exists).
    left: ID,
    /// ID of the right neighbor block (if such exists).
    right: ID,
    /// NodeID of the parent node collection that contains this block.
    parent: NodeID,
    /// Length of the key in bytes. Key must be a non-empty string.
    key_len: u8,
    /// Version of the block header in use.
    version: u8,
    /// ID of the left neighbor block at the point of insertion (if such existed).
    origin_left: ID,
    /// ID of the right neighbor block at the point of insertion (if such existed).
    origin_right: ID,
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
        if self.flags.contains(BlockFlags::LEFT) {
            Some(&self.left)
        } else {
            None
        }
    }

    pub fn set_left(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                self.flags |= BlockFlags::LEFT;
                self.left = *id;
            }
            None => {
                self.flags -= BlockFlags::LEFT;
            }
        }
    }

    pub fn right(&self) -> Option<&ID> {
        if self.flags.contains(BlockFlags::RIGHT) {
            Some(&self.right)
        } else {
            None
        }
    }

    pub fn set_right(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                self.flags |= BlockFlags::RIGHT;
                self.right = *id;
            }
            None => {
                self.flags -= BlockFlags::RIGHT;
            }
        }
    }

    pub fn origin_left(&self) -> Option<&ID> {
        if self.flags.contains(BlockFlags::ORIGIN_LEFT) {
            Some(&self.origin_left)
        } else {
            None
        }
    }

    pub fn set_origin_left(&mut self, id: ID) {
        self.origin_left = id;
        self.flags |= BlockFlags::ORIGIN_LEFT;
    }

    pub fn origin_right(&self) -> Option<&ID> {
        if self.flags.contains(BlockFlags::ORIGIN_RIGHT) {
            Some(&self.origin_right)
        } else {
            None
        }
    }

    pub fn set_origin_right(&mut self, id: ID) {
        self.origin_right = id;
        self.flags |= BlockFlags::ORIGIN_RIGHT;
    }

    pub fn entry_key<'a>(&self, body: &'a [u8]) -> Option<&'a str> {
        if self.key_len == 0 {
            None
        } else {
            let key_offset = body.len() - self.key_len as usize;
            let str = unsafe { std::str::from_utf8_unchecked(&body[key_offset..]) };
            Some(str)
        }
    }

    #[inline]
    pub fn set_parent(&mut self, parent_id: NodeID) {
        self.parent = parent_id;
    }

    pub fn content<'a>(&self, body: &'a [u8]) -> Result<BlockContent<'a>> {
        let content_end = body.len() - self.key_len as usize;
        let content = &body[..content_end];
        match self.content_type.try_into()? {
            ContentType::Deleted => Ok(BlockContent::Deleted(self.clock_len)),
            ContentType::Json => Ok(BlockContent::Json(ContentJson::new(content))),
            ContentType::Atom => Ok(BlockContent::Atom(ContentAtom::new(content))),
            ContentType::Binary => Ok(BlockContent::Binary(content)),
            ContentType::Doc => Ok(BlockContent::Doc(content)),
            ContentType::Embed => Ok(BlockContent::Embed(content)),
            ContentType::Format => Ok(BlockContent::Format(ContentFormat::new(content)?)),
            ContentType::Node => {
                let node: &NodeHeader = NodeHeader::ref_from_bytes(content)
                    .map_err(|_| crate::Error::InvalidMapping("NodeHeader"))?;
                Ok(BlockContent::Node(node))
            }
            ContentType::String => {
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                Ok(BlockContent::Text(str))
            }
        }
    }

    pub fn set_content_type(&mut self, content_type: u8) {
        self.content_type = content_type;
    }

    pub fn clock_len(&self) -> Clock {
        self.clock_len
    }

    pub fn set_clock_len(&mut self, len: Clock) {
        self.clock_len = len;
    }

    pub fn display<'a>(&'a self, body: &'a [u8]) -> DisplayBlock<'a> {
        DisplayBlock { header: self, body }
    }
}

pub struct Block<'a> {
    id: ID,
    data: &'a [u8],
}

impl<'a> Block<'a> {
    pub fn new(id: ID, data: &'a [u8]) -> Result<Self> {
        if BlockHeader::parse(data).is_err() {
            Err(Error::MalformedBlock(id))
        } else {
            Ok(Self { id, data })
        }
    }

    pub unsafe fn new_unchecked(id: ID, data: &'a [u8]) -> Self {
        Self { id, data }
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn last_id(&self) -> ID {
        ID::new(self.id.client, self.id.clock + self.clock_len())
    }

    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.id.client // same client
            && id.clock >= self.id.clock // id is larger or equal to block's start clock
            && id.clock < self.id.clock + self.clock_len() // id is smaller than block's end clock
    }

    #[inline]
    pub fn header(&self) -> &BlockHeader {
        BlockHeader::ref_from_bytes(self.data).unwrap()
    }

    pub fn entry_key(&self) -> Option<&str> {
        let (header, body) = BlockHeader::parse(&self.data).unwrap();
        header.entry_key(body)
    }

    pub fn content(&self) -> Result<BlockContent<'a>> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.content(body)
    }

    pub fn display(&self) -> DisplayBlock<'a> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.display(body)
    }

    pub fn bytes(&self) -> &[u8] {
        self.data
    }

    pub fn to_owned(&self) -> BlockMut {
        let mut body = BytesMut::with_capacity(self.data.len());
        body.extend_from_slice(self.data);
        BlockMut::parse(self.id, body).unwrap()
    }
}

impl Deref for Block<'_> {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

pub struct BlockMut {
    id: ID,
    body: BytesMut,
}

impl BlockMut {
    pub fn new(id: ID) -> Self {
        let header = BlockHeader::default();
        let mut bytes = BytesMut::with_capacity(BlockHeader::SIZE);
        bytes.extend_from_slice(header.as_bytes());
        BlockMut { id, body: bytes }
    }

    pub fn parse(id: ID, body: BytesMut) -> Result<Self> {
        if let Err(_) = BlockHeader::parse(&*body) {
            Err(Error::MalformedBlock(id))
        } else {
            Ok(Self { id, body })
        }
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn entry_key(&self) -> Option<&str> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.entry_key(body)
    }

    pub fn content(&self) -> Result<BlockContent> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.content(body)
    }

    pub(crate) fn clock_len(&self) -> Clock {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.clock_len
    }

    pub(crate) fn init_entry_key<S: AsRef<[u8]>>(&mut self, key: S) -> crate::Result<()> {
        let key = key.as_ref();
        let key_len = key.len();
        if key_len > u8::MAX as usize {
            return Err(Error::KeyTooLong);
        }
        self.key_len = key_len as u8;
        self.body.extend_from_slice(key);
        Ok(())
    }

    pub fn as_writer(&mut self) -> Writer<'_> {
        Writer::new(self)
    }

    pub fn as_ref(&self) -> Block<'_> {
        unsafe { Block::new_unchecked(self.id, &self.body) }
    }

    pub fn from_block(block: &Block<'_>) -> Self {
        let mut body = BytesMut::with_capacity(block.data.len());
        body.extend_from_slice(block.data);
        Self::parse(*block.id(), body).unwrap()
    }

    pub fn split_at(&mut self, id: &ID) -> Option<BlockMut> {
        todo!()
    }
}

impl Deref for BlockMut {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        BlockHeader::ref_from_bytes(&self.body[..BlockHeader::SIZE]).unwrap()
    }
}

impl DerefMut for BlockMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        BlockHeader::mut_from_bytes(&mut self.body).unwrap()
    }
}

impl Display for BlockMut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        write!(f, "{}, {}", self.id, header.display(body))
    }
}

pub struct Writer<'a> {
    block: &'a mut BlockMut,
}

impl<'a> Writer<'a> {
    fn new(block: &'a mut BlockMut) -> Self {
        Self { block }
    }
}

impl<'a> Write for Writer<'a> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.block.body.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.block.body.extend_from_slice(buf);
        Ok(())
    }
}

pub type ParseError<'a> = CastError<&'a [u8], BlockHeader>;
pub type ParseMutError<'a> = CastError<&'a mut [u8], BlockHeader>;

#[repr(transparent)]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
pub struct BlockFlags(u8);

bitflags! {
    impl BlockFlags : u8 {
        /// Bit flag (1st bit) used for an item which should be kept - not used atm.
        const KEEP = 0b0000_0001;
        /// Bit flag (2nd bit) for an item, which contents are considered countable.
        const COUNTABLE = 0b0000_0010;
        /// Bit flag (3rd bit) for a tombstoned (deleted) item.
        const DELETED = 0b0000_0100;
        /// Bit flag (4th bit) for a marked item - not used atm.
        const MARKED = 0b0000_1000;
        /// Bit flag (5th bit) marking if block has defined right origin.
        const RIGHT = 0b0001_0000;
        /// Bit flag (6th bit) marking if block has defined right origin.
        const LEFT = 0b0010_0000;
        /// Bit flag (7th bit) marking if block has defined right origin.
        const ORIGIN_RIGHT = 0b0100_0000;
        /// Bit flag (8th bit) marking if block has defined right origin.
        const ORIGIN_LEFT = 0b1000_0000;
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

pub struct DisplayBlock<'a> {
    header: &'a BlockHeader,
    body: &'a [u8],
}

impl<'a> Display for DisplayBlock<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "parent: {}", self.header.parent)?;
        if let Some(key) = self.header.entry_key(self.body) {
            write!(f, ", key: \"{}\"", key)?;
        }
        if self.header.flags.contains(BlockFlags::COUNTABLE) {
            write!(f, ", clock-len: {}", self.header.clock_len)?;
        }
        if self.header.flags.contains(BlockFlags::LEFT) {
            write!(f, ", left: {}", self.header.left)?;
        }
        if self.header.flags.contains(BlockFlags::RIGHT) {
            write!(f, ", right: {}", self.header.right)?;
        }
        if self.header.flags.contains(BlockFlags::ORIGIN_LEFT) {
            write!(f, ", origin-l: {}", self.header.origin_left)?;
        }
        if self.header.flags.contains(BlockFlags::ORIGIN_RIGHT) {
            write!(f, ", origin-r: {}", self.header.origin_right)?;
        }
        let deleted = self.header.flags.contains(BlockFlags::DELETED);
        if deleted {
            write!(f, " ~~")?;
        } else {
            write!(f, " ")?;
        }
        let content = self.header.content(self.body).unwrap();
        write!(f, "{}", content)?;
        if deleted {
            write!(f, "~~")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::block::{BlockMut, CONTENT_TYPE_DELETED, ID};
    use crate::content::ContentType;
    use crate::Clock;

    #[test]
    fn block_set_header_values() {
        let id = ID::new(123.into(), 1.into());
        let left = ID::new(234.into(), 3.into());
        let origin_left = ID::new(234.into(), 13.into());
        let right = ID::new(345.into(), 4.into());
        let origin_right = ID::new(345.into(), 4.into());
        let parent = ID::new(456.into(), 5.into());

        let mut block = BlockMut::new(id);
        block.set_left(Some(&left));
        block.set_right(Some(&right));
        block.set_parent(parent);
        block.set_origin_left(origin_left);
        block.set_origin_right(origin_right);

        assert_eq!(block.left(), Some(&left));
        assert_eq!(block.right(), Some(&right));
        assert_eq!(block.origin_left(), Some(&origin_left));
        assert_eq!(block.origin_right(), Some(&origin_right));
        assert_eq!(block.parent, parent);

        block.set_content_type(CONTENT_TYPE_DELETED);
        block.set_clock_len(2.into());
        block.init_entry_key("key").unwrap();

        assert_eq!(block.clock_len(), Clock::new(2));
        let content = block.content().unwrap();
        assert_eq!(content.content_type(), ContentType::Deleted);
        assert_eq!(block.entry_key(), Some("key"));
    }
}
