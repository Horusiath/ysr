use crate::block_cursor::BlockCursor;
use crate::content::{
    BlockContent, BlockContentMut, ContentFormat, ContentIter, ContentNode, ContentNodeMut,
    ContentRef, ContentType,
};
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeHeader, NodeID, NodeType};
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::transaction::TransactionState;
use crate::{ClientID, Clock, Optional};
use crate::{Error, Result};
use bitflags::bitflags;
use bytes::BytesMut;
use lmdb_rs_m::Database;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::ops::{Deref, DerefMut};
use zerocopy::{CastError, FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(C)]
#[derive(
    PartialEq, Eq, Hash, Copy, Clone, FromBytes, KnownLayout, Immutable, IntoBytes, Default,
)]
pub struct ID {
    pub client: ClientID,
    pub clock: Clock,
}

impl ID {
    #[inline]
    pub const fn new(client: ClientID, clock: Clock) -> Self {
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

impl Serialize for ID {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_tuple(2)?;
        s.serialize_element(&self.client)?;
        s.serialize_element(&self.clock.get())?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct IDVisitor;
        impl<'de> Visitor<'de> for IDVisitor {
            type Value = ID;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("struct ID")
            }

            fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let client: ClientID = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::invalid_length(0, &"expected 2 elements for ID")
                })?;
                let clock: u32 = seq.next_element()?.ok_or_else(|| {
                    serde::de::Error::invalid_length(1, &"expected 2 elements for ID")
                })?;
                let clock = Clock::new(clock);
                Ok(ID::new(client, clock))
            }
        }
        deserializer.deserialize_tuple(2, IDVisitor)
    }
}

#[repr(C)]
#[derive(PartialEq, Eq, FromBytes, KnownLayout, Immutable, IntoBytes, Default)]
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

    pub fn init(
        &mut self,
        len: Clock,
        left: Option<&ID>,
        right: Option<&ID>,
        origin_left: Option<&ID>,
        origin_right: Option<&ID>,
        parent: NodeID,
    ) {
        self.set_clock_len(len);
        self.set_parent(parent);
        self.set_left(left);
        self.set_right(right);
        if let Some(origin_left) = origin_left {
            self.set_origin_left(*origin_left);
        }
        if let Some(origin_right) = origin_right {
            self.set_origin_right(*origin_right);
        }
    }

    pub fn flags(&self) -> BlockFlags {
        self.flags
    }

    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.left.client // same client
            && id.clock >= self.left.clock // id is larger or equal to block's start clock
            && id.clock < self.left.clock + self.clock_len // id is smaller than block's end clock
    }

    pub fn parse(data: &[u8]) -> Result<(&BlockHeader, &[u8]), ParseError> {
        let (header, body) = Self::ref_from_prefix(data)?;
        Ok((header, body))
    }

    pub fn parse_mut(data: &mut [u8]) -> Result<(&mut BlockHeader, &mut [u8]), ParseMutError> {
        let (header, body) = Self::mut_from_prefix(data)?;
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
            let str = unsafe { std::str::from_utf8_unchecked(&body[..self.key_len as usize]) };
            Some(str)
        }
    }

    pub fn parent(&self) -> &NodeID {
        &self.parent
    }

    #[inline]
    pub fn set_parent(&mut self, parent_id: NodeID) {
        self.flags |= BlockFlags::HAS_PARENT;
        self.parent = parent_id;
    }

    pub fn content_slice<'a>(&self, body: &'a [u8]) -> &'a [u8] {
        &body[self.key_len as usize..]
    }

    pub fn content_slice_mut<'a>(&self, body: &'a mut [u8]) -> &'a mut [u8] {
        &mut body[self.key_len as usize..]
    }

    pub fn content<'a>(&self, body: &'a [u8]) -> Result<BlockContent<'a>> {
        let content = self.content_slice(body);
        match self.content_type.try_into()? {
            ContentType::Deleted => Ok(BlockContent::Deleted(self.clock_len)),
            ContentType::Json => Ok(BlockContent::Json(ContentRef::new(content))),
            ContentType::Atom => Ok(BlockContent::Atom(ContentRef::new(content))),
            ContentType::Binary => Ok(BlockContent::Binary(content)),
            ContentType::Doc => Ok(BlockContent::Doc(content)),
            ContentType::Embed => Ok(BlockContent::Embed(content)),
            ContentType::Format => Ok(BlockContent::Format(ContentFormat::new(content)?)),
            ContentType::Node => Ok(BlockContent::Node(ContentNode::new(content)?)),
            ContentType::String => {
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                Ok(BlockContent::Text(str))
            }
        }
    }

    pub fn content_mut<'a>(&self, body: &'a mut [u8]) -> Result<BlockContentMut<'a>> {
        let content = self.content_slice_mut(body);
        match self.content_type.try_into()? {
            ContentType::Deleted => Ok(BlockContentMut::Deleted(self.clock_len)),
            ContentType::Json => Ok(BlockContentMut::Json(ContentRef::new(content))),
            ContentType::Atom => Ok(BlockContentMut::Atom(ContentRef::new(content))),
            ContentType::Binary => Ok(BlockContentMut::Binary(content)),
            ContentType::Doc => Ok(BlockContentMut::Doc(content)),
            ContentType::Embed => Ok(BlockContentMut::Embed(content)),
            ContentType::Format => Ok(BlockContentMut::Format(ContentFormat::new(content)?)),
            ContentType::Node => Ok(BlockContentMut::Node(ContentNodeMut::new(content)?)),
            ContentType::String => {
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                Ok(BlockContentMut::Text(str))
            }
        }
    }

    #[inline]
    pub fn set_content_type(&mut self, content_type: ContentType) {
        self.content_type = content_type as u8;
        if content_type.is_countable() {
            self.flags |= BlockFlags::COUNTABLE;
        } else {
            self.flags -= BlockFlags::COUNTABLE;
        }
    }

    #[inline]
    pub fn clock_len(&self) -> Clock {
        self.clock_len
    }

    #[inline]
    pub fn set_clock_len(&mut self, len: Clock) {
        self.clock_len = len;
    }

    pub fn is_deleted(&self) -> bool {
        self.flags.contains(BlockFlags::DELETED)
    }

    pub fn set_deleted(&mut self) {
        self.flags |= BlockFlags::DELETED;
    }

    pub fn is_countable(&self) -> bool {
        self.flags.contains(BlockFlags::COUNTABLE)
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
        ID::new(self.id.client, self.id.clock + self.clock_len() - 1)
    }

    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.id.client // same client
            && id.clock >= self.id.clock // id is larger or equal to block's start clock
            && id.clock < self.id.clock + self.clock_len() // id is smaller than block's end clock
    }

    #[inline]
    pub fn header(&self) -> &BlockHeader {
        BlockHeader::ref_from_bytes(&self.data[..BlockHeader::SIZE]).unwrap()
    }

    pub fn entry_key(&self) -> Option<&str> {
        let (header, body) = BlockHeader::parse(&self.data).unwrap();
        header.entry_key(body)
    }

    pub fn content(&self) -> Result<BlockContent<'_>> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.content(body)
    }

    pub fn display(&self) -> DisplayBlock<'_> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.display(body)
    }

    pub fn bytes(&self) -> &[u8] {
        self.data
    }

    pub fn to_owned(&self) -> BlockBuilder {
        let mut body = BytesMut::with_capacity(self.data.len());
        body.extend_from_slice(self.data);
        BlockBuilder::parse(self.id, body).unwrap()
    }
}

impl Deref for Block<'_> {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

pub struct BlockMut<'a> {
    id: ID,
    data: &'a mut [u8],
}

impl<'a> BlockMut<'a> {
    pub fn new(id: ID, data: &'a mut [u8]) -> Result<Self> {
        if BlockHeader::parse(data).is_err() {
            Err(Error::MalformedBlock(id))
        } else {
            Ok(Self { id, data })
        }
    }

    pub unsafe fn new_unchecked(id: ID, data: &'a mut [u8]) -> Self {
        Self { id, data }
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn last_id(&self) -> ID {
        ID::new(self.id.client, self.id.clock + self.clock_len() - 1)
    }

    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.id.client // same client
          && id.clock >= self.id.clock // id is larger or equal to block's start clock
          && id.clock < self.id.clock + self.clock_len() // id is smaller than block's end clock
    }

    #[inline]
    pub fn header(&self) -> &BlockHeader {
        BlockHeader::ref_from_bytes(&self.data[..BlockHeader::SIZE]).unwrap()
    }

    #[inline]
    pub fn header_mut(&mut self) -> &mut BlockHeader {
        BlockHeader::mut_from_bytes(&mut self.data[..BlockHeader::SIZE]).unwrap()
    }

    pub fn entry_key(&self) -> Option<&str> {
        let (header, body) = BlockHeader::parse(&self.data).unwrap();
        header.entry_key(body)
    }

    pub fn content(&self) -> Result<BlockContent<'_>> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.content(body)
    }

    pub fn display(&self) -> DisplayBlock<'_> {
        let (header, body) = BlockHeader::parse(self.data).unwrap();
        header.display(body)
    }

    pub fn bytes(&self) -> &[u8] {
        self.data
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.data
    }

    pub fn to_owned(&self) -> BlockBuilder {
        let mut body = BytesMut::with_capacity(self.data.len());
        body.extend_from_slice(self.data);
        BlockBuilder::parse(self.id, body).unwrap()
    }
}

impl Deref for BlockMut<'_> {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

impl DerefMut for BlockMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.header_mut()
    }
}

#[derive(Clone)]
pub struct BlockBuilder {
    id: ID,
    body: BytesMut,
}

impl BlockBuilder {
    pub(crate) fn empty(id: ID) -> Self {
        let header = BlockHeader::default();
        let mut bytes = BytesMut::with_capacity(BlockHeader::SIZE);
        bytes.extend_from_slice(header.as_bytes());
        BlockBuilder { id, body: bytes }
    }

    pub(crate) fn new(
        id: ID,
        len: Clock,
        left: Option<&ID>,
        right: Option<&ID>,
        origin_left: Option<&ID>,
        origin_right: Option<&ID>,
        parent: NodeID,
        entry_key: Option<&str>,
    ) -> crate::Result<Self> {
        let mut block = Self::empty(id);
        block.init(len, left, right, origin_left, origin_right, parent);

        if let Some(key) = entry_key {
            block.init_entry_key(key)?;
        }

        Ok(block)
    }

    pub(crate) fn new_node(node: Node, kind: NodeType) -> Self {
        let name = node.as_str().unwrap_or_default();
        let mut header = BlockHeader::default();
        header.set_clock_len(1.into());
        header.set_content_type(ContentType::Node);
        let mut body = BytesMut::with_capacity(BlockHeader::SIZE + NodeHeader::SIZE + name.len());
        body.extend_from_slice(header.as_bytes());
        body.extend_from_slice(NodeHeader::new(kind as u8).as_bytes());
        body.extend_from_slice(name.as_bytes());
        let id = node.id();
        Self { id, body }
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

    pub fn last_id(&self) -> ID {
        ID::new(self.id.client, self.id.clock + self.clock_len() - 1)
    }

    pub fn entry_key(&self) -> Option<&str> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.entry_key(body)
    }

    pub fn content(&self) -> Result<BlockContent> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.content(body)
    }

    pub fn content_mut(&mut self) -> Result<BlockContentMut<'_>> {
        let (header, body) = BlockHeader::parse_mut(&mut self.body).unwrap();
        header.content_mut(body)
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

    pub fn set_entry_key(&mut self, key: &[u8]) -> crate::Result<()> {
        if key.len() > u8::MAX as usize {
            return Err(Error::KeyTooLong);
        }
        let old_key_len = self.key_len as usize;
        let new_key_len = key.len();
        match new_key_len.cmp(&old_key_len) {
            Ordering::Equal => {}
            Ordering::Less => {
                let diff = old_key_len - new_key_len;
                let content = &self.body[BlockHeader::SIZE + old_key_len..];
                let content_ptr_old = content.as_ptr();
                unsafe {
                    let content_ptr_new = content_ptr_old.sub(diff) as *mut u8;
                    std::ptr::copy(content_ptr_old, content_ptr_new, content.len());
                }
                self.body.truncate(self.body.len() - diff);
            }
            Ordering::Greater => {
                let diff = new_key_len - old_key_len;
                self.body.resize(self.body.len() + diff, 0);
                let content = &self.body[BlockHeader::SIZE + old_key_len..];
                let content_ptr_old = content.as_ptr();
                unsafe {
                    let content_ptr_new = content_ptr_old.add(diff) as *mut u8;
                    std::ptr::copy(content_ptr_old, content_ptr_new, content.len());
                }
            }
        }
        let old_key = &mut self.body[BlockHeader::SIZE..];
        unsafe {
            std::ptr::copy_nonoverlapping(key.as_ptr(), old_key.as_mut_ptr(), key.len());
        }
        self.key_len = new_key_len as u8;
        Ok(())
    }

    pub(crate) fn init_content(&mut self, content: BlockContent) -> crate::Result<()> {
        self.set_content_type(content.content_type());
        let body = content.body();
        self.body.extend_from_slice(body);
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

    pub fn splice(&mut self, offset: Clock) -> crate::Result<Option<Self>> {
        if offset == 0 {
            Ok(None)
        } else {
            let id = self.id;
            let client = id.client;
            let clock = id.clock;
            let len = self.clock_len();

            let new_id = ID::new(client, clock + offset);
            let last_id = ID::new(client, clock + offset - 1);
            let mut right = Self::new(
                new_id,
                len - offset,
                Some(&last_id),
                self.right(),
                Some(&last_id),
                self.origin_right(),
                self.parent,
                self.entry_key(),
            )?;
            self.split(offset, &mut right)?;
            self.set_right(Some(&right.id));

            Ok(Some(right))
        }
    }

    fn split(&mut self, offset: Clock, into: &mut BlockBuilder) -> crate::Result<()> {
        let (header, body) = BlockHeader::parse_mut(&mut self.body).unwrap();
        let key_len = header.key_len as usize;
        let content = &body[key_len..];
        header.clock_len = offset;
        into.content_type = header.content_type;
        if header.flags.contains(BlockFlags::COUNTABLE) {
            into.flags |= BlockFlags::COUNTABLE;
        } else {
            into.flags -= BlockFlags::COUNTABLE;
        }
        match header.content_type.try_into()? {
            ContentType::String => {
                let offset = offset.get() as usize;
                let str = unsafe { std::str::from_utf8_unchecked(content) };
                let remainder = str[offset..].as_bytes();
                into.body.extend_from_slice(remainder);
                self.body.truncate(BlockHeader::SIZE + key_len + offset);
            }
            ContentType::Atom | ContentType::Json => {
                let content_iter = ContentIter::new(content);
                let offset = offset.get() as usize;
                if let Some(slice) = content_iter.slice(offset) {
                    into.body.extend_from_slice(slice);
                    let offset = slice.len();
                    self.body.truncate(self.body.len() - offset);
                }
            }
            _ => { /* other contents are no op */ }
        }
        Ok(())
    }

    pub fn merge(&mut self, other: &BlockBuilder) -> bool {
        if self.can_merge(other) {
            let (other_header, other_body) = BlockHeader::parse(&other.body).unwrap();
            let other_content = other_header.content_slice(other_body);

            self.body.extend_from_slice(other_content);
            self.clock_len += other.clock_len;
            self.set_right(other.right());

            true
        } else {
            false
        }
    }

    pub fn can_merge(&self, other: &Self) -> bool {
        self.id.client == other.id.client
            && self.right == other.id
            && self.id.clock + self.clock_len() == other.id.clock
            && other.origin_left() == Some(&self.last_id())
            && self.origin_right() == other.origin_right()
            && self.is_deleted() == other.is_deleted()
            && self.content_type == other.content_type
            && (self.content_type == CONTENT_TYPE_DELETED
                || self.content_type == CONTENT_TYPE_STRING
                || self.content_type == CONTENT_TYPE_ATOM
                || self.content_type == CONTENT_TYPE_JSON)
    }

    pub(crate) fn integrate(
        &mut self,
        db: &mut Database,
        tx_state: &mut TransactionState,
        context: &mut IntegrationContext,
    ) -> crate::Result<()> {
        if context.offset > 0 {
            // offset could be > 0 only in context of Update::integrate,
            // is such case offset kind in use always means Yjs-compatible offset (utf-16)

            /*this.id.clock += offset;
            this.left = store
                .blocks
                .get_item_clean_end(&ID::new(this.id.client, this.id.clock - 1))
                .map(|slice| store.materialize(slice));
            this.origin = this.left.as_deref().map(|b: &Item| b.last_id());
            this.content = this
                .content
                .splice(offset as usize, OffsetKind::Utf16)
                .unwrap();
            this.len -= offset;*/

            self.id.clock += context.offset;
            let left = match db.split_block(ID::new(self.id.client, self.id.clock - 1))? {
                SplitResult::Unchanged(left) => left.last_id(),
                SplitResult::Split(left, _right) => left.last_id(), //TODO: *self = right; ?
            };
            self.set_left(Some(&left));
            self.set_origin_left(left);
        }

        if context.detect_conflict(self) {
            context.resolve_conflict(self, db)?;
        }

        if self.entry_key().is_none() {
            // try inherit entry key from left/right neighbor
            let entry_key = context
                .left
                .as_ref()
                .and_then(BlockBuilder::entry_key)
                .or_else(|| context.right.as_ref().and_then(BlockBuilder::entry_key));

            if let Some(key) = entry_key {
                self.set_entry_key(key.as_bytes())?;
            }
        }

        let mut parent_node = match &mut context.parent {
            None => todo!("delete current block"),
            Some(parent_block) => {
                if let BlockContentMut::Node(node) = parent_block.content_mut()? {
                    node
                } else {
                    return Err(crate::Error::MalformedBlock(parent_block.id));
                }
            }
        };

        // reconnect left/right + update parent map/start if necessary
        if let Some(left) = &mut context.left {
            self.set_right(left.right());
            left.set_right(Some(self.id()));
        } else {
            let right = if let Some(key) = self.entry_key() {
                // add current block to the beginning of YMap entries
                let mut right = db.entry(self.parent(), key)?;
                let mut cursor = BlockCursor::new(db.new_cursor()?);
                if let Some(()) = cursor.seek(right).optional()? {
                    // move until the left-most block
                    while let Some(block) = cursor.next_left().optional()? {
                        right = block.id;
                    }
                }
                Some(right)
            } else {
                // current block is new head of the list
                let old = parent_node.start().cloned();
                parent_node.set_start(Some(self.id()));
                old
            };
            self.set_right(right.as_ref());
        }

        if let Some(right) = self.right() {
            if context
                .right
                .as_ref()
                .map(|r| !r.contains(right))
                .unwrap_or(true)
            {
                let right = db.block_containing(*right, true)?;
                context.right = Some(right.into());
            }
            let right = context.right.as_mut().unwrap();
            right.set_left(Some(self.id()));
        } else if let Some(entry_key) = self.entry_key() {
            // set as current parent value if right === null and this is parentSub
            db.set_entry(self.parent(), entry_key, self.id())?;
            /*TODO:
               if let Some(mut left) = this.left {
                   #[cfg(feature = "weak")]
                   {
                       if left.info.is_linked() {
                           // inherit links from the block we're overriding
                           left.info.clear_linked();
                           this.info.set_linked();
                           let all_links = &mut txn.doc_mut().linked_by;
                           if let Some(linked_by) = all_links.remove(&left) {
                               all_links.insert(self_ptr, linked_by);
                               // since left is being deleted, it will remove
                               // its links from store.linkedBy anyway
                           }
                       }
                   }
                   // this is the current attribute value of parent. delete right
                   txn.delete(left);
               }
            */
        }

        if self.entry_key().is_none() && !self.is_deleted() {
            if self.is_countable() {
                // adjust length of parent
                let parent_block = context.parent.as_mut().unwrap();
                parent_block.clock_len += self.clock_len;
            }
            /*TODO:
               #[cfg(feature = "weak")]
               match (this.left, this.right) {
                   (Some(l), Some(r)) if l.info.is_linked() || r.info.is_linked() => {
                       crate::types::weak::join_linked_range(self_ptr, txn)
                   }
                   _ => {}
               }
            */
        }

        /*TODO:
           // check if this item is in a moved range
           let left_moved = this.left.and_then(|i| i.moved);
           let right_moved = this.right.and_then(|i| i.moved);
           let (doc, state) = txn.split_mut();
           if left_moved.is_some() || right_moved.is_some() {
               if left_moved == right_moved {
                   this.moved = left_moved;
               } else {
                   #[inline]
                   fn try_integrate(
                       mut item: ItemPtr,
                       doc: &mut Doc,
                       state: &mut TransactionState,
                   ) {
                       let ptr = item.clone();
                       if let ItemContent::Move(m) = &mut item.content {
                           if !m.is_collapsed() {
                               m.integrate_block(doc, state, ptr);
                           }
                       }
                   }

                   if let Some(ptr) = left_moved {
                       try_integrate(ptr, doc, state);
                   }

                   if let Some(ptr) = right_moved {
                       try_integrate(ptr, doc, state);
                   }
               }
           }
        */

        match self.content()? {
            BlockContent::Deleted(len) => {
                tx_state.delete_set.insert(self.id, len);
                self.set_deleted();
            }
            BlockContent::Doc(doc_id) => {
                /*
                   let mut borrowed = subdoc.borrow_mut();
                   doc.subdocs.insert((borrowed.guid(), this.id));
                   borrowed.subdoc = Some(self_ptr);
                   let should_load = borrowed.should_load();
                   drop(borrowed);

                   let subdocs = state.subdocs.get_or_init();
                   if should_load {
                       subdocs.loaded.push(SubDocHook::new(subdoc.clone()));
                   }
                   subdocs.added.push(SubDocHook::new(subdoc.clone()));
                */
            }
            _ => { /* do nothing */ }
        }

        /*
        if let Some(mut parent_ref) = parent {

            state.add_changed_type(parent_ref, this.parent_sub.clone());
            if this.info.is_linked() {
                if let Some(links) = doc.linked_by.get(&self_ptr).cloned() {
                    // notify links about changes
                    for link in links.iter() {
                        state.add_changed_type(*link, this.parent_sub.clone());
                    }
                }
            }
            let parent_deleted = if let TypePtr::Branch(ptr) = &this.parent {
                if let Some(block) = ptr.item {
                    block.is_deleted()
                } else {
                    false
                }
            } else {
                false
            };
            if parent_deleted || (this.parent_sub.is_some() && this.right.is_some()) {
                // delete if parent is deleted or if this is not the current attribute value of parent
                true
            } else {
                false
            }
        } else {
            true
        }
         */

        db.insert_block(self.as_ref())?;
        if let Some(right) = context.right.as_mut() {
            db.insert_block(right.as_ref())?;
        }
        if let Some(left) = context.left.as_mut() {
            db.insert_block(left.as_ref())?;
        }
        if let Some(parent_block) = context.parent.as_mut() {
            db.insert_block(parent_block.as_ref())?;
        }

        Ok(())
    }

    pub fn display(&self) -> DisplayBlock<'_> {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        header.display(body)
    }
}

impl<'a> From<Block<'a>> for BlockBuilder {
    fn from(value: Block<'a>) -> Self {
        let mut body = BytesMut::with_capacity(value.data.len());
        body.extend_from_slice(value.data);
        Self::parse(*value.id(), body).unwrap()
    }
}

impl Deref for BlockBuilder {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        BlockHeader::ref_from_bytes(&self.body[..BlockHeader::SIZE]).unwrap()
    }
}

impl DerefMut for BlockBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        BlockHeader::mut_from_bytes(&mut self.body[..BlockHeader::SIZE]).unwrap()
    }
}

impl Display for BlockBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        write!(f, "{}, {}", self.id, header.display(body))
    }
}

impl Debug for BlockBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Eq for BlockBuilder {}
impl PartialEq for BlockBuilder {
    fn eq(&self, other: &Self) -> bool {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        let (other_header, other_body) = BlockHeader::parse(&other.body).unwrap();
        header == other_header && body == other_body
    }
}

pub struct Writer<'a> {
    block: &'a mut BlockBuilder,
}

impl<'a> Writer<'a> {
    fn new(block: &'a mut BlockBuilder) -> Self {
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
#[derive(Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
pub struct BlockFlags(u8);

bitflags! {
    impl BlockFlags : u8 {
        /// Only used at decoding phase.
        const HAS_PARENT = 0b0000_0001;
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
        write!(f, ",")?;
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
    use crate::block::{BlockBuilder, ID};
    use crate::content::{BlockContent, ContentRef, ContentType};
    use crate::node::NodeID;
    use crate::{ClientID, Clock};
    use bytes::{BufMut, BytesMut};
    use serde::{Deserialize, Serialize};
    use zerocopy::IntoBytes;

    const CLIENT: ClientID = unsafe { ClientID::new_unchecked(123) };
    const PARENT: NodeID = NodeID::from_nested(ID::new(CLIENT, Clock::new(0)));

    #[test]
    fn id_serialize() {
        let id = ID::new(CLIENT, 42.into());
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, r#"{"client":123,"clock":42}"#);
    }

    #[test]
    fn block_set_header_values() {
        let id = ID::new(CLIENT, 1.into());
        let left = ID::new(CLIENT, 3.into());
        let o_left = ID::new(CLIENT, 13.into());
        let right = ID::new(CLIENT, 4.into());
        let o_right = ID::new(CLIENT, 4.into());

        let mut block = block(1, 2, 3, 4, 13, 4, Some("key"));
        block.set_content_type(ContentType::Deleted);

        assert_eq!(block.left(), Some(&left));
        assert_eq!(block.right(), Some(&right));
        assert_eq!(block.origin_left(), Some(&o_left));
        assert_eq!(block.origin_right(), Some(&o_right));
        assert_eq!(block.parent, PARENT);

        assert_eq!(block.clock_len(), Clock::new(2));
        let content = block.content().unwrap();
        assert_eq!(content.content_type(), ContentType::Deleted);
        assert_eq!(block.entry_key(), Some("key"));
    }

    #[test]
    fn block_set_key_shorter() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block
            .init_content(BlockContent::Text("hello world"))
            .unwrap();

        block.set_entry_key("123".as_bytes()).unwrap();

        assert_eq!(block.entry_key(), Some("123"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_set_key_longer() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block
            .init_content(BlockContent::Text("hello world"))
            .unwrap();

        block.set_entry_key("test123".as_bytes()).unwrap();

        assert_eq!(block.entry_key(), Some("test123"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_set_key_equal() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block
            .init_content(BlockContent::Text("hello world"))
            .unwrap();

        block.set_entry_key("1234".as_bytes()).unwrap();

        assert_eq!(block.entry_key(), Some("1234"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_split_text() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Text("hello world")).unwrap();

        let right = b.splice(6.into()).unwrap().unwrap();
        let mut expected_right = block(7, 5, 6, 13, 6, 15, Some("key"));
        expected_right
            .init_content(BlockContent::Text("world"))
            .unwrap();
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 6, 12, 7, 14, 15, Some("key"));
        expected_left
            .init_content(BlockContent::Text("hello "))
            .unwrap();
        assert_eq!(b, expected_left);
    }

    #[test]
    fn block_merge_text() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Text("hello world")).unwrap();

        let expected = b.clone();

        let right = b.splice(6.into()).unwrap().unwrap();
        assert!(b.merge(&right));

        assert_eq!(b, expected);
    }

    #[test]
    fn block_split_deleted() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Deleted(11.into())).unwrap();

        let right = b.splice(6.into()).unwrap().unwrap();
        let mut expected_right = block(7, 5, 6, 13, 6, 15, Some("key"));
        expected_right
            .init_content(BlockContent::Deleted(5.into()))
            .unwrap();
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 6, 12, 7, 14, 15, Some("key"));
        expected_left
            .init_content(BlockContent::Deleted(6.into()))
            .unwrap();
        assert_eq!(b, expected_left);
    }

    #[test]
    fn block_merge_deleted() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Deleted(11.into())).unwrap();

        let expected = b.clone();

        let right = b.splice(6.into()).unwrap().unwrap();
        assert!(b.merge(&right));

        assert_eq!(b, expected);
    }

    #[test]
    fn block_split_atom() {
        let alice = crate::lib0::to_vec(&User::new("Alice")).unwrap();
        let bob = crate::lib0::to_vec(&User::new("Bob")).unwrap();
        let mut buf = BytesMut::new();
        buf.put_u32_le(alice.len() as u32);
        buf.put_slice(alice.as_bytes());
        buf.put_u32_le(bob.len() as u32);
        buf.put_slice(bob.as_bytes());

        let mut b = block(1, 2, 0, 3, 4, 5, Some("aa"));
        b.init_content(BlockContent::Atom(ContentRef::new(&buf)))
            .unwrap();

        let right = b.splice(1.into()).unwrap().unwrap();
        let mut expected_right = block(2, 1, 1, 3, 1, 5, Some("aa"));
        expected_right
            .init_content(BlockContent::Atom(ContentRef::new(&buf[4 + alice.len()..])))
            .unwrap();
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 1, 0, 2, 4, 5, Some("aa"));
        expected_left
            .init_content(BlockContent::Atom(ContentRef::new(&buf[..4 + alice.len()])))
            .unwrap();
        assert_eq!(b, expected_left);
    }

    #[test]
    fn block_merge_atom() {
        let alice = crate::lib0::to_vec(&User::new("Alice")).unwrap();
        let bob = crate::lib0::to_vec(&User::new("Bob")).unwrap();
        let mut buf = BytesMut::new();
        buf.put_u32_le(alice.len() as u32);
        buf.put_slice(alice.as_bytes());
        buf.put_u32_le(bob.len() as u32);
        buf.put_slice(bob.as_bytes());

        let mut b = block(1, 2, 0, 3, 4, 5, Some("aa"));
        b.init_content(BlockContent::Atom(ContentRef::new(&buf)))
            .unwrap();

        let expected = b.clone();

        let right = b.splice(1.into()).unwrap().unwrap();
        assert!(b.merge(&right));

        assert_eq!(b, expected);
    }

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct User {
        name: String,
    }

    impl User {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
            }
        }
    }

    fn block(
        id: u32,
        len: u32,
        left: u32,
        right: u32,
        origin_left: u32,
        origin_right: u32,
        entry: Option<&str>,
    ) -> BlockBuilder {
        BlockBuilder::new(
            ID::new(CLIENT, id.into()),
            len.into(),
            Some(&ID::new(CLIENT, left.into())),
            Some(&ID::new(CLIENT, right.into())),
            Some(&ID::new(CLIENT, origin_left.into())),
            Some(&ID::new(CLIENT, origin_right.into())),
            PARENT,
            entry,
        )
        .unwrap()
    }
}
