use crate::block_reader::BlockRange;
use crate::content::{Content, ContentType, utf16_to_utf8};
use crate::integrate::IntegrationContext;
use crate::lmdb::Database;
use crate::node::{Node, NodeID, NodeType};
use crate::store::Db;
use crate::store::block_store::SplitResult;
use crate::transaction::TransactionState;
use crate::{ClientID, Clock, Optional, U32};
use crate::{Error, Result};
use bitflags::{Flags, bitflags};
use bytes::Bytes;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize};
use smallvec::{SmallVec, smallvec};
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::ops::{Deref, DerefMut};
use zerocopy::{CastError, FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(C)]
#[derive(
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    FromBytes,
    KnownLayout,
    Immutable,
    IntoBytes,
    Default,
    Ord,
    PartialOrd,
)]
pub struct ID {
    pub client: ClientID,
    pub clock: Clock,
}

impl ID {
    pub const SIZE: usize = size_of::<ID>();

    #[inline]
    pub const fn new(client: ClientID, clock: Clock) -> Self {
        Self { client, clock }
    }

    pub fn parse(bytes: &[u8]) -> crate::Result<&Self> {
        Self::ref_from_bytes(bytes).map_err(|_| Error::InvalidMapping("ID"))
    }

    #[inline]
    pub fn into_bytes(self) -> [u8; Self::SIZE] {
        unsafe { std::mem::transmute(self) }
    }

    #[inline]
    pub fn from_bytes(bytes: [u8; Self::SIZE]) -> Self {
        unsafe { std::mem::transmute(bytes) }
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
#[derive(Clone, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable, IntoBytes)]
pub struct BlockHeader {
    /// Yjs-compatible length of the block. Counted as a number of countable elements or
    /// UTF-16 characters.
    len: Clock,
    /// Flags that define the block's state, including presence/absence of other fields like
    /// neighbor blocks or origins.
    flags: BlockFlags,
    /// Used only when [ContentType::Node] is set. Defines the type of the node.
    node_type: NodeType,
    /// Type of the content stored by this block.
    content_type: ContentType,
    /// If we have inlined content, we keep its length here.
    inline_content_len: u8,
    /// NodeID of the parent node collection that contains this block.
    parent: NodeID,
    /// ID of the left neighbor block (if such exists).
    left: ID,
    /// ID of the right neighbor block (if such exists).
    right: ID,
    /// Inlined content data.
    inline_content: [u8; Self::INLINE_CONTENT_LEN],
    /// XX Hash of the key if provided, 0 otherwise.
    key_hash: U32,
    /// ID of the left neighbor block at the point of insertion (if such existed).
    origin_left: ID,
    /// ID of the right neighbor block at the point of insertion (if such existed).
    origin_right: ID,
}

impl BlockHeader {
    pub const SIZE: usize = size_of::<BlockHeader>();
    pub const INLINE_CONTENT_LEN: usize = 8;

    pub fn empty() -> Self {
        BlockHeader {
            len: Clock::new(0),
            flags: BlockFlags::default(),
            node_type: NodeType::default(),
            content_type: ContentType::Deleted,
            inline_content_len: 0,
            parent: NodeID::default(),
            key_hash: U32::new(0),
            left: ID::default(),
            right: ID::default(),
            origin_left: ID::default(),
            origin_right: ID::default(),
            inline_content: Default::default(),
        }
    }

    pub fn new(
        len: Clock,
        left: Option<&ID>,
        right: Option<&ID>,
        origin_left: Option<&ID>,
        origin_right: Option<&ID>,
        parent: NodeID,
        entry: Option<&str>,
    ) -> Self {
        let mut flags = BlockFlags::empty();
        if left.is_some() {
            flags |= BlockFlags::LEFT;
        }
        if right.is_some() {
            flags |= BlockFlags::RIGHT;
        }
        if origin_left.is_some() {
            flags |= BlockFlags::ORIGIN_LEFT;
        }
        if origin_right.is_some() {
            flags |= BlockFlags::ORIGIN_RIGHT;
        }
        let key_hash: U32 = if let Some(entry) = entry {
            twox_hash::XxHash32::oneshot(0, entry.as_bytes()).into()
        } else {
            U32::new(0)
        };
        Self {
            len: len,
            flags,
            node_type: NodeType::Unknown,
            content_type: ContentType::Deleted,
            inline_content_len: 0,
            parent,
            key_hash,
            left: left.copied().unwrap_or_default(),
            right: right.copied().unwrap_or_default(),
            origin_left: origin_left.copied().unwrap_or_default(),
            origin_right: origin_right.copied().unwrap_or_default(),
            inline_content: Default::default(),
        }
    }

    #[inline]
    pub fn flags(&self) -> BlockFlags {
        self.flags
    }

    #[inline]
    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.left.client // same client
            && id.clock >= self.left.clock // id is larger or equal to block's start clock
            && id.clock < self.left.clock + self.len // id is smaller than block's end clock
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

    pub fn key_hash(&self) -> Option<&U32> {
        if self.key_hash == U32::new(0) {
            None
        } else {
            Some(&self.key_hash)
        }
    }

    pub fn set_key_hash(&mut self, hash: Option<U32>) {
        self.key_hash = hash.unwrap_or(U32::new(0));
    }

    pub fn parent(&self) -> &NodeID {
        &self.parent
    }

    #[inline]
    pub fn set_parent(&mut self, parent_id: NodeID) {
        self.parent = parent_id;
    }

    pub fn start(&self) -> Option<&ID> {
        if self.content_type() == ContentType::Node {
            let bytes = self.try_inline_data()?;
            let id = ID::ref_from_bytes(bytes).ok()?;
            Some(id)
        } else {
            None
        }
    }

    pub fn set_start(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                let content = Content::new(ContentType::Node, Cow::Borrowed(id.as_bytes()));
                self.set_inline_content(&content);
            }
            None => {
                self.clear_inline_content();
            }
        }
    }

    pub fn node_type(&self) -> Option<&NodeType> {
        if self.content_type() == ContentType::Node {
            Some(&self.node_type)
        } else {
            None
        }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        if self.content_type() != ContentType::Node {
            panic!("cannot set node_type when content_type is not Node");
        }
        self.node_type = node_type;
    }

    #[inline]
    pub fn set_content_type(&mut self, content_type: ContentType) {
        self.content_type = content_type;
        if content_type.is_countable() {
            self.flags |= BlockFlags::COUNTABLE;
        } else {
            self.flags -= BlockFlags::COUNTABLE;
        }
        if matches!(self.content_type(), ContentType::Deleted) {
            self.flags |= BlockFlags::DELETED;
        } else {
            self.flags -= BlockFlags::DELETED;
        }
    }

    /// Number of countable elements within this block. The rules are:
    /// - [ContentType::Atom]/[ContentType::Json] return a number of elements represented by a single block.
    /// - [ContentType::String] returns a number of UTF-16 characters.
    /// - Other content types return `1`.
    #[inline]
    pub fn clock_len(&self) -> Clock {
        match self.content_type() {
            ContentType::String | ContentType::Atom | ContentType::Json | ContentType::Deleted => {
                self.len
            }
            _ => Clock::new(1),
        }
    }

    /// Returns a number of elements stored within this Y-collection.
    /// Works only on nodes (current block has [ContentType::Node]).
    pub fn node_len(&self) -> usize {
        if self.content_type() == ContentType::Node {
            self.len.get() as usize
        } else {
            0
        }
    }

    #[inline]
    pub fn set_clock_len(&mut self, len: Clock) {
        self.len = len;
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

    pub fn set_inline_content(&mut self, content: &Content<'_>) -> bool {
        let bytes = content.bytes();
        if bytes.len() <= Self::INLINE_CONTENT_LEN {
            self.inline_content[..bytes.len()].copy_from_slice(bytes);
            self.inline_content_len = bytes.len() as u8;
            self.set_content_type(content.content_type());
            self.flags |= BlockFlags::INLINE_CONTENT;
            true
        } else {
            false
        }
    }

    pub fn clear_inline_content(&mut self) {
        self.inline_content_len = 0; //TODO: we could remove this
        self.flags -= BlockFlags::INLINE_CONTENT;
    }

    pub fn try_inline_data(&self) -> Option<&[u8]> {
        if self.flags.contains(BlockFlags::INLINE_CONTENT) {
            let bytes = &self.inline_content[..self.inline_content_len as usize];
            Some(bytes)
        } else {
            None
        }
    }

    pub fn try_inline_content(&self) -> Option<Content<'_>> {
        if self.is_deleted() {
            Some(Content::DELETED)
        } else {
            self.try_inline_data()
                .map(|bytes| Content::new(self.content_type(), Cow::Borrowed(bytes)))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Block<'a> {
    id: ID,
    header: &'a BlockHeader,
}

impl<'a> Block<'a> {
    pub fn new(id: ID, data: &'a [u8]) -> Result<Self> {
        match BlockHeader::try_ref_from_bytes(data) {
            Ok(header) => Ok(Self { id, header }),
            Err(_) => Err(Error::MalformedBlock(id)),
        }
    }

    pub fn range(&self) -> BlockRange {
        let id = *self.id();
        BlockRange::new(id, id.clock + self.len)
    }

    pub(crate) fn info_flags(&self) -> u8 {
        let mut info = self.content_type() as u8 | (self.flags.0 & 0b1100_0000); // has left & right origin
        if self.key_hash != U32::new(0) {
            info |= 0b0010_0000;
        }
        info
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
        self.header
    }

    /// Returns the inline content of this block, if any, with the lifetime
    /// tied to the underlying `BlockHeader` reference (`'a`) rather than the
    /// local borrow of `self` (which is what you'd get going through `Deref`).
    pub fn try_inline_content(&self) -> Option<Content<'a>> {
        let header: &'a BlockHeader = self.header;
        header.try_inline_content()
    }
}

impl Deref for Block<'_> {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

#[repr(C)] // use C repr to make sure that id, header order is unchanged
#[derive(Debug, Clone, PartialEq)]
pub struct BlockMut {
    id: ID,
    header: BlockHeader,
}

impl BlockMut {
    pub fn new(id: ID, header: BlockHeader) -> Self {
        BlockMut { id, header }
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
        &self.header
    }

    #[inline]
    pub fn header_mut(&mut self) -> &mut BlockHeader {
        &mut self.header
    }

    pub fn split(&mut self, offset: Clock) -> Option<Self> {
        let is_deleted = self.is_deleted(); //TODO: should we also delete is_deleted blocks
        let is_countable = self.is_countable();
        if offset == 0 || offset > self.len || !(is_countable || is_deleted) {
            None
        } else {
            let clock_len = self.len;
            self.len = offset;

            let mut flags = self.flags;
            flags |= BlockFlags::ORIGIN_LEFT;
            flags |= BlockFlags::LEFT;
            let left = ID::new(self.id.client, self.id.clock + offset - 1);
            let right = self.right;

            self.right = ID::new(self.id.client, self.id.clock + offset);
            self.flags |= BlockFlags::RIGHT;

            let mut inline_content = [0u8; BlockHeader::INLINE_CONTENT_LEN];
            let mut inline_content_len: u8 = 0;
            if let Some(bytes) = self.try_inline_data()
                && self.content_type() == ContentType::String
            {
                // split inlined content - only applicable to strings
                let source = unsafe { std::str::from_utf8_unchecked(bytes) };
                let utf16_offset = offset.get() as usize;
                if let Some(utf8_offset) = utf16_to_utf8(source, utf16_offset) {
                    let bytes = &bytes[utf8_offset..];
                    inline_content[..bytes.len()].copy_from_slice(bytes);
                    inline_content_len = bytes.len() as u8;
                    self.inline_content_len = utf8_offset as u8; // we don't need to truncate content itself
                }
            }

            let right = BlockHeader {
                len: clock_len - offset,
                flags,
                node_type: Default::default(),
                content_type: self.content_type(),
                inline_content_len,
                inline_content,
                parent: self.parent,
                key_hash: self.key_hash,
                left,
                right,
                origin_left: left,
                origin_right: self.origin_right,
            };
            Some(Self::new(self.right, right))
        }
    }

    pub fn merge(&mut self, other: Block<'_>) -> bool {
        if self.can_merge(&other) {
            self.len += other.len;
            self.set_right(other.right());
            // other.right.left points to the last id, so we don't need to update it
            if self.content_type() == ContentType::String
                && self.len.get() <= BlockHeader::INLINE_CONTENT_LEN as u32
            {
                let size = self.inline_content_len as usize;
                let other_size = other.inline_content_len as usize;
                self.inline_content[size..(size + other_size)]
                    .copy_from_slice(&other.inline_content[..other_size]);
                self.inline_content_len += other_size as u8;
            }
            true
        } else {
            false
        }
    }

    pub fn can_merge(&self, other: &Block<'_>) -> bool {
        self.id.client == other.id.client
            && self.right == other.id
            && self.id.clock + self.clock_len() == other.id.clock
            && other.origin_left() == Some(&self.last_id())
            && self.origin_right() == other.origin_right()
            && self.is_deleted() == other.is_deleted()
            && self.content_type() == other.content_type()
            && self.content_type().is_mergeable()
    }

    pub fn as_block(&self) -> Block<'_> {
        Block {
            id: self.id,
            header: &self.header,
        }
    }
}

impl Deref for BlockMut {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

impl DerefMut for BlockMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.header_mut()
    }
}

impl<'a> From<Block<'a>> for BlockMut {
    fn from(value: Block<'a>) -> Self {
        BlockMut::new(value.id, value.header.clone())
    }
}

impl<'a> From<&'a BlockMut> for Block<'a> {
    #[inline]
    fn from(value: &'a BlockMut) -> Self {
        value.as_block()
    }
}

#[derive(Clone, PartialEq)]
pub struct InsertBlockData {
    /// Globally unique block identifier.
    pub block: BlockMut,
    /// Block content as serialized bytes. The actual content depends on the `content_type` field in the header:
    /// - For [ContentType::String] it's UTF-8 encoded string bytes.
    /// - For [ContentType::Json] and [ContentType::Atom] it's series of length-prefixed, JSON- or
    ///   lib0-encoded messages, each containing single [List] element.
    /// - For [ContentType::Embed] it's embedded data.
    /// - For [ContentType::Format] it's key-value pair of formatting attributes.
    /// - For [ContentType::Doc] it's the ID of the document.
    /// - For other content types it's empty.
    pub content: SmallVec<[Content<'static>; 1]>,
    /// Parent node identifier that contains this block.
    pub parent: Option<Node<'static>>,
    /// If the block is part of a map-like structure, this field contains the UTF-8 encoded key string.
    pub entry: Option<Bytes>,
}

impl InsertBlockData {
    pub(crate) fn new(
        id: ID,
        len: Clock,
        left: Option<&ID>,
        right: Option<&ID>,
        origin_left: Option<&ID>,
        origin_right: Option<&ID>,
        parent: Node<'_>,
        entry_key: Option<&str>,
    ) -> Self {
        let parent = parent.to_owned();
        let parent_id = parent.id();
        let block = BlockMut::new(
            id,
            BlockHeader::new(
                len,
                left,
                right,
                origin_left,
                origin_right,
                parent_id,
                entry_key,
            ),
        );
        Self {
            block,
            parent: Some(parent),
            entry: entry_key.map(|key| Bytes::copy_from_slice(key.as_bytes())),
            content: smallvec![],
        }
    }

    pub fn content(&self) -> &[Content<'_>] {
        &self.content
    }

    pub(crate) fn new_node(node: &Node, kind: NodeType, start: Option<ID>) -> Self {
        let id = node.id();
        let content = match start {
            None => Content::new(ContentType::Node, Cow::Borrowed(&[0u8; size_of::<ID>()])),
            Some(id) => Content::new(ContentType::Node, Cow::Owned(id.as_bytes().into())),
        };
        Self {
            block: BlockMut::new(
                id,
                BlockHeader {
                    len: 1.into(),
                    flags: BlockFlags::COUNTABLE,
                    node_type: kind,
                    content_type: ContentType::Node,
                    inline_content_len: size_of::<NodeID>() as u8,
                    parent: id,
                    key_hash: Default::default(),
                    left: Default::default(),
                    right: Default::default(),
                    origin_left: Default::default(),
                    origin_right: Default::default(),
                    inline_content: id.into_bytes(),
                },
            ),
            parent: None,
            entry: None,
            content: smallvec![content],
        }
    }

    pub fn id(&self) -> &ID {
        &self.block.id
    }

    pub fn last_id(&self) -> ID {
        ID::new(
            self.block.id.client,
            self.block.id.clock + self.clock_len() - 1,
        )
    }

    pub fn parent(&self) -> Option<&Node<'static>> {
        self.parent.as_ref()
    }

    pub fn entry_key(&self) -> Option<&str> {
        match &self.entry {
            None => None,
            Some(bytes) => Some(unsafe { std::str::from_utf8_unchecked(bytes) }),
        }
    }

    pub(crate) fn clock_len(&self) -> Clock {
        self.block.clock_len()
    }

    pub(crate) fn set_entry_key<S: AsRef<[u8]>>(&mut self, key: S) {
        self.entry = Some(Bytes::copy_from_slice(key.as_ref()));
    }

    pub fn merge(&mut self, other: Self) -> bool {
        if self.block.merge(other.block.as_block()) {
            if !self.content.is_empty() {
                self.content.extend(other.content);
                if self.block.content_type() == ContentType::String {
                    // compress the contents
                    let mut buf = Vec::new();
                    for content in self.content.drain(..) {
                        buf.extend_from_slice(content.data.as_bytes());
                    }
                    self.content = smallvec![Content::new(ContentType::String, Cow::Owned(buf))];
                }
            }
            true
        } else {
            false
        }
    }

    pub fn as_block(&self) -> Block<'_> {
        Block {
            id: self.block.id,
            header: &self.block.header,
        }
    }

    pub fn as_block_mut(&mut self) -> &mut BlockMut {
        &mut self.block
    }

    pub(crate) fn integrate(
        &mut self,
        db: &Database,
        tx_state: &mut TransactionState,
        context: &mut IntegrationContext,
    ) -> crate::Result<()> {
        let blocks = db.blocks();
        let mut block_cursor = blocks.cursor()?;
        if context.offset > 0 {
            // offset could be > 0 only in context of Update::integrate,
            // is such case offset kind in use always means Yjs-compatible offset (utf-16)

            self.block.id.clock += context.offset;
            let split_id = ID::new(self.block.id.client, self.block.id.clock - 1);
            let result = block_cursor.split(split_id)?;
            let left = match result {
                SplitResult::Unchanged(left) => left.last_id(),
                SplitResult::Split(left, _right) => left.last_id(), //TODO: *self = right; ?
            };
            self.block.set_left(Some(&left));
            self.block.set_origin_left(left);
        }

        if context.detect_conflict(self) {
            context.resolve_conflict(self, db)?;
        }

        if self.entry_key().is_none() {
            // try to inherit entry key from left/right neighbor
            let entry_key = context
                .left
                .as_ref()
                .and_then(|block| block.key_hash())
                .or_else(|| context.right.as_ref().and_then(|block| block.key_hash()));

            if let Some(&key) = entry_key {
                self.block.set_key_hash(Some(key))
            }
        }

        if self.parent.is_none() {
            // try to inherit parent from left/right neighbor
            let parent = context
                .left
                .as_ref()
                .map(|block| block.parent)
                .or_else(|| context.right.as_ref().map(|block| block.parent));
            if let Some(parent) = parent {
                self.block.set_parent(parent);
            }
        }

        let parent_id = *self.block.header.parent();

        // reconnect left/right + update parent map/start if necessary
        if let Some(left) = &mut context.left {
            self.block.set_right(left.right());
            left.set_right(Some(self.id()));
        } else {
            let right = if let Some(key) = self.entry_key() {
                let map_entries = db.map_entries();
                // add current block to the beginning of YMap entries
                if let Some(mut right) = map_entries.get(&parent_id, key)?.copied() {
                    if let Some(block) = block_cursor.seek(right).optional()? {
                        // move until the left-most block
                        while let Some(block) = block_cursor.left()? {
                            right = block.id;
                        }
                    }
                    Some(right)
                } else {
                    None
                }
            } else {
                if context.parent.is_none() {
                    context.parent = block_cursor.seek(parent_id).optional()?.map(BlockMut::from);
                }
                if let Some(parent) = &mut context.parent {
                    // current block is new head of the list

                    let old = parent.start().cloned();
                    parent.set_start(Some(self.id()));
                    old
                } else {
                    return Err(crate::Error::BlockNotFound(parent_id));
                }
            };
            self.block.set_right(right.as_ref());
        }

        if let Some(right) = self.block.right() {
            if context
                .right
                .as_ref()
                .map(|r| !r.contains(right))
                .unwrap_or(true)
            {
                let right = block_cursor.seek(*right).optional()?.map(BlockMut::from);
                context.right = right;
            }
            let right = context.right.as_mut().unwrap();
            right.set_left(Some(self.id()));
        } else if let Some(entry_key) = self.entry_key() {
            // set as current parent value if right === null and this is parentSub
            let map_entries = db.map_entries();
            map_entries.insert(&parent_id, entry_key, self.id())?;

            // this is the current attribute value of parent. delete right
            if let Some(left) = context.left.as_mut() {
                let parent_deleted = context
                    .parent
                    .as_ref()
                    .map(|p| p.is_deleted())
                    .unwrap_or(true);
                tx_state.delete(left, parent_deleted, &mut block_cursor, &map_entries)?;
            }
        }

        match self.block.content_type() {
            ContentType::Deleted => {
                tx_state
                    .delete_set
                    .insert(self.block.id, self.block.clock_len());
                self.block.set_deleted();
            }
            ContentType::Doc => {
                /*TODO:
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

        let content_inlined = if self.content.len() == 1 {
            self.block.set_inline_content(&self.content[0])
        } else {
            false
        };
        if !content_inlined {
            let contents = db.contents();
            contents.insert(self.block.id(), self.content.as_ref())?;
        }
        blocks.insert(self.as_block())?;

        let parent_deleted = if let Some(parent_block) = context.parent.as_mut() {
            if self.entry_key().is_none() && self.block.is_countable() && !self.block.is_deleted() {
                let parent_len = Clock::new(parent_block.node_len() as u32);
                parent_block.set_clock_len(parent_len + self.block.clock_len());
            }

            let parent = parent_block.as_block();
            let is_deleted = parent.id.is_nested() && parent.is_deleted();
            tx_state.add_changed_type(parent.id, is_deleted, self.block.key_hash());
            block_cursor.update(parent)?;
            is_deleted
        } else {
            true // parent GCed?
        };

        if parent_deleted || (self.block.key_hash().is_some() && self.block.right().is_some()) {
            // if either parent is deleted or this block is not the last block in
            // a map-like structure, delete it
            let map_entries = db.map_entries();
            tx_state.delete(
                &mut self.block,
                parent_deleted,
                &mut block_cursor,
                &map_entries,
            )?;
        }

        if let Some(right) = context.right.as_mut() {
            block_cursor.update(right.as_block())?;
        }
        if let Some(left) = context.left.as_mut() {
            block_cursor.update(left.as_block())?;
        }

        Ok(())
    }
}

impl Display for InsertBlockData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, {}", self.block.id, self.block.header)?;
        for content in self.content() {
            write!(f, " {}", content)?;
        }
        Ok(())
    }
}

impl Debug for InsertBlockData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

pub type ParseError<'a> = CastError<&'a [u8], BlockHeader>;
pub type ParseMutError<'a> = CastError<&'a mut [u8], BlockHeader>;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
pub struct BlockFlags(u8);

bitflags! {
    impl BlockFlags : u8 {
        /// If this flag is set, it means that the content has been inlined directly inside of
        /// the [BlockHeader] itself.
        const INLINE_CONTENT = 0b0000_0001;
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

impl Debug for BlockFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for flag in self.iter() {
            write!(f, "|")?;
            match flag {
                Self::INLINE_CONTENT => f.write_str("INLINE_CONTENT")?,
                Self::COUNTABLE => f.write_str("COUNTABLE")?,
                Self::DELETED => f.write_str("DELETED")?,
                Self::MARKED => f.write_str("MARKED")?,
                Self::RIGHT => f.write_str("RIGHT")?,
                Self::LEFT => f.write_str("LEFT")?,
                Self::ORIGIN_RIGHT => f.write_str("ORIGIN_RIGHT")?,
                Self::ORIGIN_LEFT => f.write_str("ORIGIN_LEFT")?,
                _ => write!(f, "{:?}", flag)?,
            }
        }
        Ok(())
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

impl Debug for BlockHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for BlockHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "parent: {}", self.parent)?;
        if let Some(key) = self.key_hash() {
            write!(f, ", hash_key: {}", key)?;
        }
        if self.flags.contains(BlockFlags::COUNTABLE) || self.flags.contains(BlockFlags::DELETED) {
            write!(f, ", clock-len: {}", self.len)?;
        }
        if self.flags.contains(BlockFlags::LEFT) {
            write!(f, ", left: {}", self.left)?;
        }
        if self.flags.contains(BlockFlags::RIGHT) {
            write!(f, ", right: {}", self.right)?;
        }
        if self.flags.contains(BlockFlags::ORIGIN_LEFT) {
            write!(f, ", origin-l: {}", self.origin_left)?;
        }
        if self.flags.contains(BlockFlags::ORIGIN_RIGHT) {
            write!(f, ", origin-r: {}", self.origin_right)?;
        }
        if let Some(content) = self.try_inline_content() {
            write!(f, ", start: {}", content)?;
        }
        write!(f, " - {}", self.content_type())?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::block::{ID, InsertBlockData};
    use crate::content::{Content, ContentType};
    use crate::node::{Node, NodeID};
    use crate::{BlockHeader, BlockMut, ClientID, Clock, lib0};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use smallvec::smallvec;

    const CLIENT: ClientID = unsafe { ClientID::new_unchecked(123) };
    const PARENT: Node = Node::nested(ID::new(CLIENT, Clock::new(0)));

    #[test]
    fn block_header_size() {
        assert_eq!(size_of::<BlockHeader>(), 60);
    }

    #[test]
    fn id_serialize() {
        let id = ID::new(123.into(), 42.into());
        let serialized = serde_json::to_string(&id).unwrap();
        assert_eq!(serialized, r#"[123,42]"#);
        let deserialized: ID = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, id);
    }

    #[test]
    fn block_set_header_values() {
        let id = ID::new(CLIENT, 1.into());
        let left = ID::new(CLIENT, 3.into());
        let o_left = ID::new(CLIENT, 13.into());
        let right = ID::new(CLIENT, 4.into());
        let o_right = ID::new(CLIENT, 4.into());

        let insert = block(1, 2, 3, 4, 13, 4, Some("key"), Content::str(&"hello"));

        assert_eq!(insert.block.left(), Some(&left));
        assert_eq!(insert.block.right(), Some(&right));
        assert_eq!(insert.block.origin_left(), Some(&o_left));
        assert_eq!(insert.block.origin_right(), Some(&o_right));
        assert_eq!(insert.block.parent(), &PARENT.id());

        assert_eq!(insert.block.clock_len(), Clock::new(2));
        let content = &insert.block.try_inline_content().unwrap();
        assert_eq!(content.len(), 5);
        assert_eq!(content.content_type(), ContentType::String);
        assert_eq!(insert.entry_key(), Some("key"));
    }

    #[test]
    fn block_set_key_shorter() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"), Content::str(&"hello world"));

        block.set_entry_key("123".as_bytes());

        assert_eq!(block.entry_key(), Some("123"));
        let content = &block.content()[0];
        assert_eq!(content, &Content::str(&"hello world"));
    }

    #[test]
    fn block_set_key_longer() {
        let mut block = block(
            1,
            3,
            0,
            4,
            0,
            4,
            Some("test"),
            Content::string("hello world"),
        );

        block.set_entry_key("test123".as_bytes());

        assert_eq!(block.entry_key(), Some("test123"));
        let content = &block.content()[0];
        assert_eq!(content, &Content::string("hello world"));
    }

    #[test]
    fn block_set_key_equal() {
        let mut block = block(
            1,
            3,
            0,
            4,
            0,
            4,
            Some("test"),
            Content::string("hello world"),
        );

        block.set_entry_key("1234".as_bytes());

        assert_eq!(block.entry_key(), Some("1234"));
        let content = &block.content()[0];
        assert_eq!(content, &Content::string("hello world"));
    }

    #[test]
    fn block_split_inline_text() {
        let parent: NodeID = Node::root_named("parent").id();
        let header = BlockHeader::new(
            7.into(),
            Some(&ID::new(CLIENT, 4.into())),
            Some(&ID::new(CLIENT, 16.into())),
            Some(&ID::new(CLIENT, 4.into())),
            Some(&ID::new(CLIENT, 16.into())),
            parent,
            None,
        );
        let mut left = BlockMut::new(ID::new(1.into(), 5.into()), header);
        assert!(left.set_inline_content(&Content::str(&"hello w")));

        let right = left.split(5.into()).unwrap();

        assert_eq!(left.clock_len(), Clock::new(5));
        assert_eq!(left.content_type(), ContentType::String);
        assert_eq!(left.try_inline_data().unwrap(), b"hello");
        assert_eq!(left.left(), Some(&ID::new(CLIENT, 4.into())));
        assert_eq!(left.right(), Some(&ID::new(1.into(), 10.into())));

        assert_eq!(right.clock_len(), Clock::new(2));
        assert_eq!(right.content_type(), ContentType::String);
        assert_eq!(right.try_inline_data().unwrap(), b" w");
        assert_eq!(right.left(), Some(&ID::new(1.into(), 9.into())));
        assert_eq!(right.right(), Some(&ID::new(CLIENT, 16.into())));
    }

    #[test]
    fn block_merge_inline_text() {
        let parent: NodeID = Node::root_named("parent").id();
        let mut left = BlockMut::new(
            ID::new(CLIENT, 5.into()),
            BlockHeader::new(
                5.into(),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 10.into())),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 16.into())),
                parent,
                None,
            ),
        );
        left.set_inline_content(&Content::str(&"hello"));
        let mut right = BlockMut::new(
            ID::new(CLIENT, 10.into()),
            BlockHeader::new(
                2.into(),
                Some(&ID::new(CLIENT, 9.into())),
                Some(&ID::new(CLIENT, 16.into())),
                Some(&ID::new(CLIENT, 9.into())),
                Some(&ID::new(CLIENT, 16.into())),
                parent,
                None,
            ),
        );
        right.set_inline_content(&Content::str(&" w"));

        assert!(left.merge(right.as_block()));
        assert_eq!(left.clock_len(), Clock::new(7));
        assert_eq!(left.try_inline_data().unwrap(), b"hello w");
        assert_eq!(left.left(), Some(&ID::new(CLIENT, 4.into())));
        assert_eq!(left.right(), Some(&ID::new(CLIENT, 16.into())));
    }

    #[test]
    fn block_split_deleted() {
        let parent: NodeID = Node::root_named("parent").id();
        let mut left = BlockMut::new(
            ID::new(CLIENT, 5.into()),
            BlockHeader::new(
                5.into(),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 10.into())),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 10.into())),
                parent,
                None,
            ),
        );
        left.set_content_type(ContentType::Deleted);

        let right = left.split(3.into()).unwrap();

        let mut expected = BlockMut::new(
            ID::new(CLIENT, 5.into()),
            BlockHeader::new(
                3.into(),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 8.into())),
                Some(&ID::new(CLIENT, 4.into())),
                Some(&ID::new(CLIENT, 10.into())),
                parent,
                None,
            ),
        );
        expected.set_deleted();
        assert_eq!(left, expected);
        let mut expected = BlockMut::new(
            ID::new(CLIENT, 8.into()),
            BlockHeader::new(
                2.into(),
                Some(&ID::new(CLIENT, 7.into())),
                Some(&ID::new(CLIENT, 10.into())),
                Some(&ID::new(CLIENT, 7.into())),
                Some(&ID::new(CLIENT, 10.into())),
                parent,
                None,
            ),
        );
        expected.set_deleted();
        assert_eq!(right, expected);
    }

    #[test]
    fn block_merge_deleted() {
        let parent: NodeID = Node::root_named("parent").id();
        let mut block = BlockMut::new(
            ID::new(CLIENT, 1.into()),
            BlockHeader::new(
                11.into(),
                Some(&ID::new(CLIENT, 12.into())),
                Some(&ID::new(CLIENT, 13.into())),
                Some(&ID::new(CLIENT, 14.into())),
                Some(&ID::new(CLIENT, 15.into())),
                parent,
                None,
            ),
        );
        block.set_content_type(ContentType::Deleted);

        let expected = block.clone();

        let right = block.split(6.into()).unwrap();
        assert!(block.merge(right.as_block()));

        assert_eq!(block, expected);
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
        content: Content<'static>,
    ) -> InsertBlockData {
        let mut insert = InsertBlockData::new(
            ID::new(CLIENT, id.into()),
            len.into(),
            Some(&ID::new(CLIENT, left.into())),
            Some(&ID::new(CLIENT, right.into())),
            Some(&ID::new(CLIENT, origin_left.into())),
            Some(&ID::new(CLIENT, origin_right.into())),
            PARENT,
            entry,
        );
        insert.block.set_content_type(content.content_type());
        if content.len() <= 8 {
            insert.block.set_inline_content(&content);
        } else {
            insert.content = smallvec![content];
        }
        insert
    }
}
