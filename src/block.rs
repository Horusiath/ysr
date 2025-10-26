use crate::block_cursor::BlockCursor;
use crate::content::{BlockContent, ContentIter, ContentType};
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeID, NodeType};
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::transaction::TransactionState;
use crate::{ClientID, Clock, Optional, U32};
use crate::{Error, Result};
use bitflags::bitflags;
use bytes::{Bytes, BytesMut};
use lmdb_rs_m::Database;
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
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
    clock_len: Clock,
    /// Flags that define the block's state, including presence/absence of other fields like
    /// neighbor blocks or origins.
    flags: BlockFlags,
    /// Flags that define the block's content type.
    content_type: ContentType,
    /// Used only when [ContentType::Node] is set. Defines the type of the node.
    node_type: NodeType,
    _padding: [u8; 1], // not used atm. we could use it to i.e. navigate XML element tags
    /// NodeID of the parent node collection that contains this block.
    parent: NodeID,
    /// XX Hash of the key if provided, 0 otherwise.
    key_hash: U32,
    /// ID of the left neighbor block (if such exists).
    left: ID,
    /// ID of the right neighbor block (if such exists).
    right: ID,
    /// ID of the left neighbor block at the point of insertion (if such existed).
    origin_left: ID,
    /// ID of the right neighbor block at the point of insertion (if such existed).
    origin_right: ID,
    /// Used only when [ContentType::Node] is set for list-like block. Defines the first block.
    start: ID,
}
impl BlockHeader {
    pub const SIZE: usize = size_of::<BlockHeader>();

    pub fn empty() -> Self {
        BlockHeader {
            clock_len: Clock::new(0),
            flags: BlockFlags::default(),
            content_type: ContentType::Deleted,
            node_type: NodeType::default(),
            _padding: [0; 1],
            parent: NodeID::default(),
            key_hash: U32::new(0),
            left: ID::default(),
            right: ID::default(),
            origin_left: ID::default(),
            origin_right: ID::default(),
            start: ID::default(),
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
            clock_len: len,
            flags,
            content_type: ContentType::Deleted,
            node_type: NodeType::Unknown,
            _padding: [0; 1],
            parent,
            key_hash,
            left: left.copied().unwrap_or_default(),
            right: right.copied().unwrap_or_default(),
            origin_left: origin_left.copied().unwrap_or_default(),
            origin_right: origin_right.copied().unwrap_or_default(),
            start: ID::new(ClientID::default(), Clock::new(0)),
        }
    }

    pub fn flags(&self) -> BlockFlags {
        self.flags
    }

    pub fn content_type(&self) -> ContentType {
        self.content_type
    }

    pub fn contains(&self, id: &ID) -> bool {
        id.client == self.left.client // same client
            && id.clock >= self.left.clock // id is larger or equal to block's start clock
            && id.clock < self.left.clock + self.clock_len // id is smaller than block's end clock
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
        if self.flags.contains(BlockFlags::HAS_START) {
            Some(&self.start)
        } else {
            None
        }
    }

    pub fn set_start(&mut self, id: Option<&ID>) {
        match id {
            Some(id) => {
                self.flags |= BlockFlags::HAS_START;
                self.start = *id;
            }
            None => {
                self.flags -= BlockFlags::HAS_START;
            }
        }
    }

    pub fn node_type(&self) -> Option<&NodeType> {
        if self.content_type == ContentType::Node {
            Some(&self.node_type)
        } else {
            None
        }
    }

    pub fn set_node_type(&mut self, node_type: NodeType) {
        if self.content_type != ContentType::Node {
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
        if matches!(self.content_type, ContentType::Deleted) {
            self.flags |= BlockFlags::DELETED;
        } else {
            self.flags -= BlockFlags::DELETED;
        }
    }

    #[inline]
    pub fn clock_len(&self) -> Clock {
        self.clock_len
    }

    pub fn len(&self) -> Clock {
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
        if offset == 0 || offset > self.clock_len || !(self.is_countable() || self.is_deleted()) {
            None
        } else {
            let clock_len = self.clock_len;
            self.clock_len = offset;

            let mut flags = self.flags;
            flags |= BlockFlags::ORIGIN_LEFT;
            flags |= BlockFlags::LEFT;
            let left = ID::new(self.id.client, self.id.clock + offset - 1);
            let right = self.right;

            self.right = ID::new(self.id.client, self.id.clock + offset);
            self.flags |= BlockFlags::RIGHT;

            let right = BlockHeader {
                clock_len: clock_len - offset,
                flags,
                content_type: self.content_type,
                node_type: Default::default(),
                _padding: [0; 1],
                parent: self.parent,
                key_hash: self.key_hash,
                left,
                right,
                origin_left: left,
                origin_right: self.origin_right,
                start: Default::default(), // nodes are not splittable
            };
            Some(Self::new(self.right, right))
        }
    }

    pub fn merge(&mut self, other: Block<'_>) -> bool {
        if self.can_merge(&other) {
            self.clock_len += other.clock_len;

            self.set_right(other.right());

            // other.right.left points to the last id, so we don't need to update it
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
            && self.content_type == other.content_type
            && self.content_type.is_mergeable()
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
    pub content: BytesMut,
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
            content: Default::default(),
        }
    }

    pub fn content(&self) -> crate::Result<BlockContent<'_>> {
        BlockContent::new(self.block.content_type(), &self.content)
    }

    pub(crate) fn new_node(node: Node, kind: NodeType) -> Self {
        let id = node.id();
        Self {
            block: BlockMut::new(
                id,
                BlockHeader {
                    clock_len: 1.into(),
                    flags: BlockFlags::COUNTABLE,
                    content_type: ContentType::Node,
                    node_type: kind,
                    _padding: [0; 1],
                    parent: id,
                    key_hash: Default::default(),
                    left: Default::default(),
                    right: Default::default(),
                    origin_left: Default::default(),
                    origin_right: Default::default(),
                    start: Default::default(),
                },
            ),
            parent: None,
            entry: None,
            content: Default::default(),
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

    pub(crate) fn add_content(&mut self, content: BlockContent) {
        let bytes = content.body();
        self.content.extend(bytes);
    }

    pub fn merge(&mut self, other: Self) -> bool {
        if self.block.merge(other.block.as_block()) {
            // contents are mergeable through simple byte concatenation
            self.content.extend_from_slice(&other.content);
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

    pub fn split(&mut self, offset: Clock) -> Option<Self> {
        let new_block = self.block.split(offset)?;

        let new_content = {
            let mut offset = offset.get() as usize;
            match new_block.content_type {
                ContentType::String => {
                    let str = unsafe { std::str::from_utf8_unchecked(&self.content) };
                    let mut byte_offset = 0;
                    for c in str.chars() {
                        if offset == 0 {
                            break;
                        }
                        offset -= 1;
                        let utf8_len = c.len_utf8();
                        byte_offset += utf8_len;
                    }
                    let (_, right) = self.content.split_at(byte_offset);
                    let new_content = BytesMut::from(right);
                    self.content.truncate(byte_offset);
                    new_content
                }
                ContentType::Atom | ContentType::Json => {
                    let content_iter = ContentIter::new(&self.content);
                    if let Some(right) = content_iter.slice(offset) {
                        let new_content = BytesMut::from(right);
                        let byte_offset = self.content.len() - new_content.len();
                        self.content.truncate(byte_offset);
                        new_content
                    } else {
                        BytesMut::new()
                    }
                }
                _ => BytesMut::new(),
            }
        };

        Some(Self {
            block: new_block,
            content: new_content,
            parent: self.parent.clone(),
            entry: self.entry.clone(),
        })
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

            self.block.id.clock += context.offset;
            let left =
                match db.split_block(ID::new(self.block.id.client, self.block.id.clock - 1))? {
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
                // add current block to the beginning of YMap entries
                let mut right = *db.entry(parent_id, key)?;
                let mut cursor = BlockCursor::new(db.new_cursor()?);
                if let Some(()) = cursor.seek(right).optional()? {
                    // move until the left-most block
                    while let Some(block) = cursor.next_left().optional()? {
                        right = block.id;
                    }
                }
                Some(right)
            } else {
                if context.parent.is_none() {
                    context.parent = Some(db.fetch_block(parent_id, true)?.into());
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
                let right = db.fetch_block(*right, true)?;
                context.right = Some(right.into());
            }
            let right = context.right.as_mut().unwrap();
            right.set_left(Some(self.id()));
        } else if let Some(entry_key) = self.entry_key() {
            // set as current parent value if right === null and this is parentSub
            db.set_entry(parent_id, entry_key, self.id())?;

            // this is the current attribute value of parent. delete right
            if let Some(left) = context.left.as_mut() {
                let parent_deleted = context
                    .parent
                    .as_ref()
                    .map(|p| p.is_deleted())
                    .unwrap_or(true);
                tx_state.delete(db, left, parent_deleted)?;
            }
        }

        if self.entry_key().is_none() && !self.block.is_deleted() {
            //TODO: adjust parent length
            //TODO: linked type joining
        }

        //TODO: check if this item is in a moved range and merge moves

        match self.content()? {
            BlockContent::Deleted => {
                tx_state
                    .delete_set
                    .insert(self.block.id, self.block.clock_len());
                self.block.set_deleted();
            }
            BlockContent::Doc(doc_id) => {
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

        db.insert_block(self)?;

        let parent_deleted = if let Some(parent_block) = context.parent.as_mut() {
            let parent = parent_block.as_block();
            let is_deleted = parent.id.is_nested() && parent.is_deleted();
            tx_state.add_changed_type(parent.id, is_deleted, self.block.key_hash());
            db.update_block(parent)?;
            is_deleted
        } else {
            true // parent GCed?
        };

        if parent_deleted || (self.block.key_hash().is_some() && self.block.right().is_some()) {
            // if either parent is deleted or this block is not the last block in
            // a map-like structure, delete it
            tx_state.delete(db, &mut self.block, parent_deleted)?;
        }

        if let Some(right) = context.right.as_mut() {
            db.update_block(right.as_block())?;
        }
        if let Some(left) = context.left.as_mut() {
            db.update_block(left.as_block())?;
        }

        Ok(())
    }

    pub(crate) fn init_content(&mut self, content: BlockContent) {
        self.block.set_content_type(content.content_type());
        self.content = BytesMut::from(content.body());
    }
}

impl Display for InsertBlockData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let content = self.content().map_err(|_| std::fmt::Error)?;
        write!(f, "{}, {} {}", self.block.id, self.block.header, content)
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
        /// Only used at decoding phase.
        const HAS_START = 0b0000_0001;
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

impl Debug for BlockHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for BlockHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "parent: {}", self.parent)?;
        if let Some(key) = self.key_hash() {
            write!(f, ", hash_key: \"{}\"", key)?;
        }
        if self.flags.contains(BlockFlags::COUNTABLE) || self.flags.contains(BlockFlags::DELETED) {
            write!(f, ", clock-len: {}", self.clock_len)?;
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
        if self.flags.contains(BlockFlags::HAS_START) {
            write!(f, ", start: {}", self.start)?;
        }
        write!(f, " - {}", self.content_type)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::block::{InsertBlockData, ID};
    use crate::content::{BlockContent, ContentRef, ContentType};
    use crate::node::Node;
    use crate::{ClientID, Clock};
    use bytes::{BufMut, BytesMut};
    use serde::{Deserialize, Serialize};
    use zerocopy::IntoBytes;

    const CLIENT: ClientID = unsafe { ClientID::new_unchecked(123) };
    const PARENT: Node = Node::nested(ID::new(CLIENT, Clock::new(0)));

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

        let mut insert = block(1, 2, 3, 4, 13, 4, Some("key"));
        insert.block.set_content_type(ContentType::Deleted);

        assert_eq!(insert.block.left(), Some(&left));
        assert_eq!(insert.block.right(), Some(&right));
        assert_eq!(insert.block.origin_left(), Some(&o_left));
        assert_eq!(insert.block.origin_right(), Some(&o_right));
        assert_eq!(insert.parent, Some(PARENT.clone()));

        assert_eq!(insert.clock_len(), Clock::new(2));
        let content = insert.content().unwrap();
        assert_eq!(content.content_type(), ContentType::Deleted);
        assert_eq!(insert.entry_key(), Some("key"));
    }

    #[test]
    fn block_set_key_shorter() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block.init_content(BlockContent::Text("hello world"));

        block.set_entry_key("123".as_bytes());

        assert_eq!(block.entry_key(), Some("123"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_set_key_longer() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block.init_content(BlockContent::Text("hello world"));

        block.set_entry_key("test123".as_bytes());

        assert_eq!(block.entry_key(), Some("test123"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_set_key_equal() {
        let mut block = block(1, 3, 0, 4, 0, 4, Some("test"));
        block.init_content(BlockContent::Text("hello world"));

        block.set_entry_key("1234".as_bytes());

        assert_eq!(block.entry_key(), Some("1234"));
        let content = block.content().unwrap();
        assert_eq!(content, BlockContent::Text("hello world"));
    }

    #[test]
    fn block_split_text() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Text("hello world"));

        let right = b.split(6.into()).unwrap();
        let mut expected_right = block(7, 5, 6, 13, 6, 15, Some("key"));
        expected_right.init_content(BlockContent::Text("world"));
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 6, 12, 7, 14, 15, Some("key"));
        expected_left.init_content(BlockContent::Text("hello "));
        assert_eq!(b, expected_left);
    }

    #[test]
    fn block_merge_text() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Text("hello world"));

        let expected = b.clone();

        let right = b.split(6.into()).unwrap();
        assert!(b.merge(right));

        assert_eq!(b, expected);
    }

    #[test]
    fn block_split_deleted() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Deleted);

        let right = b.split(6.into()).unwrap();
        let mut expected_right = block(7, 5, 6, 13, 6, 15, Some("key"));
        expected_right.init_content(BlockContent::Deleted);
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 6, 12, 7, 14, 15, Some("key"));
        expected_left.init_content(BlockContent::Deleted);
        assert_eq!(b, expected_left);
    }

    #[test]
    fn block_merge_deleted() {
        let mut b = block(1, 11, 12, 13, 14, 15, Some("key"));
        b.init_content(BlockContent::Deleted);

        let expected = b.clone();

        let right = b.split(6.into()).unwrap();
        assert!(b.merge(right));

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
        b.init_content(BlockContent::Atom(ContentRef::new(&buf)));

        let right = b.split(1.into()).unwrap();
        let mut expected_right = block(2, 1, 1, 3, 1, 5, Some("aa"));
        expected_right.init_content(BlockContent::Atom(ContentRef::new(&buf[4 + alice.len()..])));
        assert_eq!(right, expected_right);

        let mut expected_left = block(1, 1, 0, 2, 4, 5, Some("aa"));
        expected_left.init_content(BlockContent::Atom(ContentRef::new(&buf[..4 + alice.len()])));
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
        b.init_content(BlockContent::Atom(ContentRef::new(&buf)));

        let expected = b.clone();

        let right = b.split(1.into()).unwrap();
        assert!(b.merge(right));

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
    ) -> InsertBlockData {
        InsertBlockData::new(
            ID::new(CLIENT, id.into()),
            len.into(),
            Some(&ID::new(CLIENT, left.into())),
            Some(&ID::new(CLIENT, right.into())),
            Some(&ID::new(CLIENT, origin_left.into())),
            Some(&ID::new(CLIENT, origin_right.into())),
            PARENT,
            entry,
        )
    }
}
