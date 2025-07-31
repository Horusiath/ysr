use crate::content::{BlockContent, ContentFormat, ContentIter, ContentRef, ContentType};
use crate::node::{NodeHeader, NodeID};
use crate::store::lmdb::store::SplitResult;
use crate::store::lmdb::BlockStore;
use crate::transaction::TransactionState;
use crate::{ClientID, Clock};
use crate::{Error, Result};
use bitflags::bitflags;
use bytes::BytesMut;
use lmdb_rs_m::Database;
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
            let str = unsafe { std::str::from_utf8_unchecked(&body[..self.key_len as usize]) };
            Some(str)
        }
    }

    pub fn parent(&self) -> &NodeID {
        &self.parent
    }

    #[inline]
    pub fn set_parent(&mut self, parent_id: NodeID) {
        self.parent = parent_id;
    }

    pub fn content_slice<'a>(&self, body: &'a [u8]) -> &'a [u8] {
        &body[self.key_len as usize..]
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

#[derive(Clone)]
pub struct BlockMut {
    id: ID,
    body: BytesMut,
}

impl BlockMut {
    pub(crate) fn empty(id: ID) -> Self {
        let header = BlockHeader::default();
        let mut bytes = BytesMut::with_capacity(BlockHeader::SIZE);
        bytes.extend_from_slice(header.as_bytes());
        BlockMut { id, body: bytes }
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

    fn split(&mut self, offset: Clock, into: &mut BlockMut) -> crate::Result<()> {
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

    pub fn merge(&mut self, other: &BlockMut) -> bool {
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
        offset: Clock,
    ) -> crate::Result<()> {
        if offset > 0 {
            // offset could be > 0 only in context of Update::integrate,
            // is such case offset kind in use always means Yjs-compatible offset (utf-16)
            self.id.clock += offset;
            let left = match db.split_block(ID::new(self.id.client, self.id.clock - 1))? {
                SplitResult::Unchanged(left) => left.last_id(),
                SplitResult::Split(left, _) => left.last_id(),
            };
            self.set_left(Some(&left));
            self.set_origin_left(left);
        }
        todo!()
        /*
        let self_ptr = self.clone();
        let this = self.deref_mut();
        let store = txn.doc_mut();
        let encoding = store.options.offset_kind;
        if offset > 0 {
            // offset could be > 0 only in context of Update::integrate,
            // is such case offset kind in use always means Yjs-compatible offset (utf-16)
            this.id.clock += offset;
            this.left = store
                .blocks
                .get_item_clean_end(&ID::new(this.id.client, this.id.clock - 1))
                .map(|slice| store.materialize(slice));
            this.origin = this.left.as_deref().map(|b: &Item| b.last_id());
            this.content = this
                .content
                .splice(offset as usize, OffsetKind::Utf16)
                .unwrap();
            this.len -= offset;
        }

        let parent = match &this.parent {
            TypePtr::Branch(branch) => Some(*branch),
            TypePtr::Named(name) => {
                let branch = store.get_or_create_type(name.clone(), TypeRef::Undefined);
                this.parent = TypePtr::Branch(branch);
                Some(branch)
            }
            TypePtr::ID(id) => {
                if let Some(item) = store.blocks.get_item(id) {
                    if let Some(branch) = item.as_branch() {
                        this.parent = TypePtr::Branch(branch);
                        Some(branch)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            TypePtr::Unknown => return true,
        };

        let left: Option<&Item> = this.left.as_deref();
        let right: Option<&Item> = this.right.as_deref();

        let right_is_null_or_has_left = match right {
            None => true,
            Some(i) => i.left.is_some(),
        };
        let left_has_other_right_than_self = match left {
            Some(i) => i.right != this.right,
            _ => false,
        };

        if let Some(mut parent_ref) = parent {
            if (left.is_none() && right_is_null_or_has_left) || left_has_other_right_than_self {
                // set the first conflicting item
                let mut o = if let Some(left) = left {
                    left.right
                } else if let Some(sub) = &this.parent_sub {
                    let mut o = parent_ref.map.get(sub).cloned();
                    while let Some(item) = o.as_deref() {
                        if item.left.is_some() {
                            o = item.left.clone();
                            continue;
                        }
                        break;
                    }
                    o.clone()
                } else {
                    parent_ref.start
                };

                let mut left = this.left.clone();
                let mut conflicting_items = HashSet::new();
                let mut items_before_origin = HashSet::new();

                // Let c in conflicting_items, b in items_before_origin
                // ***{origin}bbbb{this}{c,b}{c,b}{o}***
                // Note that conflicting_items is a subset of items_before_origin
                while let Some(item) = o {
                    if Some(item) == this.right {
                        break;
                    }

                    items_before_origin.insert(item);
                    conflicting_items.insert(item);
                    if this.origin == item.origin {
                        // case 1
                        if item.id.client < this.id.client {
                            left = Some(item.clone());
                            conflicting_items.clear();
                        } else if this.right_origin == item.right_origin {
                            // `self` and `item` are conflicting and point to the same integration
                            // points. The id decides which item comes first. Since `self` is to
                            // the left of `item`, we can break here.
                            break;
                        }
                    } else {
                        if let Some(origin_ptr) = item
                            .origin
                            .as_ref()
                            .and_then(|id| store.blocks.get_item(id))
                        {
                            if items_before_origin.contains(&origin_ptr) {
                                if !conflicting_items.contains(&origin_ptr) {
                                    left = Some(item.clone());
                                    conflicting_items.clear();
                                }
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    o = item.right.clone();
                }
                this.left = left;
            }

            if this.parent_sub.is_none() {
                if let Some(item) = this.left.as_deref() {
                    if item.parent_sub.is_some() {
                        this.parent_sub = item.parent_sub.clone();
                    } else if let Some(item) = this.right.as_deref() {
                        this.parent_sub = item.parent_sub.clone();
                    }
                }
            }

            // reconnect left/right
            if let Some(left) = this.left.as_deref_mut() {
                this.right = left.right.replace(self_ptr);
            } else {
                let r = if let Some(parent_sub) = &this.parent_sub {
                    // update parent map/start if necessary
                    let mut r = parent_ref.map.get(parent_sub).cloned();
                    while let Some(item) = r {
                        if item.left.is_some() {
                            r = item.left;
                        } else {
                            break;
                        }
                    }
                    r
                } else {
                    let start = parent_ref.start.replace(self_ptr);
                    start
                };
                this.right = r;
            }

            if let Some(right) = this.right.as_deref_mut() {
                right.left = Some(self_ptr);
            } else if let Some(parent_sub) = &this.parent_sub {
                // set as current parent value if right === null and this is parentSub
                parent_ref.map.insert(parent_sub.clone(), self_ptr);
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
            }

            // adjust length of parent
            if this.parent_sub.is_none() && !this.is_deleted() {
                if this.is_countable() {
                    // adjust length of parent
                    parent_ref.block_len += this.len;
                    parent_ref.content_len += this.content_len(encoding);
                }
                #[cfg(feature = "weak")]
                match (this.left, this.right) {
                    (Some(l), Some(r)) if l.info.is_linked() || r.info.is_linked() => {
                        crate::types::weak::join_linked_range(self_ptr, txn)
                    }
                    _ => {}
                }
            }

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

            match &mut this.content {
                ItemContent::Deleted(len) => {
                    state.delete_set.insert(this.id, *len);
                    this.mark_as_deleted();
                }
                ItemContent::Move(m) => m.integrate_block(doc, state, self_ptr),
                ItemContent::Doc(subdoc) => {
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
                }
                ItemContent::Format(_, _) => {
                    // @todo searchmarker are currently unsupported for rich text documents
                    // /** @type {AbstractType<any>} */ (item.parent)._searchMarker = null
                }
                #[cfg(feature = "weak")]
                ItemContent::Type(branch) => {
                    let ptr = BranchPtr::from(branch);
                    if let TypeRef::WeakLink(source) = &ptr.type_ref {
                        source.materialize(doc, ptr);
                    }
                }
                _ => {
                    // other types don't define integration-specific actions
                }
            }
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
    }
}

impl<'a> From<Block<'a>> for BlockMut {
    fn from(value: Block<'a>) -> Self {
        let mut body = BytesMut::with_capacity(value.data.len());
        body.extend_from_slice(value.data);
        Self::parse(*value.id(), body).unwrap()
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
        BlockHeader::mut_from_bytes(&mut self.body[..BlockHeader::SIZE]).unwrap()
    }
}

impl Display for BlockMut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        write!(f, "{}, {}", self.id, header.display(body))
    }
}

impl Debug for BlockMut {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Eq for BlockMut {}
impl PartialEq for BlockMut {
    fn eq(&self, other: &Self) -> bool {
        let (header, body) = BlockHeader::parse(&self.body).unwrap();
        let (other_header, other_body) = BlockHeader::parse(&other.body).unwrap();
        header == other_header && body == other_body
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
#[derive(PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
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
    use crate::content::{BlockContent, ContentRef, ContentType};
    use crate::node::NodeID;
    use crate::{ClientID, Clock};
    use bytes::{BufMut, BytesMut};
    use serde::{Deserialize, Serialize};
    use zerocopy::IntoBytes;

    const CLIENT: ClientID = unsafe { ClientID::new_unchecked(123) };
    const PARENT: NodeID = NodeID::from_nested(ID::new(CLIENT, Clock::new(0)));

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
    ) -> BlockMut {
        BlockMut::new(
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
