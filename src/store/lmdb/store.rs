use crate::block::{Block, BlockMut, InsertBlockData, ID};
use crate::block_reader::Carrier;
use crate::content::{BlockContentRef, ContentType};
use crate::id_set::IDSet;
use crate::node::{Named, Node, NodeID, NodeType};
use crate::store::lmdb::inspect::DocInspector;
use crate::transaction::TransactionState;
use crate::{ClientID, Clock, Error, Optional, StateVector, U32};
use lmdb_rs_m::core::MdbResult;
use lmdb_rs_m::{Cursor, Database, MdbError, MdbValue, ToMdbValue};
use smallvec::{smallvec, ExtendFromSlice, SmallVec};
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Deref, DerefMut};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub trait BlockStore<'tx> {
    fn cursor(&self) -> crate::Result<BlockCursor<'_>>;
    fn fetch_block(&self, id: ID, direct_only: bool) -> crate::Result<Block<'_>>;
    fn insert_block(&mut self, builder: &InsertBlockData) -> crate::Result<()>;
    fn update_block(&mut self, block: Block) -> crate::Result<()>;
    fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock>;
    fn split_block(&self, id: ID) -> crate::Result<SplitResult>;
    fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>>;
    fn state_vector(&self) -> crate::Result<StateVector>;

    fn block_content(&self, id: ID, kind: ContentType) -> crate::Result<BlockContentRef<'_>>;
    fn set_block_content(&mut self, id: ID, content: &BlockContentRef) -> crate::Result<()>;

    fn entry(&self, map: NodeID, entry_key: &str) -> crate::Result<&ID>;
    fn set_entry(&mut self, map: NodeID, entry_key: &str, value: &ID) -> crate::Result<()>;

    fn intern_string(&mut self, string: &str, alias: U32) -> crate::Result<()>;

    fn insert_pending_update(
        &mut self,
        missing_sv: &StateVector,
        remaining: &BTreeMap<ClientID, VecDeque<Carrier>>,
        pending_delete_set: &IDSet,
    ) -> crate::Result<()>;

    fn get_or_insert_node(
        &mut self,
        node: Node<'_>,
        node_type: NodeType,
    ) -> crate::Result<BlockMut> {
        match self.fetch_block(node.id(), true) {
            Ok(block) => Ok(block.into()),
            Err(crate::Error::BlockNotFound(_)) => {
                if let Node::Root(Named::Name(name)) = &node {
                    // since root nodes live forever, we can create it if it does not exist
                    let data = InsertBlockData::new_node(&node, node_type);
                    self.insert_block(&data)?;
                    self.intern_string(name, data.id().clock)?;
                    Ok(data.block)
                } else {
                    // nested nodes are not created automatically, if we didn't find it, we return an error
                    Err(crate::Error::NotFound)
                }
            }
            Err(e) => Err(e),
        }
    }

    fn inspect(&self) -> crate::Result<DocInspector<'_>>;
}

impl<'tx> BlockStore<'tx> for Database<'tx> {
    fn cursor(&self) -> crate::Result<BlockCursor<'_>> {
        let cursor = self.new_cursor()?;
        Ok(BlockCursor::from(cursor))
    }

    /// Returns the block which contains the given ID.
    /// If `direct_only` is true, it will only search for blocks that starts with the given ID.
    /// If `direct_only` is false, it will search for blocks that contain the ID anywhere within
    /// their range.
    fn fetch_block(&self, id: ID, direct_only: bool) -> crate::Result<Block<'_>> {
        let mut cursor = self.cursor()?;
        if let Some(_) = cursor.seek(id, direct_only)? {
            let block = cursor.block()?.unwrap();
            Ok(block)
        } else {
            Err(crate::Error::BlockNotFound(id))
        }
    }

    /// Inserts a block into the store, updating the state vector as necessary.
    fn insert_block(&mut self, insert: &InsertBlockData) -> crate::Result<()> {
        // insert block metadata
        self.set(&BlockKey::new(*insert.id()), &insert.as_block().as_bytes())?;
        self.try_update_clock(insert.last_id())?;

        // insert block content if any
        if !insert.content.is_empty() {
            let mut id = *insert.id();
            for content in insert.content.items() {
                self.set(&BlockContentKey::new(id), &content.as_bytes())?;
                id.clock += 1;
            }
        }
        // insert block entry key if any
        if let Some(key) = insert.entry.as_deref() {
            let key = unsafe { str::from_utf8_unchecked(key) };
            if let Some(parent) = insert.parent() {
                self.set_entry(parent.id(), key, insert.id())?;
            } else {
                return Err(crate::Error::NotFound);
            }
        }

        Ok(())
    }

    fn update_block(&mut self, block: Block<'_>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        let mut cursor = self.new_cursor()?;
        cursor.to_key(&key)?;
        cursor.replace(&block.as_bytes())?;
        Ok(())
    }

    /// Inserts an [ID] into the state vector, updating the clock for the client if necessary.
    /// Returns the updated clock value: if [ID] is greater than the existing clock, its own clock
    /// is returned, otherwise the existing clock is returned.
    fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock> {
        let key = StateVectorKey::new(id.client);
        match self.get(&key) {
            Ok(value) => {
                let existing =
                    Clock::ref_from_bytes(value).map_err(|_| Error::InvalidMapping("Clock"))?;

                if &id.clock > existing {
                    self.set(&key, &id.clock.as_bytes())?;
                    Ok(id.clock)
                } else {
                    Ok(*existing)
                }
            }
            Err(lmdb_rs_m::MdbError::NotFound) => {
                self.set(&key, &id.clock.as_bytes())?;
                Ok(id.clock)
            }
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    fn split_block(&self, id: ID) -> crate::Result<SplitResult> {
        let mut cursor = self.cursor()?;
        match cursor.seek(id, false)? {
            None => Err(crate::Error::BlockNotFound(id)),
            Some(found_id) => {
                let offset = id.clock - found_id.clock;
                match cursor.split_at(offset) {
                    Ok(SplitResult::Split(left, right)) => {
                        self.set(&BlockKey::new(*right.id()), &right.as_bytes())?;
                        match left.content_type() {
                            ContentType::Json | ContentType::String | ContentType::Atom => {
                                split_content(cursor.inner, &left, &right)?
                            }
                            _ => { /* no content to split */ }
                        };

                        Ok(SplitResult::Split(left, right))
                    }
                    other => other,
                }
            }
        }
    }

    /// Returns the state vector clock for a given client ID.
    fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.get(&key) {
            Ok(value) => {
                let clock =
                    Clock::ref_from_bytes(value).map_err(|_| Error::InvalidMapping("Clock"))?;
                Ok(Some(*clock))
            }
            Err(lmdb_rs_m::MdbError::NotFound) => Ok(None),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    /// Returns the state vector for the current document.
    fn state_vector(&self) -> crate::Result<StateVector> {
        let mut state_vector = BTreeMap::new();
        let mut cursor = self.new_cursor()?;
        match cursor.to_gte_key(&[KEY_PREFIX_STATE_VECTOR].as_slice()) {
            Ok(()) => { /* found the first state vector key */ }
            Err(MdbError::NotFound) => return Ok(StateVector::new(state_vector)),
            Err(e) => return Err(Error::Lmdb(e)),
        }

        loop {
            let key: &[u8] = cursor.get_key()?;
            if key[0] != KEY_PREFIX_STATE_VECTOR {
                break;
            }

            let value: &[u8] = cursor.get_value()?;
            let client_id = *ClientID::parse(&key[1..])?;
            let clock =
                *Clock::ref_from_bytes(&value).map_err(|_| Error::InvalidMapping("Clock"))?;
            if client_id != ClientID::MAX_VALUE {
                state_vector.insert(client_id, clock);
            }

            cursor.to_next_key()?;
        }

        Ok(StateVector::new(state_vector))
    }

    fn block_content(&self, id: ID, kind: ContentType) -> crate::Result<BlockContentRef<'_>> {
        let data: &[u8] = if !kind.is_empty() {
            let key = BlockContentKey::new(id);
            self.get(&key)?
        } else {
            &[]
        };
        BlockContentRef::new(data)
    }

    fn set_block_content(&mut self, id: ID, content: &BlockContentRef) -> crate::Result<()> {
        let key = BlockContentKey::new(id);
        Ok(self.set(&key, &content.body())?)
    }

    fn entry(&self, map: ID, entry_key: &str) -> crate::Result<&ID> {
        let key = map_key(map, entry_key);
        match self.get(&key.as_bytes()) {
            Ok(value) => {
                let id = ID::parse(value)?;
                Ok(id)
            }
            Err(lmdb_rs_m::MdbError::NotFound) => Err(Error::NotFound),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    fn set_entry(&mut self, map: NodeID, entry_key: &str, value: &ID) -> crate::Result<()> {
        let key = map_key(map, entry_key);
        self.set(&key.as_bytes(), &value.as_bytes())?;
        Ok(())
    }

    fn intern_string(&mut self, string: &str, alias: U32) -> crate::Result<()> {
        let mut key: SmallVec<[u8; 5]> = smallvec![KEY_PREFIX_INTERN_STR];
        key.extend_from_slice(alias.as_bytes());
        self.set(&key.as_bytes(), &string.as_bytes())?;
        Ok(())
    }

    fn insert_pending_update(
        &mut self,
        missing_sv: &StateVector,
        remaining: &BTreeMap<ClientID, VecDeque<Carrier>>,
        pending_delete_set: &IDSet,
    ) -> crate::Result<()> {
        todo!()
    }

    fn inspect(&self) -> crate::Result<DocInspector<'_>> {
        let cursor = self.new_cursor()?;
        Ok(DocInspector::new(cursor))
    }
}

fn split_content(mut cursor: Cursor<'_>, left: &BlockMut, right: &BlockMut) -> crate::Result<()> {
    let left_id = BlockContentKey::new(*left.id());
    cursor.to_key(&left_id.as_bytes())?;
    let left_content = cursor.get_value()?;
    let content_type = left.content_type();
    let offset = left.clock_len().get() as usize;
    match content_type {
        ContentType::String => {
            let content = unsafe { std::str::from_utf8_unchecked(left_content) };
            // We need to map UTF-16 offset (which is used by Yjs) into UTF-8 (Rust's representation).
            let mut utf16 = 0;
            let mut utf8 = 0;
            for c in content.chars() {
                if utf16 == offset {
                    break;
                }
                utf16 += c.len_utf16();
                utf8 += c.len_utf8();
            }
            let (left_content, right_content) = content.split_at(utf8);
            cursor.del()?;
            cursor.set(&left_id.as_bytes(), &left_content.as_bytes(), 0)?;
            let right_id = BlockContentKey::new(*right.id());
            cursor.set(&right_id.as_bytes(), &right_content.as_bytes(), 0)?;
        }
        ContentType::Json | ContentType::Atom => {
            /* atoms and JSON are already kept split over multiple entries */
        }
        _ => unreachable!("unexpected content type"),
    }

    Ok(())
}

pub struct BlockCursor<'a> {
    inner: lmdb_rs_m::Cursor<'a>,
}

impl<'a> BlockCursor<'a> {
    /// Seeks the cursor to the given block ID.
    /// If `direct` is true, it will only seek to the block that starts with the given ID.
    /// If `direct` is false, it will seek to the block that contains the ID anywhere within its range.
    pub fn seek(&mut self, id: ID, direct: bool) -> crate::Result<Option<&ID>> {
        let key = BlockKey::new(id);
        // try to seek to the exact key first
        match self.inner.to_gte_key(&key) {
            Ok(()) => {
                let key: &[u8] = self.inner.get_key()?;
                if key[0] == KEY_PREFIX_BLOCK {
                    // the nearest >= key is a block, check if it's the one we're looking for
                    let current_id = ID::parse(&key[1..])?;
                    if current_id == &id {
                        return Ok(Some(current_id)); // found the block directly
                    } else if direct {
                        return Ok(None); // failed to found direct match
                    }
                }
            }
            Err(lmdb_rs_m::MdbError::NotFound) => {
                // no >= key found, if we're looking for direct match, return None
                if direct {
                    return Ok(None);
                }
            }
            Err(e) => return Err(Error::Lmdb(e)),
        }

        // at this point we either didn't find the block directly, and we're looking for indirect match
        // we need to move left to find the block that might contain the ID
        self.seek_prev_indirect(&id)
    }

    fn seek_prev_indirect(&mut self, id: &ID) -> crate::Result<Option<&ID>> {
        if self.prev()? {
            let key: &[u8] = self.inner.get_key()?;
            if key[0] == KEY_PREFIX_BLOCK {
                let current_id = ID::parse(&key[1..])?;
                if current_id.client == id.client {
                    // client IDs match, check clock range
                    let value = self.inner.get_value()?;
                    let block = Block::new(*current_id, value)?;
                    if block.contains(id) {
                        // found a block that contains the ID
                        return Ok(Some(current_id));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn split_at(&mut self, offset: Clock) -> crate::Result<SplitResult> {
        let block = match self.block()? {
            None => return Err(crate::Error::NotFound),
            Some(block) => block,
        };
        let mut left = BlockMut::from(block);
        match left.split(offset) {
            None => Ok(SplitResult::Unchanged(left)),
            Some(right) => {
                // update split block
                self.inner.replace(&left.as_bytes())?;
                Ok(SplitResult::Split(left, right))
            }
        }
    }

    pub fn next(&mut self) -> crate::Result<bool> {
        match self.inner.to_next_key() {
            Ok(_) => Ok(true),
            Err(lmdb_rs_m::MdbError::NotFound) => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    pub fn prev(&mut self) -> crate::Result<bool> {
        match self.inner.to_prev_key() {
            Ok(_) => Ok(true),
            Err(lmdb_rs_m::MdbError::NotFound) => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    pub fn current_id(&mut self) -> crate::Result<Option<&ID>> {
        let key: &[u8] = self.inner.get_key()?;
        if key[0] != KEY_PREFIX_BLOCK {
            return Ok(None);
        }
        let id = ID::parse(&key[1..])?;
        Ok(Some(id))
    }

    pub fn block(&mut self) -> crate::Result<Option<Block<'a>>> {
        self.inner.get_block().optional()
    }
}

pub struct Entries<'a, 'b> {
    cursor: &'b mut lmdb_rs_m::Cursor<'a>,
    prefix: [u8; 9],
    init: bool,
}

impl<'a, 'b> Entries<'a, 'b> {
    pub fn new(cursor: &'b mut lmdb_rs_m::Cursor<'a>, node_id: NodeID) -> Self {
        let mut prefix = [KEY_PREFIX_MAP, 0, 0, 0, 0, 0, 0, 0, 0];
        prefix[1..].copy_from_slice(node_id.as_bytes());
        Entries {
            cursor,
            prefix,
            init: false,
        }
    }

    fn cursor_parse(
        cursor: &mut lmdb_rs_m::Cursor<'a>,
        prefix: &[u8],
    ) -> crate::Result<Option<(&'a str, &'a ID)>> {
        let key: &[u8] = cursor.get_key()?;
        if !key.starts_with(&prefix) {
            return Ok(None);
        }
        let value: &[u8] = cursor.get_value()?;
        let id = ID::parse(&value)?;
        // function `map_keys` puts key string after the keyspace tag, map id and hash of the key itself
        const KEY_OFFSET: usize = 1 + size_of::<NodeID>() + size_of::<U32>();
        let entry_key = unsafe { str::from_utf8_unchecked(&key[KEY_OFFSET..]) };
        Ok(Some((entry_key, id)))
    }

    pub fn next_entry(&mut self) -> crate::Result<Option<(&'a str, &'a ID)>> {
        if !self.init {
            match self.cursor.to_gte_key(&self.prefix.as_bytes()) {
                Ok(_) => {}
                Err(MdbError::NotFound) => return Ok(None),
                Err(e) => return Err(Error::Lmdb(e)),
            }
            self.init = true;
        } else {
            match self.cursor.to_next_key() {
                Ok(_) => { /* ok */ }
                Err(MdbError::NotFound) => return Ok(None),
                Err(e) => return Err(Error::Lmdb(e)),
            }
        }
        Self::cursor_parse(&mut self.cursor, &self.prefix)
    }
}

impl<'a, 'b> Iterator for Entries<'a, 'b> {
    type Item = crate::Result<(&'a str, &'a ID)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'tx> From<lmdb_rs_m::Cursor<'tx>> for BlockCursor<'tx> {
    fn from(cursor: lmdb_rs_m::Cursor<'tx>) -> Self {
        BlockCursor { inner: cursor }
    }
}

pub enum SplitResult {
    Unchanged(BlockMut),
    Split(BlockMut, BlockMut),
}

pub(crate) const KEY_PREFIX_META: u8 = 0x00;
pub(crate) const KEY_PREFIX_INTERN_STR: u8 = 0x01;
pub(crate) const KEY_PREFIX_STATE_VECTOR: u8 = 0x02;
pub(crate) const KEY_PREFIX_BLOCK: u8 = 0x03;
pub(crate) const KEY_PREFIX_MAP: u8 = 0x04;
pub(crate) const KEY_PREFIX_CONTENT: u8 = 0x05;

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockKey {
    tag: u8,
    id: ID,
}

impl BlockKey {
    pub fn new(id: ID) -> Self {
        BlockKey {
            tag: KEY_PREFIX_BLOCK,
            id,
        }
    }
}

impl ToMdbValue for BlockKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let ptr = std::ptr::from_ref(self) as *const _;
        unsafe { MdbValue::new(ptr, size_of::<Self>()) }
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockContentKey {
    tag: u8,
    id: ID,
}

impl BlockContentKey {
    pub fn new(id: ID) -> Self {
        BlockContentKey {
            tag: KEY_PREFIX_CONTENT,
            id,
        }
    }
}

impl ToMdbValue for BlockContentKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let ptr = std::ptr::from_ref(self) as *const _;
        unsafe { MdbValue::new(ptr, size_of::<Self>()) }
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateVectorKey {
    tag: u8,
    client_id: ClientID,
}

impl StateVectorKey {
    pub fn new(client_id: ClientID) -> Self {
        StateVectorKey {
            tag: KEY_PREFIX_STATE_VECTOR,
            client_id,
        }
    }
}

impl ToMdbValue for StateVectorKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let ptr = std::ptr::from_ref(self) as *const _;
        unsafe { MdbValue::new(ptr, size_of::<Self>()) }
    }
}

pub type MapBucketKey = SmallVec<[u8; 24]>;
pub fn map_key(map: NodeID, key: &str) -> MapBucketKey {
    let hash: U32 = twox_hash::xxhash32::Hasher::oneshot(0, key.as_ref()).into();
    let mut res = SmallVec::new();
    res.push(KEY_PREFIX_MAP);
    res.extend_from_slice(map.as_bytes());
    res.extend_from_slice(hash.as_ref());
    res.extend_from_slice(key.as_bytes());
    res
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct MapBucketHashKey {
    tag: u8,
    node_id: NodeID,
    hash: U32,
}

impl MapBucketHashKey {
    pub fn new(node_id: NodeID, hash: U32) -> Self {
        MapBucketHashKey {
            tag: KEY_PREFIX_MAP,
            node_id,
            hash,
        }
    }
}

impl ToMdbValue for MapBucketHashKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let ptr = std::ptr::from_ref(self) as *const _;
        unsafe { MdbValue::new(ptr, size_of::<Self>()) }
    }
}

pub trait CursorExt<'a> {
    fn get_block(&mut self) -> crate::Result<Block<'a>>;
    fn entries(&mut self, node_id: NodeID) -> Entries<'a, '_>;
    fn delete_current(
        &mut self,
        state: &mut TransactionState,
        block: &mut BlockMut,
        parent_deleted: bool,
    ) -> crate::Result<bool>;
    fn content(&mut self, block: ID) -> crate::Result<&[u8]>;
}

impl<'a> CursorExt<'a> for lmdb_rs_m::Cursor<'a> {
    fn get_block(&mut self) -> crate::Result<Block<'a>> {
        let key: &[u8] = self.get_key()?;
        let value: &[u8] = self.get_value()?;
        if key[0] != KEY_PREFIX_BLOCK {
            return Err(crate::Error::NotFound);
        }
        let id = ID::parse(&key[1..])?;

        let block = Block::new(*id, value)?;
        Ok(block)
    }

    fn entries(&mut self, node_id: NodeID) -> Entries<'a, '_> {
        Entries::new(self, node_id)
    }

    fn delete_current(
        &mut self,
        state: &mut TransactionState,
        block: &mut BlockMut,
        parent_deleted: bool,
    ) -> crate::Result<bool> {
        if block.is_deleted() {
            return Ok(false);
        }
        // key to return cursor position to
        let rollback_key: &[u8] = self.get_key()?;

        block.set_deleted();
        self.replace(&block.header().as_bytes())?;
        state.delete_set.insert(*block.id(), block.clock_len());
        state.add_changed_type(*block.parent(), parent_deleted, block.key_hash());

        match block.content_type() {
            ContentType::Node => {
                // iterate over list values of the node and delete them
                let mut current = block.start().copied();
                while let Some(id) = current {
                    self.to_key(&BlockKey::new(id))?;
                    let mut block: BlockMut = self.get_block()?.into();
                    if !self.delete_current(state, &mut block, true)?
                        && block.id().clock < state.begin_state.get(&block.id().client)
                    {
                        // This will be gc'd later and we want to merge it if possible
                        // We try to merge all deleted items after each transaction,
                        // but we have no knowledge about that this needs to be merged
                        // since it is not in transaction.ds. Hence we add it to transaction._mergeStructs
                        state.merge_blocks.insert(*block.id());
                    }
                    current = block.right().copied();
                }

                //iterate over map entries of the node and delete them
                let mut to_delete = Vec::new();
                for result in self.entries(*block.parent()) {
                    let (_, &entry_id) = result?;
                    to_delete.push(entry_id);
                }

                for entry_id in to_delete {
                    self.to_key(&BlockKey::new(entry_id))?;
                    let mut block: BlockMut = self.get_block()?.into();
                    if !self.delete_current(state, &mut block, true)?
                        && entry_id.clock < state.begin_state.get(&entry_id.client)
                    {
                        // same as above
                        state.merge_blocks.insert(entry_id);
                    }
                }
                state.changed.remove(block.id());

                // restore cursor position
                self.to_key(&rollback_key)?;
            }
            ContentType::Doc => { /*TODO: document delete events */ }
            _ => { /* not used */ }
        }
        Ok(true)
    }

    fn content(&mut self, block: ID) -> crate::Result<&[u8]> {
        let key = BlockContentKey::new(block);
        self.to_key(&key)?;
        let value: &[u8] = self.get_value()?;
        Ok(&value[1..])
    }
}

/// A variant of MDB cursor that owns itself rather than being constrained to Database.
pub struct OwnedCursor<'a> {
    db: Box<Database<'a>>,
    cursor: Cursor<'a>,
}

impl<'a> OwnedCursor<'a> {
    pub fn new(db: Database<'a>) -> MdbResult<Self> {
        let db = Box::new(db);
        let cursor = db.new_cursor()?;
        let cursor: Cursor<'a> = unsafe { std::mem::transmute(cursor) };
        Ok(OwnedCursor { db, cursor })
    }
}

impl<'a> Deref for OwnedCursor<'a> {
    type Target = Cursor<'a>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'a> DerefMut for OwnedCursor<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

#[cfg(test)]
mod test {
    use crate::block::{InsertBlockData, ID};
    use crate::node::Node;
    use crate::store::lmdb::store::{BlockStore, CursorExt};
    use lmdb_rs_m::DbFlags;
    use std::collections::BTreeMap;
    use zerocopy::IntoBytes;

    #[test]
    fn find_block_directly() {
        let dir = tempfile::tempdir().unwrap();
        let env = lmdb_rs_m::Environment::builder()
            .max_dbs(10)
            .open(dir.path(), 0o777)
            .unwrap();
        let h = env.create_db("test", DbFlags::DbCreate).unwrap();
        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);

        let node_id = Node::nested(ID::new(1.into(), 1.into()));
        let id = ID::new(1.into(), 2.into());
        let insert = InsertBlockData::new(id, 1.into(), None, None, None, None, node_id, None);

        db.insert_block(&insert).unwrap();

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);
        let actual = db.fetch_block(id, true).unwrap();

        assert_eq!(actual.as_bytes(), insert.block.as_bytes());
    }

    #[test]
    fn find_block_indirectly() {
        let dir = tempfile::tempdir().unwrap();
        let env = lmdb_rs_m::Environment::builder()
            .max_dbs(10)
            .open(dir.path(), 0o777)
            .unwrap();
        let h = env.create_db("test", DbFlags::DbCreate).unwrap();
        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);

        let node_id = Node::nested(ID::new(1.into(), 1.into()));
        let searched = {
            let id = ID::new(1.into(), 2.into());
            let block =
                InsertBlockData::new(id, 10.into(), None, None, None, None, node_id.clone(), None);

            db.insert_block(&block).unwrap();
            block
        };
        {
            let id = ID::new(1.into(), 12.into());
            let block = InsertBlockData::new(id, 2.into(), None, None, None, None, node_id, None);

            db.insert_block(&block).unwrap();
        }

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let db = tx.bind(&h);

        let id = ID::new(1.into(), 5.into());
        let actual = db.fetch_block(id, false).unwrap();

        assert_eq!(actual.as_bytes(), searched.block.as_bytes());
    }

    #[test]
    fn get_set_entries() {
        let dir = tempfile::tempdir().unwrap();
        let env = lmdb_rs_m::Environment::builder()
            .max_dbs(10)
            .open(dir.path(), 0o777)
            .unwrap();
        let h = env.create_db("test", DbFlags::DbCreate).unwrap();
        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);

        let map = Node::nested(ID::new(1.into(), 1.into()));

        let expected = BTreeMap::from([
            ("key-1".to_string(), ID::new(2.into(), 0.into())),
            ("key-2".to_string(), ID::new(2.into(), 1.into())),
            ("key-3".to_string(), ID::new(2.into(), 2.into())),
        ]);

        for (k, v) in &expected {
            db.set_entry(map.id(), k, v).unwrap();
        }

        for (k, v) in &expected {
            let actual = db.entry(map.id(), k).unwrap();
            assert_eq!(actual, v);
        }

        let mut actual = BTreeMap::new();
        let mut cursor = db.new_cursor().unwrap();
        let entries = cursor.entries(map.id());
        for result in entries {
            let (k, v) = result.unwrap();
            actual.insert(k.to_string(), *v);
        }
        drop(cursor);

        assert_eq!(actual, expected);

        tx.commit().unwrap();
    }
}
