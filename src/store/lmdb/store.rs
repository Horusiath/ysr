use crate::block::{Block, BlockMut, InsertBlockData, ID};
use crate::block_reader::{BlockRange, Carrier};
use crate::bucket::Bucket;
use crate::content::{BlockContent, ContentType};
use crate::id_set::IDSet;
use crate::node::{Node, NodeType};
use crate::{ClientID, Clock, Error, StateVector, U64};
use lmdb_rs_m::{Database, MdbError};
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::collections::{BTreeMap, VecDeque};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub trait BlockStore<'tx> {
    fn cursor(&self) -> crate::Result<BlockCursor<'_>>;
    fn fetch_block(&self, id: ID, direct_only: bool) -> crate::Result<Block<'_>>;
    fn insert_block(&mut self, builder: &InsertBlockData) -> crate::Result<()>;
    fn update_block(&mut self, block: Block) -> crate::Result<()>;
    fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock>;
    fn split_block(&self, id: ID) -> crate::Result<SplitResult>;
    fn remove(&mut self, id: &BlockRange) -> crate::Result<()>;
    fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>>;
    fn state_vector(&self) -> crate::Result<StateVector>;

    fn block_content(&self, id: ID, kind: ContentType) -> crate::Result<BlockContent<'_>>;
    fn set_block_content(&mut self, id: ID, content: &BlockContent) -> crate::Result<()>;

    fn entry(&self, map: &ID, entry_key: &str) -> crate::Result<ID>;
    fn set_entry(&mut self, map: &ID, entry_key: &str, value: &ID) -> crate::Result<()>;

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
                if node.is_root() {
                    // since root nodes live forever, we can create it if it does not exist
                    let data = InsertBlockData::new_node(node, node_type);
                    self.insert_block(&data)?;
                    Ok(data.block)
                } else {
                    // nested nodes are not created automatically, if we didn't find it, we return an error
                    Err(crate::Error::NotFound)
                }
            }
            Err(e) => Err(e),
        }
    }
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
        self.set(
            &BlockKey::new(*insert.id()).as_bytes(),
            &insert.as_block().as_bytes(),
        )?;
        self.try_update_clock(insert.last_id())?;

        // insert block content if any
        if !insert.content.is_empty() {
            self.set(
                &BlockContentKey::new(*insert.id()).as_bytes(),
                &insert.content.as_bytes(),
            )?;
        }

        // insert block entry key if any
        if let Some(key) = insert.entry.as_deref() {
            let key = unsafe { str::from_utf8_unchecked(key) };
            if let Some(parent) = insert.parent() {
                self.set_entry(&parent.id(), key, insert.id())?;
            } else {
                return Err(crate::Error::NotFound);
            }
        }

        Ok(())
    }

    fn update_block(&mut self, block: Block<'_>) -> crate::Result<()> {
        self.set(&BlockKey::new(*block.id()).as_bytes(), &block.as_bytes())?;
        Ok(())
    }

    /// Inserts an [ID] into the state vector, updating the clock for the client if necessary.
    /// Returns the updated clock value: if [ID] is greater than the existing clock, its own clock
    /// is returned, otherwise the existing clock is returned.
    fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock> {
        let key = StateVectorKey::new(id.client);
        let key = key.as_bytes();
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
                        self.set(&BlockKey::new(*right.id()).as_bytes(), &right.as_bytes())?;
                        todo!("split content for blocks not yet implemented");
                        Ok(SplitResult::Split(left, right))
                    }
                    other => other,
                }
            }
        }
    }

    fn remove(&mut self, id: &BlockRange) -> crate::Result<()> {
        todo!()
    }

    /// Returns the state vector clock for a given client ID.
    fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.get(&key.as_bytes()) {
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
            cursor.to_next_key()?;
            let key: &[u8] = cursor.get_key()?;
            if key[0] != KEY_PREFIX_STATE_VECTOR {
                break;
            }

            let value: &[u8] = cursor.get_value()?;
            let client_id = ClientID::ref_from_bytes(&key[1..])
                .map_err(|_| Error::InvalidMapping("ClientID"))?;
            let clock =
                Clock::ref_from_bytes(&value).map_err(|_| Error::InvalidMapping("Clock"))?;
            state_vector.insert(*client_id, *clock);
        }

        Ok(StateVector::new(state_vector))
    }

    fn block_content(&self, id: ID, kind: ContentType) -> crate::Result<BlockContent<'_>> {
        let data: &[u8] = if !kind.is_empty() {
            let key = BlockContentKey::new(id);
            self.get(&key.as_bytes())?
        } else {
            &[]
        };
        BlockContent::new(kind, data)
    }

    fn set_block_content(&mut self, id: ID, content: &BlockContent) -> crate::Result<()> {
        let key = BlockContentKey::new(id);
        Ok(self.set(&key.as_bytes(), &content.body())?)
    }

    fn entry(&self, map: &ID, entry_key: &str) -> crate::Result<ID> {
        let hash: U64 = twox_hash::xxhash3_64::Hasher::oneshot(entry_key.as_bytes()).into();
        let key = map_bucket_key(map, hash);
        let bucket = match self.get(&key.as_slice()) {
            Ok(value) => Bucket::from_bytes(value),
            Err(MdbError::NotFound) => return Err(crate::Error::NotFound),
            Err(e) => return Err(Error::Lmdb(e)),
        };
        match bucket.get(entry_key.as_bytes()) {
            None => Err(crate::Error::NotFound),
            Some(id) => Ok(*id),
        }
    }

    fn set_entry(&mut self, map: &ID, entry_key: &str, value: &ID) -> crate::Result<()> {
        let entry_key = entry_key.as_bytes();
        let hash: U64 = twox_hash::xxhash3_64::Hasher::oneshot(entry_key).into();
        let key = map_bucket_key(map, hash);
        let mut bucket = match self.get(&key.as_slice()) {
            Ok(value) => Bucket::from_bytes(value),
            Err(MdbError::NotFound) => Bucket::from_bytes(&[]),
            Err(e) => return Err(Error::Lmdb(e)),
        };
        bucket.insert(entry_key, value);
        self.set(&key.as_slice(), &bucket.as_bytes())?;
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
        match self.inner.to_gte_key(&key.as_bytes()) {
            Ok(()) => {
                let key: &[u8] = self.inner.get_key()?;
                if key[0] == KEY_PREFIX_BLOCK {
                    // the nearest >= key is a block, check if it's the one we're looking for
                    let current_id =
                        ID::ref_from_bytes(&key[1..]).map_err(|_| Error::InvalidMapping("ID"))?;
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
                // previous entry is a block, check if it contains the ID
                let current_id =
                    ID::ref_from_bytes(&key[1..]).map_err(|_| Error::InvalidMapping("ID"))?;
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
        let id = ID::ref_from_bytes(&key[1..]).map_err(|_| Error::InvalidMapping("ID"))?;
        Ok(Some(id))
    }

    pub fn block(&mut self) -> crate::Result<Option<Block<'a>>> {
        let key: &[u8] = self.inner.get_key()?;
        let value: &[u8] = self.inner.get_value()?;
        if key[0] != KEY_PREFIX_BLOCK {
            return Ok(None);
        }
        let id = ID::ref_from_bytes(&key[1..]).map_err(|_| Error::InvalidMapping("ID"))?;

        let block = Block::new(*id, value)?;
        Ok(Some(block))
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

const KEY_PREFIX_META: u8 = 0x00;
const KEY_PREFIX_STATE_VECTOR: u8 = 0x01;
const KEY_PREFIX_BLOCK: u8 = 0x02;
const KEY_PREFIX_MAP: u8 = 0x03;
const KEY_PREFIX_CONTENT: u8 = 0x04;

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

fn map_bucket_key(map: &ID, hash: U64) -> SmallVec<[u8; 16]> {
    let mut buf = smallvec![KEY_PREFIX_MAP];
    buf.extend_from_slice(map.as_bytes());
    buf.extend_from_slice(hash.as_bytes());
    buf
}

#[cfg(test)]
mod test {
    use crate::block::{InsertBlockData, ID};
    use crate::node::Node;
    use crate::store::lmdb::store::BlockStore;
    use lmdb_rs_m::DbFlags;
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
        let block = InsertBlockData::new(id, 1.into(), None, None, None, None, node_id, None);

        db.insert_block(&block).unwrap();

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);
        let actual = db.fetch_block(id, true).unwrap();

        assert_eq!(actual.as_bytes(), block.as_bytes());
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

        assert_eq!(actual.as_bytes(), searched.as_bytes());
    }
}
