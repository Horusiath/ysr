use crate::block::{Block, BlockBuilder, ID};
use crate::block_reader::BlockRange;
use crate::node::{Node, NodeType};
use crate::{ClientID, Clock, Error, StateVector};
use lmdb_rs_m::{Database, MdbError};
use smallvec::{smallvec, SmallVec};
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub trait BlockStore<'tx> {
    fn cursor(&self) -> crate::Result<BlockCursor<'_>>;
    fn insert_block(&mut self, block: Block) -> crate::Result<()>;
    fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock>;
    fn split_block(&self, id: ID) -> crate::Result<SplitResult>;
    fn remove(&mut self, id: &BlockRange) -> crate::Result<()>;
    fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>>;
    fn state_vector(&self) -> crate::Result<StateVector>;
    fn block_containing(&self, id: ID, direct_only: bool) -> crate::Result<Block>;

    fn entry(&self, map: &ID, entry_key: &str) -> crate::Result<ID>;
    fn set_entry(&mut self, map: &ID, entry_key: &str, value: &ID) -> crate::Result<()>;

    fn get_or_insert_node(
        &mut self,
        node: Node<'_>,
        node_type: NodeType,
    ) -> crate::Result<BlockBuilder> {
        match self.block_containing(node.id(), true) {
            Ok(block) => Ok(block.into()),
            Err(crate::Error::BlockNotFound(_)) => {
                if node.is_root() {
                    // since root nodes live forever, we can create it if it does not exist
                    let block = BlockBuilder::new_node(node, node_type);
                    self.insert_block(block.as_ref())?;
                    Ok(block)
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

    /// Inserts a block into the store, updating the state vector as necessary.
    fn insert_block(&mut self, block: Block) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        let key = key.as_bytes();
        let value = block.bytes();

        self.set(&key, &value)?;
        self.try_update_clock(block.last_id())?;

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
        let left = self.block_containing(id, false)?;
        let clock = left.id().clock;
        if id.clock > clock && id.clock < clock + left.clock_len() {
            let offset = id.clock - left.id().clock;
            let mut left = BlockBuilder::from_block(&left);
            let right = left.splice(offset)?;
            Ok(SplitResult::Split(left, right.unwrap()))
        } else {
            Ok(SplitResult::Unchanged(left))
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

    /// Returns the block which contains the given ID.
    /// If `direct_only` is true, it will only search for blocks that starts with the given ID.
    /// If `direct_only` is false, it will search for blocks that contain the ID anywhere within
    /// their range.
    fn block_containing(&self, id: ID, direct_only: bool) -> crate::Result<Block<'_>> {
        let mut cursor = self.cursor()?;
        let found = cursor.seek(id)?;
        if !found && direct_only {
            // block was not found, but we're only looking for direct matches
            return Err(Error::BlockNotFound(id));
        }

        let block = cursor.block()?;
        match block {
            Some(block) if block.contains(&id) => Ok(block),
            _ => {
                if !direct_only && cursor.prev()? {
                    // if we didn't find the block directly, we need to check the previous block
                    if let Some(block) = cursor.block()? {
                        if block.contains(&id) {
                            return Ok(block);
                        }
                    }
                }
                Err(Error::BlockNotFound(id))
            }
        }
    }

    fn entry(&self, map: &ID, entry_key: &str) -> crate::Result<ID> {
        let key = map_entry_key(map, entry_key);
        let value: &[u8] = match self.get(&key.as_slice()) {
            Ok(value) => value,
            Err(MdbError::NotFound) => return Err(crate::Error::NotFound),
            Err(e) => return Err(Error::Lmdb(e)),
        };
        let block_id = ID::ref_from_bytes(value).map_err(|_| Error::InvalidMapping("ID"))?;
        Ok(*block_id)
    }

    fn set_entry(&mut self, map: &ID, entry_key: &str, value: &ID) -> crate::Result<()> {
        let key = map_entry_key(map, entry_key);
        self.set(&key.as_slice(), &value.as_bytes())?;
        Ok(())
    }
}

pub struct BlockCursor<'a> {
    inner: lmdb_rs_m::Cursor<'a>,
}

impl<'a> BlockCursor<'a> {
    pub fn seek(&mut self, id: ID) -> crate::Result<bool> {
        let key = BlockKey::new(id);
        match self.inner.to_gte_key(&key.as_bytes()) {
            Ok(_) => Ok(true),
            Err(lmdb_rs_m::MdbError::NotFound) => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
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

pub enum SplitResult<'a> {
    Unchanged(Block<'a>),
    Split(BlockBuilder, BlockBuilder),
}

const KEY_PREFIX_META: u8 = 0x00;
const KEY_PREFIX_STATE_VECTOR: u8 = 0x01;
const KEY_PREFIX_BLOCK: u8 = 0x02;
const KEY_PREFIX_MAP: u8 = 0x03;

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

fn map_entry_key(map: &ID, key: &str) -> SmallVec<[u8; 16]> {
    let mut buf = smallvec![KEY_PREFIX_MAP];
    buf.extend_from_slice(map.as_bytes());
    buf.extend_from_slice(key.as_bytes());
    buf
}

#[cfg(test)]
mod test {
    use crate::block::{BlockBuilder, ID};
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

        let id = ID::new(1.into(), 2.into());
        let block = BlockBuilder::new(
            id,
            1.into(),
            None,
            None,
            None,
            None,
            ID::new(1.into(), 1.into()),
            None,
        )
        .unwrap();

        db.insert_block(block.as_ref()).unwrap();

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let mut db = tx.bind(&h);
        let actual = db.block_containing(id, true).unwrap();

        assert_eq!(actual.as_bytes(), block.as_ref().as_bytes());
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

        let searched = {
            let id = ID::new(1.into(), 2.into());
            let block = BlockBuilder::new(
                id,
                10.into(),
                None,
                None,
                None,
                None,
                ID::new(1.into(), 1.into()),
                None,
            )
            .unwrap();

            db.insert_block(block.as_ref()).unwrap();
            block
        };
        {
            let id = ID::new(1.into(), 12.into());
            let block = BlockBuilder::new(
                id,
                2.into(),
                None,
                None,
                None,
                None,
                ID::new(1.into(), 1.into()),
                None,
            )
            .unwrap();

            db.insert_block(block.as_ref()).unwrap();
        }

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let db = tx.bind(&h);

        let id = ID::new(1.into(), 5.into());
        let actual = db.block_containing(id, false).unwrap();

        assert_eq!(actual.as_bytes(), searched.as_ref().as_bytes());
    }
}
