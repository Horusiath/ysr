use crate::block::{Block, BlockHeader, BlockMut, ID};
use crate::block_reader::BlockRange;
use crate::{ClientID, Clock, Error, StateVector};
use lmdb_rs_m::Database;
use std::collections::BTreeMap;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

pub struct BlockStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> BlockStore<'tx> {
    pub fn new(db: Database<'tx>) -> crate::Result<Self> {
        let store = BlockStore { db };
        Ok(store)
    }
}

impl<'tx> BlockStore<'tx> {
    /// Inserts a block into the store, updating the state vector as necessary.
    pub fn insert_block(&mut self, block: Block) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        let key = key.as_bytes();
        let value = block.bytes();

        self.db.set(&key, &value)?;
        self.try_update_clock(block.last_id())?;

        Ok(())
    }

    /// Inserts an [ID] into the state vector, updating the clock for the client if necessary.
    /// Returns the updated clock value: if [ID] is greater than the existing clock, its own clock
    /// is returned, otherwise the existing clock is returned.
    pub fn try_update_clock(&mut self, id: ID) -> crate::Result<Clock> {
        let key = StateVectorKey::new(id.client);
        let key = key.as_bytes();
        match self.db.get(&key) {
            Ok(value) => {
                let existing =
                    Clock::ref_from_bytes(value).map_err(|_| Error::InvalidMapping("Clock"))?;

                if &id.clock > existing {
                    self.db.set(&key, &id.clock.as_bytes())?;
                    Ok(id.clock)
                } else {
                    Ok(*existing)
                }
            }
            Err(lmdb_rs_m::MdbError::NotFound) => {
                self.db.set(&key, &id.clock.as_bytes())?;
                Ok(id.clock)
            }
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    pub fn split_block(&self, id: ID) -> crate::Result<SplitResult> {
        let left = self.block_containing(id, false)?;
        if left.contains(&id) {
            let mut left = BlockMut::from_block(&left);
            let right = left.split_at(&id).unwrap();
            Ok(SplitResult::Split(left, right))
        } else {
            Ok(SplitResult::Unchanged(left))
        }
    }

    pub fn remove(&mut self, id: &BlockRange) -> crate::Result<()> {
        todo!()
    }

    /// Returns the state vector clock for a given client ID.
    pub fn clock(&self, client_id: ClientID) -> crate::Result<Option<Clock>> {
        let key = StateVectorKey::new(client_id);
        match self.db.get(&key.as_bytes()) {
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
    pub fn state_vector(&self) -> crate::Result<StateVector> {
        let mut state_vector = BTreeMap::new();
        let mut cursor = self.db.new_cursor()?;
        cursor.to_gte_key(&[KEY_PREFIX_STATE_VECTOR].as_slice())?;

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
    pub fn block_containing(&self, id: ID, direct_only: bool) -> crate::Result<Block> {
        let key = BlockKey::new(id);
        let mut cursor = self.db.new_cursor()?;
        let res = cursor.to_key(&key.as_bytes());

        if let Err(lmdb_rs_m::MdbError::NotFound) = res {
            if direct_only {
                return Err(lmdb_rs_m::MdbError::NotFound.into());
            }
            // If not found directly, we will search for the block indirectly
            cursor.to_prev_key()?;
        }

        let key: &[u8] = cursor.get_key()?;
        let value: &[u8] = cursor.get_value()?;
        let id = ID::ref_from_bytes(&key[1..]).map_err(|_| Error::InvalidMapping("ID"))?;

        let block = Block::new(*id, value)?;
        if block.contains(&id) {
            Ok(block)
        } else {
            Err(lmdb_rs_m::MdbError::NotFound.into())
        }
    }
}

pub enum SplitResult<'a> {
    Unchanged(Block<'a>),
    Split(BlockMut, BlockMut),
}

const KEY_PREFIX_META: u8 = 0x00;
const KEY_PREFIX_STATE_VECTOR: u8 = 0x01;
const KEY_PREFIX_BLOCK: u8 = 0x02;

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

#[cfg(test)]
mod test {
    use crate::block::{BlockMut, ID};
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
        let mut db = BlockStore::new(tx.bind(&h)).unwrap();

        let id = ID::new(1.into(), 2.into());
        let mut block = BlockMut::new(id);
        block.set_clock_len(1.into());

        db.insert_block(block.as_ref()).unwrap();

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let db = BlockStore::new(tx.bind(&h)).unwrap();
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
        let mut db = BlockStore::new(tx.bind(&h)).unwrap();

        let searched = {
            let id = ID::new(1.into(), 2.into());
            let mut block = BlockMut::new(id);
            block.set_clock_len(10.into());

            db.insert_block(block.as_ref()).unwrap();
            block
        };
        {
            let id = ID::new(1.into(), 12.into());
            let mut block = BlockMut::new(id);
            block.set_clock_len(2.into());

            db.insert_block(block.as_ref()).unwrap();
        }

        tx.commit().unwrap();

        let tx = env.new_transaction().unwrap();
        let db = BlockStore::new(tx.bind(&h)).unwrap();

        let id = ID::new(1.into(), 5.into());
        let actual = db.block_containing(id, false).unwrap();

        assert_eq!(actual.as_bytes(), searched.as_ref().as_bytes());
    }
}
