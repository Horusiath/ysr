use crate::block::{Block, ID};
use crate::block_reader::BlockRange;
use crate::{ClientID, Clock, StateVector};
use heed::types::Bytes;
use heed::{RoIter, RoTxn, RwTxn};

pub struct BlockStore<Tx> {
    db: heed::Database<Bytes, Bytes>,
    tx: Tx,
}

impl<Tx> BlockStore<Tx> {
    pub fn new(db: heed::Database<Bytes, Bytes>, tx: Tx) -> crate::Result<Self> {
        let store = BlockStore { db, tx };
        Ok(store)
    }
}

impl<'tx, 'env> BlockStore<&'tx mut RwTxn<'env>> {
    pub fn into_ro(self) -> BlockStore<&'tx RoTxn<'env>> {
        BlockStore {
            db: self.db,
            tx: self.tx,
        }
    }

    /// Inserts a block into the store, updating the state vector as necessary.
    pub fn insert_block(&mut self, block: Block) -> crate::Result<()> {
        todo!()
    }

    pub fn split_block(&mut self, id: &ID) -> crate::Result<(Block, Block)> {
        todo!()
    }

    pub fn remove(&mut self, id: &BlockRange) -> crate::Result<()> {
        todo!()
    }
}

impl<'tx, 'env> BlockStore<&'tx RoTxn<'env>> {
    /// Returns the state vector clock for a given client ID.
    pub fn clock(&self, client_id: ClientID) -> crate::Result<Clock> {
        todo!()
    }

    /// Returns the state vector for the current document.
    pub fn state_vector(&self) -> crate::Result<StateVector> {
        todo!()
    }

    /// Returns the block which contains the given ID.
    pub fn block_containing(&self, id: &ID) -> crate::Result<Block> {
        todo!()
    }
}
