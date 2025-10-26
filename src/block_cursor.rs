use crate::block::{Block, ID};
use crate::store::lmdb::store::BlockKey;
use lmdb_rs_m::core::MdbResult;
use lmdb_rs_m::{Cursor, MdbError};
use std::cmp::Ordering;
use std::ops::{Deref, DerefMut};
use zerocopy::IntoBytes;

pub(crate) struct BlockCursor<'tx> {
    cursor: Cursor<'tx>,
    last: Option<Block<'tx>>,
}

impl<'tx> BlockCursor<'tx> {
    pub fn new(cursor: Cursor<'tx>) -> Self {
        BlockCursor { cursor, last: None }
    }

    pub fn seek(&mut self, id: ID) -> crate::Result<()> {
        if let Some(block) = &self.last {
            let block_id = block.id();
            if id.client == block_id.client {
                let len = block.clock_len();
                match id.clock.cmp(&block_id.clock) {
                    Ordering::Less => {
                        // searched id is lower than the block's clock
                    }
                    Ordering::Equal => {
                        // if the id is equal to the block's clock, we are already at the right place
                        return Ok(());
                    }
                    Ordering::Greater => match id.clock.cmp(&(block_id.clock + len)) {
                        Ordering::Less => {
                            // searched id is within the block's clock range
                            return Ok(());
                        }
                        Ordering::Equal => {
                            // searched id is directly to the right of the block's clock
                            self.cursor.to_next_key()?;
                            return Ok(());
                        }
                        Ordering::Greater => {
                            // searched id is greater than the block's clock range
                        }
                    },
                }
            }
        }
        let key = BlockKey::new(id);
        match self.cursor.to_gte_key(&key) {
            Ok(()) => Ok(()),
            Err(MdbError::NotFound) => Err(crate::Error::BlockNotFound(id)),
            Err(e) => Err(crate::Error::from(e)),
        }
    }

    pub fn next_right(&mut self) -> crate::Result<Block<'tx>> {
        todo!()
    }

    pub fn next_left(&mut self) -> crate::Result<Block<'tx>> {
        todo!()
    }
}

impl<'tx> Deref for BlockCursor<'tx> {
    type Target = Cursor<'tx>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'tx> DerefMut for BlockCursor<'tx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}
