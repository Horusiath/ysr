use crate::store::lmdb::store::KEY_PREFIX_BLOCK;
use crate::{Block, Error, ID};
use lmdb_rs_m::{MdbError, MdbValue, ToMdbValue};
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct BlockStore<'tx> {
    cursor: lmdb_rs_m::Cursor<'tx>,
}

impl<'tx> BlockStore<'tx> {
    const PREFIX: u8 = KEY_PREFIX_BLOCK;

    pub fn new(cursor: lmdb_rs_m::Cursor<'tx>) -> Self {
        Self { cursor }
    }

    /// Returns an [ID] of the block at the current position.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn current_id(&mut self) -> crate::Result<Option<&ID>> {
        let key: &'tx [u8] = match self.cursor.get_key() {
            Ok(key) => key,
            // we reached the boundary of the database or cursor was not set yet
            Err(MdbError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if key[0] == Self::PREFIX {
            let id = ID::parse(&key[1..])?;
            Ok(Some(id))
        } else {
            Ok(None) // we run outside the block key-space
        }
    }

    /// Returns a [Block] at the current cursor position.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn current(&mut self) -> crate::Result<Option<Block<'tx>>> {
        match self.current_id()? {
            None => Ok(None),
            Some(&id) => {
                let value: &'tx [u8] = self.cursor.get_value()?;
                Ok(Some(Block::new(id, value)?))
            }
        }
    }

    /// Moves current cursor position to a block starting with a given [ID].
    /// Returns true if block has been found.
    pub fn seek(&mut self, id: ID) -> crate::Result<Option<Block<'tx>>> {
        let key = BlockKey::new(id);
        match self.cursor.to_key(&key) {
            Ok(_) => self.current(),
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves current cursor position to a block containing an element with a given [ID].
    /// Returns true if block has been found.
    pub fn seek_containing(&mut self, id: ID) -> crate::Result<Option<Block<'tx>>> {
        let key = BlockKey::new(id);
        // try to seek to the exact key first
        match self.cursor.to_gte_key(&key) {
            Ok(()) => {
                if let Some(block) = self.current()?
                    && block.id() == &id
                {
                    // the nearest >= key is a block, check if it's the one we're looking for
                    return Ok(Some(block));
                }
            }
            Err(lmdb_rs_m::MdbError::NotFound) => { /* no >= key found */ }
            Err(e) => return Err(Error::Lmdb(e)),
        }

        // at this point we either didn't find the block directly, and we're looking for indirect match
        // we need to move left to find the block that might contain the ID
        self.seek_prev_indirect(&id)
    }

    fn seek_prev_indirect(&mut self, id: &ID) -> crate::Result<Option<Block<'tx>>> {
        if let Some(block) = self.prev()?
            && block.contains(id)
        {
            Ok(Some(block))
        } else {
            Ok(None)
        }
    }

    /// Moves current cursor position to a next block, returning it.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn next(&mut self) -> crate::Result<Option<Block<'tx>>> {
        self.cursor.to_next_key()?;
        self.current()
    }

    /// Moves current cursor position to a previous block, returning it.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn prev(&mut self) -> crate::Result<Option<Block<'tx>>> {
        self.cursor.to_prev_key()?;
        self.current()
    }

    /// Moves current cursor position to a block, that's a right neighbor of the current block.
    /// Returns `None` if the right neighbor could not be found.
    pub fn right(&mut self) -> crate::Result<Option<Block<'tx>>> {
        let curr = match self.current()? {
            Some(block) => block,
            None => return Ok(None),
        };
        let right_id = match curr.right() {
            Some(id) => id,
            None => return Ok(None),
        };

        self.seek(*right_id)
    }

    /// Moves current cursor position to a block, that's a left neighbor of the current block.
    /// Returns `None` if the left neighbor could not be found.
    pub fn left(&mut self) -> crate::Result<Option<Block<'tx>>> {
        let curr = match self.current()? {
            Some(block) => block,
            None => return Ok(None),
        };
        let left_id = match curr.left() {
            Some(id) => id,
            None => return Ok(None),
        };

        self.seek_containing(*left_id) // left id is point at the end of the block
    }

    /// Inserts a new block into database.
    pub fn insert(&mut self, block: Block<'tx>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        self.cursor.set(&key, block.header(), 0)?;
        Ok(())
    }

    /// Moves the cursor position into the given block location and replaces existing block header
    /// with a provided one. This method will throw an error if a block hadn't been inserted into
    /// a database before.
    pub fn update(&mut self, block: Block<'tx>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        self.cursor.to_key(&key)?;
        self.cursor.replace(block.header())?;
        Ok(())
    }

    pub fn inspect(&mut self) -> Inspector<'_> {
        Inspector { store: self }
    }
}

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

pub struct Inspector<'tx> {
    store: &'tx mut BlockStore<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_list();
        if let Some(block) = self.store.current().map_err(|_| std::fmt::Error)? {
            s.entry(&block);

            while let Some(block) = self.store.next().map_err(|_| std::fmt::Error)? {
                s.entry(&block);
            }
        }

        s.finish()
    }
}
