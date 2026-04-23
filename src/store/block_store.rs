use crate::block::BlockFlags;
use crate::content::ContentType;
use crate::id_set::IDSet;
use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::node::{Named, Node, NodeType};
use crate::store::KEY_PREFIX_BLOCK;
use crate::store::content_store::ContentStore;
use crate::store::intern_strings::InternStringsStore;
use crate::{Block, BlockHeader, BlockMut, ClientID, Clock, Error, ID, Optional, lmdb};
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct BlockStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> BlockStore<'tx> {
    pub fn new(db: Database<'tx>) -> Self {
        Self { db }
    }

    pub fn cursor(&self) -> crate::Result<BlockCursor<'tx>> {
        BlockCursor::new(self.db)
    }

    pub fn contents(self) -> ContentStore<'tx> {
        ContentStore::new(self.db)
    }

    pub fn get(&self, id: ID) -> crate::Result<Block<'tx>> {
        let key = BlockKey::new(id);
        match self.db.get(key.as_bytes()) {
            Ok(value) => Ok(Block::new(id, value)?),
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Inserts a new block into database.
    pub fn insert(&self, block: Block<'_>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        self.db.put(key.as_bytes(), block.header().as_bytes())?;
        Ok(())
    }

    pub fn split(&self, id: ID) -> crate::Result<SplitResult> {
        let mut cursor = self.cursor()?;
        cursor.split(id)
    }

    pub fn inspect(&self) -> Inspector<'_> {
        Inspector { db: self.db }
    }
}

impl<'tx> From<BlockStore<'tx>> for Database<'tx> {
    fn from(value: BlockStore<'tx>) -> Self {
        value.db
    }
}

pub struct BlockCursor<'tx> {
    cursor: Cursor<'tx>,
    db: Database<'tx>,
}

impl<'tx> BlockCursor<'tx> {
    const PREFIX: u8 = KEY_PREFIX_BLOCK;

    pub fn new(db: Database<'tx>) -> crate::Result<Self> {
        let cursor = db.cursor()?;
        Ok(BlockCursor { cursor, db })
    }

    pub fn insert(&mut self, block: Block<'_>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        self.cursor
            .put(key.as_bytes(), block.header().as_bytes(), 0)?;
        Ok(())
    }

    pub fn get_or_insert_node(&self, node: Node, node_type: NodeType) -> crate::Result<BlockMut> {
        let node_id = node.id();
        let key = BlockKey::new(node_id);
        match self.db.get(key.as_bytes()) {
            Ok(value) => {
                let header: &BlockHeader = BlockHeader::try_ref_from_bytes(value)
                    .map_err(|_| crate::Error::MalformedBlock(node_id))?;
                Ok(BlockMut::new(node_id, header.clone()))
            }
            Err(LmdbError::NOT_FOUND) if node_id.is_root() => {
                if let Node::Root(Named::Name(name)) = node {
                    let strings = InternStringsStore::new(self.db);
                    strings.intern(name.as_ref())?;
                }
                // root types don't carry over extra data
                let mut header = BlockHeader::empty();
                header.set_content_type(ContentType::Node);
                header.set_node_type(node_type);
                let block = BlockMut::new(node_id, header);
                self.db.put(key.as_bytes(), block.header().as_bytes())?;
                Ok(block)
            }
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn db(&self) -> &Database<'tx> {
        &self.db
    }

    /// Moves the cursor position into the given block location and replaces existing block header
    /// with a provided one. This method will throw an error if a block hadn't been inserted into
    /// a database before.
    pub fn update(&mut self, block: Block<'_>) -> crate::Result<()> {
        let key = BlockKey::new(*block.id());
        let key_bytes = key.as_bytes();
        let already_here = self
            .cursor
            .key_value()
            .ok()
            .is_some_and(|(k, _)| k == key_bytes);
        if !already_here {
            self.cursor.set_key(key_bytes)?;
        }
        self.cursor
            .put_current(key_bytes, block.header().as_bytes())?;
        Ok(())
    }

    /// Try to interpret a raw LMDB key+value pair as a Block.
    /// Returns `None` if the key prefix doesn't match the block key-space.
    fn parse_block(key: &[u8], value: &'tx [u8]) -> crate::Result<Option<Block<'tx>>> {
        if key.first() == Some(&Self::PREFIX) {
            let &id = ID::parse(&key[1..])?;
            Ok(Some(Block::new(id, value)?))
        } else {
            Ok(None)
        }
    }

    /// Returns a [Block] at the current cursor position.
    pub fn current(&mut self) -> crate::Result<Block<'tx>> {
        let (key, value) = self.cursor.key_value()?;
        Self::parse_block(key, value)?.ok_or(crate::Error::NotFound)
    }

    /// Move cursor to the beginning of the block store space.
    pub fn start_from(&mut self, id: ID) -> crate::Result<()> {
        let key = BlockKey::new(id);
        match self.cursor.set_range(key.as_bytes()) {
            Ok(_) => Ok(()),
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves current cursor position to a block starting with a given [ID].
    pub fn seek(&mut self, id: ID) -> crate::Result<Block<'tx>> {
        // fast path: check if we're already at the right position
        if let Ok((key, value)) = self.cursor.key_value() {
            if let Some(block) = Self::parse_block(key, value)? {
                if block.id() == &id {
                    return Ok(block);
                }
            }
        }

        let key = BlockKey::new(id);
        match self.cursor.set_key(key.as_bytes()) {
            Ok((_, value)) => Ok(Block::new(id, value)?),
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves current cursor position to a block containing an element with a given [ID].
    pub fn seek_containing(&mut self, id: ID) -> crate::Result<Block<'tx>> {
        let key = BlockKey::new(id);
        match self.cursor.set_range(key.as_bytes()) {
            Ok((found_key, value)) => {
                if let Some(block) = Self::parse_block(found_key, value)? {
                    if block.id() == &id {
                        return Ok(block);
                    }
                }
            }
            Err(LmdbError::NOT_FOUND) => { /* no >= key found */ }
            Err(e) => return Err(Error::Lmdb(e)),
        }

        // move left to find the block that might contain the ID
        self.seek_prev_indirect(&id)
    }

    fn seek_prev_indirect(&mut self, id: &ID) -> crate::Result<Block<'tx>> {
        if let Some(block) = self.prev()?
            && block.contains(id)
        {
            Ok(block)
        } else {
            Err(crate::Error::NotFound)
        }
    }

    /// Moves current cursor position to a next block, returning it.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn next(&mut self) -> crate::Result<Option<Block<'tx>>> {
        match self.cursor.next() {
            Ok((key, value)) => Self::parse_block(key, value),
            Err(LmdbError::NOT_FOUND) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves current cursor position to a previous block, returning it.
    /// Returns `None` if current cursor position is outside the block boundaries.
    pub fn prev(&mut self) -> crate::Result<Option<Block<'tx>>> {
        match self.cursor.prev() {
            Ok((key, value)) => Self::parse_block(key, value),
            Err(LmdbError::NOT_FOUND) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Moves current cursor position to a block, that's a right neighbor of the current block.
    /// Returns `None` if the right neighbor could not be found.
    pub fn right(&mut self) -> crate::Result<Option<Block<'tx>>> {
        let curr = match self.current().optional()? {
            Some(block) => block,
            None => return Ok(None),
        };
        let right_id = match curr.right() {
            Some(id) => id,
            None => return Ok(None),
        };

        self.seek(*right_id).optional()
    }

    /// Moves current cursor position to a block, that's a left neighbor of the current block.
    /// Returns `None` if the left neighbor could not be found.
    pub fn left(&mut self) -> crate::Result<Option<Block<'tx>>> {
        let curr = match self.current().optional()? {
            Some(block) => block,
            None => return Ok(None),
        };
        let left_id = match curr.left() {
            Some(id) => id,
            None => return Ok(None),
        };

        self.seek_containing(*left_id).optional() // left id is point at the end of the block
    }

    #[inline]
    pub fn update_current(&mut self, id: ID, header: &BlockHeader) -> crate::Result<()> {
        let key = BlockKey::new(id);
        self.cursor.put_current(key.as_bytes(), header.as_bytes())?;
        Ok(())
    }

    pub fn content_store(&self) -> ContentStore<'tx> {
        ContentStore::new(self.db)
    }

    pub fn split_current(&mut self, offset: Clock) -> crate::Result<SplitResult> {
        let mut left: BlockMut = self.current()?.into();
        match left.split(offset) {
            None => Ok(SplitResult::Unchanged(left)),
            Some(right) => {
                self.update_current(*left.id(), left.header())?;
                let key = BlockKey::new(*right.id());
                self.cursor
                    .put(key.as_bytes(), right.as_block().header().as_bytes(), 0)?;

                if !left.flags().contains(BlockFlags::INLINE_CONTENT)
                    && left.content_type() == ContentType::String
                {
                    let contents = ContentStore::new(self.db);
                    contents.split_string(*left.id(), offset)?;
                }

                Ok(SplitResult::Split(left, right))
            }
        }
    }

    pub fn split(&mut self, id: ID) -> crate::Result<SplitResult> {
        let mut left: BlockMut = self.seek_containing(id)?.into();
        let offset = id.clock - left.id().clock;
        match left.split(offset) {
            None => Ok(SplitResult::Unchanged(left)),
            Some(right) => {
                self.update_current(*left.id(), left.header())?;
                let key = BlockKey::new(*right.id());
                self.cursor
                    .put(key.as_bytes(), right.as_block().header().as_bytes(), 0)?;

                if !left.flags().contains(BlockFlags::INLINE_CONTENT)
                    && left.content_type() == ContentType::String
                {
                    let contents = ContentStore::new(self.db);
                    contents.split_string(*left.id(), offset)?;
                }

                Ok(SplitResult::Split(left, right))
            }
        }
    }

    pub fn delete_set(&mut self) -> crate::Result<IDSet> {
        let start = BlockKey::new(ID::new(unsafe { ClientID::new_unchecked(1) }, 0.into()));

        let mut ds = IDSet::default();
        let (mut key, mut value) = match self.cursor.set_range(start.as_bytes()) {
            Ok(kv) => kv,
            Err(lmdb::Error::NOT_FOUND) => return Ok(ds),
            Err(e) => return Err(e.into()),
        };

        loop {
            match Self::parse_block(key, value)? {
                Some(block) => {
                    if block.is_deleted() {
                        ds.insert(*block.id(), block.clock_len());
                    }
                }
                None => break,
            }
            match self.cursor.next() {
                Ok(kv) => {
                    key = kv.0;
                    value = kv.1;
                }
                Err(lmdb::Error::NOT_FOUND) => break,
                Err(e) => return Err(e.into()),
            }
        }
        ds.squash();
        Ok(ds)
    }
}

pub enum SplitResult {
    Unchanged(BlockMut),
    Split(BlockMut, BlockMut),
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

pub struct Inspector<'tx> {
    db: Database<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_list();
        let mut c = BlockCursor::new(self.db).map_err(|_| std::fmt::Error)?;
        // we need to set cursor position at the beginning of the space
        let _ = c.seek(ID::new(0.into(), 0.into()));

        while let Some(block) = c.next().map_err(|_| std::fmt::Error)? {
            s.entry(&block);
        }

        s.finish()
    }
}
