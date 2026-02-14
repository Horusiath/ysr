use crate::block_reader::BlockRange;
use crate::content::BlockContentRef;
use crate::store::lmdb::store::{BlockContentKey, KEY_PREFIX_CONTENT};
use crate::{Clock, ID};
use lmdb_rs_m::Cursor;
use std::io::BufRead;
use zerocopy::{BE, U32};

#[repr(transparent)]
pub(crate) struct ContentStore<'a> {
    cursor: Cursor<'a>,
}

impl<'a> ContentStore<'a> {
    const PREFIX: u8 = KEY_PREFIX_CONTENT;

    pub fn new(cursor: Cursor<'a>) -> Self {
        ContentStore { cursor }
    }

    /// Returns a block key range current cursor is pointing to.
    /// If cursor is not pointing anywhere within content keyspace `(None, None)` will be returned.
    /// If cursor points to a block having a single element `(Some(id), None)` will be returned.
    /// If cursor points to a block having multiple splittable elements `(Some(id), Some(end))` will be returned.
    pub fn current_range(&mut self) -> crate::Result<Option<&BlockRange>> {
        let key: &[u8] = self.cursor.get_key()?;
        if key[0] != Self::PREFIX {
            return Ok(None);
        }

        let range = BlockRange::parse(&key[1..])?;
        Ok(Some(range))
    }

    pub fn current_content(&mut self) -> crate::Result<BlockContentRef<'a>> {
        let value: &[u8] = self.cursor.get_value()?;
        //TODO: lz4 compression for big values
        BlockContentRef::from_slice(value)
    }

    pub fn seek(&mut self, id: ID) -> crate::Result<Option<Clock>> {
        let key = BlockContentKey::new(id);
        self.cursor.to_gte_key(&key)?;
        match self.current_range()? {
            (Some(current)) if current.head() != &id => {
                // we jumped beyond the ID we're looking for. We might be inside of block content
                // containing our ID, so we need to jump back.
                self.cursor.to_prev_key()?;
                match self.current_range()? {
                    // only for multi-element ranges moving to previous keys could possibly work
                    Some(current) if current.contains(&id) => Ok(Some(current.end() - id.clock)),
                    _ => Ok(None),
                }
            }
            _ => Ok(Some(Clock::new(0))),
        }
    }

    pub fn read_range<'b: 'a>(&'b mut self, range: BlockRange) -> ReadRange<'b> {
        ReadRange::new(self, range)
    }

    pub fn insert_content(&mut self, id: &ID, content: BlockContentRef<'_>) -> crate::Result<()> {
        let key = BlockContentKey::new(id.clone());
        todo!()
    }

    pub fn delete_range(&mut self, range: &BlockRange) -> crate::Result<()> {
        todo!()
    }
}

pub struct ReadRange<'a> {
    store: &'a mut ContentStore<'a>,
    range: BlockRange,
    initialized: bool,
}

impl<'a> ReadRange<'a> {
    fn new(store: &'a mut ContentStore<'a>, range: BlockRange) -> Self {
        ReadRange {
            store,
            range,
            initialized: false,
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<BlockContentRef<'a>>> {
        let init_offset = if !self.initialized {
            match self.initialise()? {
                None => return Ok(None),
                Some(offset) => {
                    self.initialized = true;
                    offset
                }
            }
        } else {
            self.store.cursor.to_next_key()?;
            Clock::new(0)
        };

        match self.store.current_range()? {
            Some(&range)
                if self.range.head().client == range.head().client
                    && self.range.head().clock <= range.end() =>
            {
                let mut content = self.store.current_content()?;
                if init_offset != 0 {
                    let (_, right) = content.split(init_offset.get() as usize);
                    content = right; // trim from start
                }

                if range.end() > self.range.end() {
                    // trim from the end
                    let offset = range.end() - self.range.end();
                    let (left, _) = content.split(offset.get() as usize);
                    content = left;
                }
                Ok(Some(content))
            }
            _ => Ok(None), // we reached the end
        }
    }

    fn initialise(&mut self) -> crate::Result<Option<Clock>> {
        match self.store.current_range()? {
            Some(current) if current.head() == self.range.head() => Ok(Some(Clock::new(0))), // cursor is in correct position
            _ => self.store.seek(*self.range.head()), // we need to reset cursor position
        }
    }
}

#[cfg(test)]
mod test {
    use crate::content::BlockContent;
    use crate::store::content_store::ContentStore;
    use crate::test_util::multi_doc;
    use crate::{ClientID, Clock, ID};

    #[test]
    fn insert_read_range() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test-doc").unwrap();
        let (db, _state) = tx.split_mut();
        let mut store = ContentStore::new(db.new_cursor().unwrap());

        store
            .insert_content(
                &ID::new(ClientID::new_random(), Clock::new(0)),
                BlockContent::string("hello").as_ref(),
            )
            .unwrap();
    }
}
