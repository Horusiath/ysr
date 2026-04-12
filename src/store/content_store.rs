use crate::block_reader::BlockRange;
use crate::content::{Content, ContentType, utf16_to_utf8};
use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::store::{KEY_PREFIX_CONTENT, ReadableBytes};
use crate::{Clock, ID, Optional};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct ContentStore<'a> {
    db: &'a Database<'a>,
}

impl<'a> ContentStore<'a> {
    const PREFIX: u8 = KEY_PREFIX_CONTENT;

    pub fn new(db: &'a Database<'a>) -> Self {
        ContentStore { db }
    }

    pub fn get(&self, key: ID) -> crate::Result<&'a [u8]> {
        let key = BlockContentKey::new(key);
        match self.db.get(key.as_bytes()) {
            Ok(value) => Ok(value),
            Err(LmdbError::NOT_FOUND) => Err(crate::Error::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert(&self, id: ID, data: &[u8]) -> crate::Result<()> {
        let key = BlockContentKey::new(id);
        self.db.put(key.as_bytes(), data)?;
        Ok(())
    }

    pub fn insert_range(&self, mut id: ID, content: &[Content<'_>]) -> crate::Result<()> {
        let mut cursor = self.db.cursor()?;
        for content in content {
            let key = BlockContentKey::new(id);
            cursor.put(key.as_bytes(), content.bytes(), 0)?;
            id.clock += 1; // this will only happen for multipart
        }
        Ok(())
    }

    pub fn delete_range(
        &mut self,
        content_type: ContentType,
        range: &BlockRange,
    ) -> crate::Result<usize> {
        let is_multipart = match content_type {
            ContentType::Deleted | ContentType::Node | ContentType::Embed => {
                return Ok(0); // these types don't have their content stored in ContentStore
            }
            ContentType::Binary | ContentType::String | ContentType::Format | ContentType::Doc => {
                false // these types are always stored on a single content entry
            }
            ContentType::Json | ContentType::Atom => {
                true // these types can be stored on multiple entries
            }
        };
        let mut cursor = self.db.cursor()?;
        let mut curr = *range.head();
        let key = BlockContentKey::new(curr);
        cursor.set_key(key.as_bytes())?;
        cursor.del()?;
        let mut deleted_entries = 1;

        if is_multipart {
            let end = ID::new(curr.client, range.end());
            while curr != end {
                cursor.next()?;
                curr = match parse_id(cursor.key()?)? {
                    Some(id) => *id,
                    None => break,
                };
                if curr != end {
                    cursor.del()?;
                    deleted_entries += 1;
                }
            }
        }
        Ok(deleted_entries)
    }

    pub fn read_range(&self, content_type: ContentType, range: BlockRange) -> ReadRange<'_> {
        ReadRange::new(self.db, content_type, range)
    }

    pub fn split_string(&self, id: ID, offset: Clock) -> crate::Result<()> {
        let data = self.get(id)?;
        let source = unsafe { std::str::from_utf8_unchecked(data) };
        let utf16_offset = offset.get() as usize;
        if let Some(utf8_offset) = utf16_to_utf8(source, utf16_offset) {
            // Copy data before writing, since LMDB may invalidate the pointer
            let data = data.to_vec();
            let left_bytes = &data[..utf8_offset];
            let right_bytes = &data[utf8_offset..];
            self.insert(id, left_bytes)?;
            self.insert(id.add(offset), right_bytes)?;
        }
        Ok(())
    }

    pub fn inspect(&self) -> Inspect<'a> {
        Inspect { db: self.db }
    }
}

fn parse_id(key: &[u8]) -> crate::Result<Option<&ID>> {
    if key[0] != ContentStore::PREFIX {
        return Ok(None);
    }

    let id = ID::parse(&key[1..])?;
    Ok(Some(id))
}

pub struct Inspect<'tx> {
    db: &'tx Database<'tx>,
}

impl<'tx> Debug for Inspect<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();

        let mut cursor = self.db.cursor().map_err(|_| std::fmt::Error)?;
        cursor
            .set_range(&[ContentStore::PREFIX])
            .map_err(|_| std::fmt::Error)?;
        while let Some(id) =
            parse_id(cursor.key().map_err(|_| std::fmt::Error)?).map_err(|_| std::fmt::Error)?
        {
            s.key(id);
            s.value(&ReadableBytes::new(
                cursor.value().map_err(|_| std::fmt::Error)?,
            ));
            cursor.next().map_err(|_| std::fmt::Error)?;
        }
        s.finish()
    }
}

pub struct ReadRange<'a> {
    state: ReadRangeState<'a>,
    range: BlockRange,
    content_type: ContentType,
}

enum ReadRangeState<'a> {
    Uninit(&'a Database<'a>),
    Init(Cursor<'a>),
    Finished,
}

impl<'a> ReadRange<'a> {
    fn new(db: &'a Database<'a>, content_type: ContentType, range: BlockRange) -> Self {
        ReadRange {
            state: ReadRangeState::Uninit(db),
            range,
            content_type,
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<Content<'a>>> {
        match &mut self.state {
            ReadRangeState::Finished => Ok(None),
            ReadRangeState::Init(cursor) => match cursor.next().optional()? {
                Some(_) => {
                    let end = ID::new(self.range.head().client, self.range.end());
                    match parse_id(cursor.key()?)? {
                        Some(&id) if id <= end => {
                            let content =
                                Content::new(self.content_type, Cow::Borrowed(cursor.value()?));
                            Ok(Some(content))
                        }
                        _ => {
                            self.state = ReadRangeState::Finished;
                            Ok(None)
                        }
                    }
                }
                None => {
                    self.state = ReadRangeState::Finished;
                    Ok(None)
                }
            },
            ReadRangeState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let key = BlockContentKey::new(*self.range.head());
                let value = match cursor.set_key(key.as_bytes()) {
                    Ok(_) => Content::new(self.content_type, Cow::Borrowed(cursor.value()?)),
                    Err(LmdbError::NOT_FOUND) => {
                        self.state = ReadRangeState::Finished;
                        return Ok(None);
                    }
                    Err(e) => return Err(e.into()),
                };
                self.state = ReadRangeState::Init(cursor);
                Ok(Some(value))
            }
        }
    }
}

impl<'a> Iterator for ReadRange<'a> {
    type Item = crate::Result<Content<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next() {
            Ok(None) => None,
            Ok(Some(content)) => Some(Ok(content)),
            Err(e) => Some(Err(e)),
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
