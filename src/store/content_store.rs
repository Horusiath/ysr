use crate::block_reader::BlockRange;
use crate::content::{Content, ContentType};
use crate::store::lmdb::store::KEY_PREFIX_CONTENT;
use crate::{Clock, ID, Optional};
use lmdb_rs_m::{Cursor, MdbError, MdbValue, ToMdbValue};
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
    pub fn current_id(&mut self) -> crate::Result<Option<&ID>> {
        let key: &[u8] = self.cursor.get_key()?;
        if key[0] != Self::PREFIX {
            return Ok(None);
        }

        let id = ID::parse(&key[1..])?;
        Ok(Some(id))
    }

    pub fn seek(&mut self, id: ID) -> crate::Result<Option<&'a [u8]>> {
        let key = BlockContentKey::new(id);
        match self.cursor.to_key(&key) {
            Ok(_) => {
                let value: &'a [u8] = self.cursor.get_value()?;
                Ok(Some(value))
            }
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn read_range<'b: 'a>(
        &'b mut self,
        content_type: ContentType,
        range: BlockRange,
    ) -> ReadRange<'b> {
        ReadRange::new(self, content_type, range)
    }

    pub fn insert(&mut self, id: &ID, content: &[Content<'_>]) -> crate::Result<()> {
        let mut id = *id;
        for content in content {
            let key = BlockContentKey::new(id);
            self.cursor.set(&key, content.bytes(), 0)?;
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
        let mut curr = *range.head();
        let key = BlockContentKey::new(curr);
        self.cursor.to_key(&key)?;
        self.cursor.del()?;
        let mut deleted_entries = 1;

        if is_multipart {
            let end = ID::new(curr.client, range.end());
            while curr != end {
                self.cursor.to_next_key()?;
                curr = match self.current_id()? {
                    Some(&id) if id != end => id,
                    _ => break,
                };

                self.cursor.del()?;
                deleted_entries += 1;
            }
        }
        Ok(deleted_entries)
    }

    pub fn iter(&mut self) -> Iter<'a> {
        Iter { store: self }
    }

    pub fn inspect(&mut self) -> Inspect<'a> {
        Inspect { store: self }
    }
}

pub struct Iter<'a> {
    store: &'a mut ContentStore<'a>,
}

impl<'a> Iter<'a> {
    pub fn next(&mut self) -> crate::Result<Option<(&'a ID, &'a [u8])>> {
        match self.store.current_id()? {
            None => Ok(None),
            Some(id) => {
                let value: &'a [u8] = self.store.cursor.get_value()?;
                self.store.cursor.to_next_key().optional()?;
                Ok(Some((id, value)))
            }
        }
    }
}

pub struct Inspect<'tx> {
    store: &'tx mut ContentStore<'tx>,
}

impl<'tx> Debug for Inspect<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();

        let mut i = self.store.iter();
        while let Some((id, content)) = i.next().map_err(|_| std::fmt::Error)? {
            s.key(id);
            s.value(&ReadableBytes::new(content));
        }

        s.finish()
    }
}

struct ReadableBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> ReadableBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> Debug for ReadableBytes<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "b\"")?;
        for &b in self.bytes {
            // https://doc.rust-lang.org/reference/tokens.html#byte-escapes
            if b == b'\n' {
                write!(f, "\\n")?;
            } else if b == b'\r' {
                write!(f, "\\r")?;
            } else if b == b'\t' {
                write!(f, "\\t")?;
            } else if b == b'\\' || b == b'"' {
                write!(f, "\\{}", b as char)?;
            } else if b == b'\0' {
                write!(f, "\\0")?;
            // ASCII printable
            } else if (0x20..0x7f).contains(&b) {
                write!(f, "{}", b as char)?;
            } else {
                write!(f, "\\x{:02x}", b)?;
            }
        }
        write!(f, "\"")?;
        Ok(())
    }
}

pub struct ReadRange<'a> {
    store: &'a mut ContentStore<'a>,
    range: BlockRange,
    content_type: ContentType,
    initialized: bool,
}

impl<'a> ReadRange<'a> {
    fn new(store: &'a mut ContentStore<'a>, content_type: ContentType, range: BlockRange) -> Self {
        ReadRange {
            store,
            range,
            content_type,
            initialized: false,
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<Content<'a>>> {
        if !self.initialized {
            if self.initialise()? {
                self.initialized = true;
            } else {
                return Ok(None);
            }
        } else {
            self.store.cursor.to_next_key()?;
        };

        match self.store.current_range()? {
            Some(&range)
                if self.range.head().client == range.head().client
                    && self.range.head().clock <= range.end() =>
            {
                let value: &'a [u8] = self.store.cursor.get_value()?;
                let content = Content::new(self.content_type, value);
                Ok(Some(content)) //TODO: implement content slicing when block range intersects content boundaries
            }
            _ => Ok(None), // we reached the end
        }
    }

    fn initialise(&mut self) -> crate::Result<bool> {
        match self.store.current_range()? {
            Some(current) if current.head() == self.range.head() => Ok(Some(Clock::new(0))), // cursor is in correct position
            _ => self.store.seek(*self.range.head()), // we need to reset cursor position
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

impl ToMdbValue for BlockContentKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        let ptr = std::ptr::from_ref(self) as *const _;
        unsafe { MdbValue::new(ptr, size_of::<Self>()) }
    }
}
