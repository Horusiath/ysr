use crate::lmdb::{Cursor, Database};
use crate::store::block_store::BlockStore;
use crate::store::content_store::ContentStore;
use crate::store::delete_set::DeleteSetStore;
use crate::store::intern_strings::InternStringsStore;
pub(crate) use crate::store::map_entries::MapEntriesStore;
use crate::store::meta_store::MetaStore;
use crate::store::state_vector::StateVectorStore;
use std::fmt::{Debug, Formatter};

pub(crate) mod block_store;
pub(crate) mod content_store;
mod delete_set;
pub mod inspect;
pub(crate) mod intern_strings;
pub(crate) mod map_entries;
pub(crate) mod meta_store;
pub(crate) mod state_vector;

pub(super) const KEY_PREFIX_META: u8 = 0x00;
pub(super) const KEY_PREFIX_INTERN_STR: u8 = 0x01;
pub(super) const KEY_PREFIX_STATE_VECTOR: u8 = 0x02;
pub(super) const KEY_PREFIX_BLOCK: u8 = 0x03;
pub(super) const KEY_PREFIX_MAP: u8 = 0x04;
pub(super) const KEY_PREFIX_CONTENT: u8 = 0x05;

pub trait Db<'tx> {
    fn cursor(&self) -> crate::Result<Cursor<'tx>>;
    fn meta(&self) -> MetaStore<'_>;
    fn blocks(&self) -> BlockStore<'_>;
    fn contents(&self) -> ContentStore<'_>;
    fn intern_strings(&self) -> InternStringsStore<'_>;
    fn map_entries(&self) -> MapEntriesStore<'_>;
    fn state_vector(&self) -> StateVectorStore<'_>;
    fn delete_set(&self) -> DeleteSetStore<'_>;
}

impl<'tx> Db<'tx> for Database<'tx> {
    fn cursor(&self) -> crate::Result<Cursor<'tx>> {
        Ok(self.cursor()?)
    }

    fn meta(&self) -> MetaStore<'_> {
        MetaStore::new(self)
    }

    fn blocks(&self) -> BlockStore<'_> {
        BlockStore::new(self)
    }

    fn contents(&self) -> ContentStore<'_> {
        ContentStore::new(self)
    }

    #[inline]
    fn intern_strings(&self) -> InternStringsStore<'_> {
        InternStringsStore::new(self)
    }

    fn map_entries(&self) -> MapEntriesStore<'_> {
        MapEntriesStore::new(self)
    }

    fn state_vector(&self) -> StateVectorStore<'_> {
        StateVectorStore::new(self)
    }

    fn delete_set(&self) -> DeleteSetStore<'_> {
        DeleteSetStore::new(self)
    }
}

pub(super) struct ReadableBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> ReadableBytes<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
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
