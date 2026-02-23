use crate::store::block_store::BlockStore;
use crate::store::content_store::ContentStore;
use crate::store::inspect::DbInspector;
use crate::store::intern_strings::InternStringsStore;
use crate::store::map_entries::MapEntriesStore;
use crate::store::meta_store::MetaStore;
use crate::store::state_vector::StateVectorStore;

mod block_store;
mod content_store;
mod inspect;
mod intern_strings;
pub mod lmdb;
mod map_entries;
mod meta_store;
mod state_vector;

pub trait Db<'tx> {
    fn meta(&self) -> crate::Result<MetaStore<'_>>;
    fn blocks(&self) -> crate::Result<BlockStore<'_>>;
    fn contents(&self) -> crate::Result<ContentStore<'_>>;
    fn intern_strings(&self) -> crate::Result<InternStringsStore<'_>>;
    fn map_entries(&self) -> crate::Result<MapEntriesStore<'_>>;
    fn state_vector(&self) -> crate::Result<StateVectorStore<'_>>;
    fn inspect(&self) -> DbInspector<'_> {
        DbInspector::new(self)
    }
}

impl<'tx> Db<'tx> for lmdb_rs_m::Database<'tx> {
    fn meta(&self) -> crate::Result<MetaStore<'_>> {
        Ok(MetaStore::new(self))
    }

    fn blocks(&self) -> crate::Result<BlockStore<'_>> {
        let cursor = self.new_cursor()?;
        Ok(BlockStore::new(cursor))
    }

    fn contents(&self) -> crate::Result<ContentStore<'_>> {
        let cursor = self.new_cursor()?;
        Ok(ContentStore::new(cursor))
    }

    #[inline]
    fn intern_strings(&self) -> crate::Result<InternStringsStore<'_>> {
        Ok(InternStringsStore::new(self))
    }

    fn map_entries(&self) -> crate::Result<MapEntriesStore<'_>> {
        let cursor = self.new_cursor()?;
        Ok(MapEntriesStore::new(cursor))
    }

    fn state_vector(&self) -> crate::Result<StateVectorStore<'_>> {
        let cursor = self.new_cursor()?;
        Ok(StateVectorStore::new(cursor))
    }
}
