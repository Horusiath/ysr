use crate::store::lmdb::store::KEY_PREFIX_META;
use lmdb_rs_m::{Database, MdbError};
use smallvec::SmallVec;

#[repr(transparent)]
pub struct MetaStore<'tx> {
    db: &'tx Database<'tx>,
}

impl<'tx> MetaStore<'tx> {
    pub fn new(db: &'tx Database<'tx>) -> Self {
        Self { db }
    }

    pub fn get(&mut self, key: &str) -> crate::Result<Option<&'tx [u8]>> {
        let key = meta_key(key);
        match self.db.get(&key) {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert(&mut self, key: &str, value: &[u8]) -> crate::Result<()> {
        let key = meta_key(key);
        self.db.set(&key, value)?;
        Ok(())
    }
}

fn meta_key(key: &str) -> SmallVec<[u8; 24]> {
    let mut buf = SmallVec::with_capacity(1 + key.len());
    buf.push(KEY_PREFIX_META);
    buf.extend_from_slice(key.as_bytes());
    buf
}
