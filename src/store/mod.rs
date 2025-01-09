use crate::{ClientID, Clock, StateVector};
use std::collections::BTreeMap;

#[cfg(feature = "lmdb")]
mod lmdb;

mod keys;
#[cfg(feature = "rocksdb")]
mod rocksdb;

pub trait AsKey {
    type Key;
    type Value;

    fn as_key(&self) -> &[u8];
    fn parse_key(key: &[u8]) -> Option<&Self::Key>;
    fn parse_value(value: &[u8]) -> Option<&Self::Value>;
}

pub trait Store {
    type Transaction<'db>: Transaction<'db>
    where
        Self: 'db;

    fn open(&self, doc_id: &[u8]) -> crate::Result<Self::Transaction<'_>>;
}

pub trait Transaction<'db> {
    type Cursor<'tx, K: AsKey>: Cursor<K>
    where
        Self: 'tx;

    fn commit(self) -> crate::Result<()>;
    fn rollback(self) -> crate::Result<()>;

    fn get<K: AsKey>(&self, key: &K) -> crate::Result<Option<K::Value>>;
    fn prefixed<'tx, K: AsKey>(&'tx mut self, from: K) -> crate::Result<Self::Cursor<'tx, K>>;

    fn next_sequence_number(&mut self, client_id: &ClientID) -> crate::Result<Clock>;
    fn state_vector(&mut self) -> crate::Result<StateVector> {
        let mut cursor = self.prefixed(keys::StateVectorKey)?;
        let mut sv = BTreeMap::new();
        for res in cursor {
            let entry = res?;
            let client_id: &ClientID = entry
                .key()
                .ok_or(crate::Error::InvalidMapping("ClientID"))?;
            let clock: &Clock = entry.value().ok_or(crate::Error::InvalidMapping("Clock"))?;
            sv.insert(*client_id, *clock);
        }
        Ok(StateVector::new(sv))
    }
}

pub trait Cursor<K: AsKey>: Iterator<Item = crate::Result<Self::Entry>> {
    type Entry: CursorEntry<K>;
}

pub trait CursorEntry<K: AsKey> {
    fn key(&self) -> Option<&K::Key>;
    fn value(&self) -> Option<&K::Value>;
}
