mod lmdb;

use crate::block::BlockMut;
use crate::{ClientID, Clock, StateVector};
use std::collections::BTreeMap;

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

    fn put_block(&mut self, block: BlockMut) -> crate::Result<()>;
    fn prefixed<'tx, K: AsKey>(&'tx self, prefix: &K) -> crate::Result<Self::Cursor<'tx, K>>;
    fn next_sequence_number(&mut self, client_id: &ClientID) -> crate::Result<Clock>;
    fn state_vector(&self) -> crate::Result<StateVector>;
}

pub trait Cursor<K: AsKey>: Iterator<Item = crate::Result<Self::Entry>> {
    type Entry: CursorEntry<K>;
}

pub trait CursorEntry<K: AsKey> {
    fn key(&self) -> Option<&K::Key>;
    fn value(&self) -> Option<&K::Value>;
}
