use crate::StateVector;

#[cfg(feature = "lmdb")]
mod lmdb;

#[cfg(feature = "rocksdb")]
mod rocksdb;

pub trait AsKey {
    type Value;

    fn as_key(&self) -> &[u8];
}

pub trait Store {
    type Transaction: Transaction;

    fn doc_transaction(&self, doc_id: &[u8]) -> crate::Result<Self::Transaction>;
}

pub trait Transaction {
    type Cursor<K: AsKey>: Cursor<K>;

    fn state_vector(&self) -> crate::Result<StateVector>;
    fn range<K: AsKey>(&self, from: Option<K>, to: Option<K>) -> Self::Cursor<K>;
}

pub trait Cursor<K: AsKey>: Iterator<Item = (K, K::Value)> {}
