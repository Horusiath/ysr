use crate::store::keys::STATE_VECTOR_KEY;
use crate::store::{AsKey, Cursor, CursorEntry, Transaction};
use crate::{ClientID, Clock, MultiDoc, Store};
use rocksdb::{DBIteratorWithThreadMode, OptimisticTransactionDB};
use std::marker::PhantomData;
use zerocopy::{FromBytes, IntoBytes};

impl MultiDoc<RocksDb> {
    pub fn open_rocksdb(db: OptimisticTransactionDB) -> Self {
        MultiDoc::new(RocksDb::new(db))
    }
}

pub struct RocksDb {
    db: OptimisticTransactionDB,
}

impl RocksDb {
    fn new(db: OptimisticTransactionDB) -> Self {
        Self { db }
    }
}

impl From<OptimisticTransactionDB> for RocksDb {
    #[inline]
    fn from(db: OptimisticTransactionDB) -> Self {
        Self::new(db)
    }
}

impl Store for RocksDb {
    type Transaction<'db> = RocksDbTransaction<'db>;

    fn open(&self, doc_id: &[u8]) -> crate::Result<Self::Transaction<'_>> {
        let inner = self.db.transaction();
        Ok(RocksDbTransaction::new(inner, doc_id))
    }
}

pub struct RocksDbTransaction<'db> {
    inner: rocksdb::Transaction<'db, OptimisticTransactionDB>,
    prefix: Vec<u8>,
}

impl<'db> RocksDbTransaction<'db> {
    fn new(inner: rocksdb::Transaction<'db, OptimisticTransactionDB>, doc_id: &[u8]) -> Self {
        Self {
            inner,
            prefix: Vec::from(doc_id),
        }
    }
}

impl<'db> Transaction<'db> for RocksDbTransaction<'db> {
    type Cursor<'tx, K: AsKey> = RocksDbCursor<'tx, K> where Self: 'tx;

    fn commit(self) -> crate::Result<()> {
        Ok(self.inner.commit()?)
    }

    fn rollback(self) -> crate::Result<()> {
        Ok(self.inner.rollback()?)
    }

    fn get<K: AsKey>(&self, key: &K) -> crate::Result<Option<K::Value>> {
        todo!()
    }

    fn prefixed<'tx, K: AsKey>(&'tx mut self, from: K) -> crate::Result<Self::Cursor<'tx, K>> {
        let key = from.as_key();
        let mut prefix = self.prefix.clone();
        prefix.extend_from_slice(key);
        let iter = self.inner.prefix_iterator(prefix);
        Ok(RocksDbCursor::new(iter))
    }

    fn next_sequence_number(&mut self, client_id: &ClientID) -> crate::Result<Clock> {
        let mut key = self.prefix.clone();
        key.extend_from_slice(&STATE_VECTOR_KEY);
        key.extend_from_slice(client_id.as_bytes());
        match self.inner.get_pinned_for_update(key.as_bytes(), true)? {
            None => {
                let clock = Clock::from(0);
                self.inner.put(key.as_bytes(), clock.as_bytes())?;
                Ok(clock)
            }
            Some(mut pinned) => {
                let mut clock = *Clock::ref_from_bytes(pinned.as_bytes())
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                clock += 1;
                self.inner.put(key.as_bytes(), clock.as_bytes())?;
                Ok(clock)
            }
        }
    }
}

pub struct RocksDbCursor<'db, K: AsKey> {
    inner: DBIteratorWithThreadMode<'db, rocksdb::Transaction<'db, OptimisticTransactionDB>>,
    _marker: PhantomData<K>,
}

impl<'db, K> RocksDbCursor<'db, K>
where
    K: AsKey,
{
    fn new(
        inner: DBIteratorWithThreadMode<'db, rocksdb::Transaction<'db, OptimisticTransactionDB>>,
    ) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<'db, K: AsKey> Cursor<K> for RocksDbCursor<'db, K> {
    type Entry = RocksDbCursorEntry<K>;
}

impl<'db, K: AsKey> Iterator for RocksDbCursor<'db, K> {
    type Item = crate::Result<RocksDbCursorEntry<K>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(Err(err)) => Some(Err(err.into())),
            Some(Ok((key, value))) => Some(Ok(RocksDbCursorEntry::new(key, value))),
        }
    }
}

pub struct RocksDbCursorEntry<K: AsKey> {
    key: Box<[u8]>,
    value: Box<[u8]>,
    _marker: PhantomData<K>,
}

impl<K: AsKey> RocksDbCursorEntry<K> {
    fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        RocksDbCursorEntry {
            key,
            value,
            _marker: Default::default(),
        }
    }
}

impl<K: AsKey> CursorEntry<K> for RocksDbCursorEntry<K> {
    #[inline]
    fn key(&self) -> Option<&K::Key> {
        K::parse_key(&self.key)
    }

    #[inline]
    fn value(&self) -> Option<&K::Value> {
        K::parse_value(&self.value)
    }
}

impl From<rocksdb::Error> for crate::Error {
    fn from(err: rocksdb::Error) -> Self {
        crate::Error::Store(err.into())
    }
}

#[cfg(test)]
mod test {
    use crate::store::rocksdb::RocksDb;
    use crate::store::Transaction;
    use crate::{ClientID, StateVector, Store};
    use rocksdb::OptimisticTransactionDB;
    use tempfile::TempDir;

    pub const A: ClientID = ClientID::new(123);
    pub const B: ClientID = ClientID::new(234);
    pub const C: ClientID = ClientID::new(345);

    #[test]
    fn state_vector() {
        let test_db = open_db();
        let store = RocksDb::new(test_db.db);

        let mut tx = store.open(b"test").unwrap();
        assert_eq!(tx.next_sequence_number(&A).unwrap(), 0);
        assert_eq!(tx.next_sequence_number(&A).unwrap(), 1);
        assert_eq!(tx.next_sequence_number(&B).unwrap(), 0);
        assert_eq!(tx.next_sequence_number(&A).unwrap(), 2);
        assert_eq!(tx.next_sequence_number(&B).unwrap(), 1);
        assert_eq!(tx.next_sequence_number(&C).unwrap(), 0);

        let expected = StateVector::from_iter([(A, 2.into()), (B, 1.into()), (C, 0.into())]);

        let sv1 = tx.state_vector().unwrap();
        tx.commit().unwrap();
        assert_eq!(sv1, expected);

        let mut tx = store.open(b"test").unwrap();
        let sv2 = tx.state_vector().unwrap();
        assert_eq!(sv2, expected);
    }

    fn open_db() -> TestDb {
        let temp_dir = TempDir::new().unwrap();
        let db = OptimisticTransactionDB::open_default(temp_dir.path()).unwrap();
        TestDb { db, temp_dir }
    }

    struct TestDb {
        db: OptimisticTransactionDB,
        temp_dir: TempDir,
    }
}
