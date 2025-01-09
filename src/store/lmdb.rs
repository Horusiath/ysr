use crate::store::keys::STATE_VECTOR_KEY;
use crate::store::{AsKey, Cursor, CursorEntry, Transaction};
use crate::{ClientID, Clock, MultiDoc, Store};
use heed::types::Bytes;
use heed::{Database, Env, MdbError, PutFlags, RwPrefix, RwTxn};
use smallvec::{smallvec, smallvec_inline, SmallVec};
use std::borrow::Cow;
use std::io::Write;
use std::marker::PhantomData;
use std::str::from_utf8;
use zerocopy::{FromBytes, IntoBytes};

impl MultiDoc<Lmdb> {
    pub fn open_lmdb(env: Env) -> Self {
        MultiDoc::new(Lmdb::new(env))
    }
}

pub struct Lmdb {
    env: Env,
}

impl Lmdb {
    fn new(env: Env) -> Self {
        Self { env }
    }
}

impl From<Env> for Lmdb {
    #[inline]
    fn from(value: Env) -> Self {
        Self::new(value)
    }
}

impl Store for Lmdb {
    type Transaction<'db> = LmdbTransaction<'db> where Self: 'db;

    fn open(&self, doc_id: &[u8]) -> crate::Result<Self::Transaction<'_>> {
        let db_name = from_utf8(doc_id).map_err(|_| crate::Error::InvalidMapping("db name"))?;
        let mut tx = self.env.write_txn()?;
        let db = self.env.create_database(&mut tx, Some(db_name))?;
        Ok(LmdbTransaction::new(tx, db))
    }
}

pub struct LmdbTransaction<'db> {
    tx: RwTxn<'db>,
    db: Database<Bytes, Bytes>,
}

impl<'db> LmdbTransaction<'db> {
    fn new(tx: RwTxn<'db>, db: Database<Bytes, Bytes>) -> Self {
        LmdbTransaction { tx, db }
    }
}

impl<'db> Transaction<'db> for LmdbTransaction<'db> {
    type Cursor<'tx, K: AsKey> = LmdbCursor<'tx, K> where Self: 'tx;

    fn commit(self) -> crate::Result<()> {
        Ok(self.tx.commit()?)
    }

    fn rollback(self) -> crate::Result<()> {
        drop(self);
        Ok(())
    }

    fn get<K: AsKey>(&self, key: &K) -> crate::Result<Option<K::Value>> {
        todo!()
    }

    fn prefixed<'tx, K: AsKey>(&'tx mut self, from: K) -> crate::Result<Self::Cursor<'tx, K>> {
        let prefix = from.as_key();
        let cursor = self.db.prefix_iter_mut(&mut self.tx, prefix)?;
        Ok(LmdbCursor::new(cursor))
    }

    fn next_sequence_number(&mut self, client_id: &ClientID) -> crate::Result<Clock> {
        let b = client_id.as_bytes();
        let key = smallvec_inline![
            STATE_VECTOR_KEY[0],
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7]
        ];
        match self
            .db
            .get_or_put(&mut self.tx, &key, Clock::from(0).as_bytes())
        {
            Ok(None) => Ok(Clock::from(0)),
            Ok(Some(mut value)) => {
                let mut clock = *Clock::ref_from_bytes(value.as_bytes())
                    .map_err(|_| crate::Error::InvalidMapping("Clock"))?;
                clock += 1;
                self.db.put(&mut self.tx, &key, clock.as_bytes())?;
                Ok(clock)
            }
            Err(err) => Err(err.into()),
        }
    }
}

pub struct LmdbCursor<'tx, K: AsKey> {
    inner: RwPrefix<'tx, Bytes, Bytes>,
    _marker: PhantomData<K>,
}

impl<'tx, K: AsKey> LmdbCursor<'tx, K> {
    fn new(inner: RwPrefix<'tx, Bytes, Bytes>) -> Self {
        Self {
            inner,
            _marker: Default::default(),
        }
    }
}

impl<'tx, K: AsKey> Cursor<K> for LmdbCursor<'tx, K> {
    type Entry = LmdbCursorEntry<'tx, K>;
}

impl<'tx, K: AsKey> Iterator for LmdbCursor<'tx, K> {
    type Item = crate::Result<LmdbCursorEntry<'tx, K>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            None => None,
            Some(Err(err)) => Some(Err(err.into())),
            Some(Ok(x)) => {
                let (key, value) = x;
                Some(Ok(LmdbCursorEntry::new(key, value)))
            }
        }
    }
}

pub struct LmdbCursorEntry<'tx, K: AsKey> {
    key: &'tx [u8],
    value: &'tx [u8],
    _marker: PhantomData<K>,
}

impl<'tx, K: AsKey> LmdbCursorEntry<'tx, K> {
    fn new(key: &'tx [u8], value: &'tx [u8]) -> Self {
        Self {
            key,
            value,
            _marker: Default::default(),
        }
    }
}

impl<'tx, K: AsKey> CursorEntry<K> for LmdbCursorEntry<'tx, K> {
    fn key(&self) -> Option<&K::Key> {
        K::parse_key(&self.key)
    }

    fn value(&self) -> Option<&K::Value> {
        K::parse_value(&self.value)
    }
}

impl From<heed::Error> for crate::Error {
    fn from(value: heed::Error) -> Self {
        Self::Store(value.into())
    }
}

#[cfg(test)]
mod test {
    use crate::store::lmdb::Lmdb;
    use crate::store::Transaction;
    use crate::{ClientID, StateVector, Store};
    use heed::Env;
    use tempfile::TempDir;

    pub const A: ClientID = ClientID::new(123);
    pub const B: ClientID = ClientID::new(234);
    pub const C: ClientID = ClientID::new(345);

    #[test]
    fn state_vector() {
        let test_db = open_db();
        let store = Lmdb::new(test_db.env);

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
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .max_dbs(100)
                .open(temp_dir.path())
                .unwrap()
        };
        TestDb { env, temp_dir }
    }

    struct TestDb {
        env: Env,
        temp_dir: TempDir,
    }
}
