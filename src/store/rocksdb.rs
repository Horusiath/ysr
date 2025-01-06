use crate::{MultiDoc, Store};

pub struct RocksDb {}

impl RocksDb {}

impl Store for RocksDb {
    type Transaction = ();
}

impl MultiDoc<RocksDb> {
    pub fn open_rocksdb(db: rocksdb::DB) -> Self {
        todo!()
    }
}
