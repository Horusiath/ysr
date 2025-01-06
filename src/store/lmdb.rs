use crate::{MultiDoc, Store};

pub struct Lmdb {}

impl Lmdb {}

impl Store for Lmdb {
    type Transaction = ();

    fn doc_transaction(&self, doc_id: &[u8]) -> crate::Result<Self::Transaction> {}
}

impl MultiDoc<Lmdb> {
    pub fn open_lmdb(env: heed3::Env) -> Self {
        todo!()
    }
}
