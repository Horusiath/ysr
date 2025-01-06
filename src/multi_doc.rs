use crate::{Store, Transaction};

pub struct MultiDoc<S> {
    store: S,
}

impl<S: Store> MultiDoc<S> {
    pub fn new(store: S) -> Self {
        MultiDoc { store }
    }

    pub fn doc(&self, doc_id: &[u8]) -> Transaction<S> {
        todo!()
    }
}
