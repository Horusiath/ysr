use crate::{StateVector, Store};
use std::io::{Read, Write};

pub(crate) struct TransactionState {}

pub struct Transaction<S: Store> {
    inner: S::Transaction,
    state: Option<Box<TransactionState>>,
}

impl<S: Store> Transaction<S> {
    pub(crate) fn new(inner: S::Transaction) -> Self {
        Self { inner, state: None }
    }

    pub fn split_mut(&mut self) -> (&mut S::Transaction, &mut TransactionState) {
        let state = self
            .state
            .get_or_insert_with(|| Box::new(TransactionState {}));
        (&mut self.inner, state)
    }

    pub fn state_vector(&self) -> crate::Result<StateVector> {
        use crate::store::Transaction;
        self.inner.state_vector()
    }

    pub fn create_update<W: Write>(
        &self,
        since: &StateVector,
        writer: &mut W,
    ) -> crate::Result<()> {
        todo!()
    }

    pub fn apply_update<R: Read>(&mut self, reader: &mut R) -> crate::Result<()> {
        todo!()
    }

    pub fn commit(self) {
        todo!()
    }
}
