use crate::block_reader::BlockReader;
use crate::read::Decoder;
use crate::{StateVector, Store};
use smallvec::SmallVec;
use std::io::{Read, Write};

pub(crate) struct TransactionState {
    origin: Option<Origin>,
}

impl TransactionState {
    fn new() -> Self {
        todo!()
    }

    fn commit<'db, T: crate::store::Transaction<'db>>(&self, tx: &T) -> crate::Result<()> {
        todo!()
    }
}

pub struct Transaction<'db, S: Store + 'db> {
    inner: S::Transaction<'db>,
    state: Option<Box<TransactionState>>,
}

impl<'db, S: Store> Transaction<'db, S> {
    pub(crate) fn new(inner: S::Transaction<'db>) -> Self {
        Self { inner, state: None }
    }

    pub fn split_mut(&mut self) -> (&mut S::Transaction<'db>, &mut TransactionState) {
        let state = self
            .state
            .get_or_insert_with(|| Box::new(TransactionState::new()));
        (&mut self.inner, state)
    }

    pub fn state_vector(&mut self) -> crate::Result<StateVector> {
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

    pub fn apply_update<D: Decoder>(&mut self, decoder: &mut D) -> crate::Result<()> {
        let block_reader = BlockReader::new(decoder)?;
        todo!()
    }

    pub fn commit(mut self) -> crate::Result<()> {
        use crate::store::Transaction;
        if let Some(state) = self.state.take() {
            // commit the transaction
            state.commit(&self.inner)?;
            self.inner.commit()
        } else {
            Ok(()) // readonly or already committed transaction
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Origin(SmallVec<[u8; 16]>);
