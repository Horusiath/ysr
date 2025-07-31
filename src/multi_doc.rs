use crate::transaction::Origin;
use crate::Transaction;
use lmdb_rs_m::{DbFlags, Environment};

pub struct MultiDoc {
    env: Environment,
}

impl MultiDoc {
    pub fn new(env: Environment) -> Self {
        MultiDoc { env }
    }

    pub fn transact_mut<O: Into<Origin>>(
        &self,
        doc_id: &str,
        origin: Option<O>,
    ) -> crate::Result<Transaction<'_>> {
        let handle = self.env.create_db(doc_id, DbFlags::DbCreate)?;
        let tx = self.env.new_transaction()?;
        Ok(Transaction::new(tx, handle, origin.map(Into::into)))
    }
}

impl From<Environment> for MultiDoc {
    #[inline]
    fn from(value: Environment) -> Self {
        Self::new(value)
    }
}
