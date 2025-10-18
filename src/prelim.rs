use crate::block::InsertBlockData;
use crate::Transaction;

pub trait Prelim {
    type Return;

    fn prepare(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return>;
}
