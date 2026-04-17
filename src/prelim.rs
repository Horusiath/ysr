use crate::block::InsertBlockData;
use crate::content::{Content, ContentType};
use crate::transaction::WriteTxScope;
use crate::{Clock, Transaction};
use serde::Serialize;
use smallvec::smallvec;

pub trait Prelim {
    type Return;

    fn clock_len(&self) -> Clock;

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()>;

    fn integrate<'tx>(
        self,
        insert: &mut InsertBlockData,
        tx: &mut WriteTxScope<'tx>,
    ) -> crate::Result<Self::Return>;
}

impl<T> Prelim for T
where
    T: Serialize,
{
    type Return = ();

    #[inline]
    fn clock_len(&self) -> Clock {
        Clock::new(1)
    }

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()> {
        insert.block.set_content_type(ContentType::Atom);
        insert.content = smallvec![Content::atom(&self)?];
        Ok(())
    }

    #[inline]
    fn integrate<'tx>(
        self,
        _insert: &mut InsertBlockData,
        _tx: &mut WriteTxScope<'tx>,
    ) -> crate::Result<Self::Return> {
        Ok(())
    }
}
