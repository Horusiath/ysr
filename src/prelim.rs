use crate::block::InsertBlockData;
use crate::content::{BlockContent, ContentType};
use crate::{lib0, Transaction};
use serde::Serialize;
use smallvec::smallvec;

pub trait Prelim {
    type Return;

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()>;

    fn integrate(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return>;
}

impl<T> Prelim for T
where
    T: Serialize,
{
    type Return = ();

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()> {
        let mut content = BlockContent::new(ContentType::Atom);
        lib0::to_writer(&mut content, self)?;
        insert.content = smallvec![content];
        Ok(())
    }

    #[inline]
    fn integrate(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return> {
        Ok(())
    }
}
