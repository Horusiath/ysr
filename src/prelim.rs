use crate::block::InsertBlockData;
use crate::content::{BlockContent, ContentRef};
use crate::{lib0, Transaction};
use serde::Serialize;
use smallvec::{smallvec, SmallVec};

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
        let mut buf: SmallVec<[u8; 16]> = smallvec![0, 0, 0, 0];
        lib0::to_writer(&mut buf, self)?;
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_be_bytes());
        insert.init_content(BlockContent::Atom(ContentRef::new(&buf)));
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
