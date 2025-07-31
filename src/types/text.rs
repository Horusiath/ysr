use crate::{Mounted, Transaction};
use std::fmt::{Display, Formatter};
use std::ops::{Deref, RangeBounds};

pub type TextRef<Txn> = Mounted<Text, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Text;

impl<'tx, 'db> TextRef<&'tx Transaction<'db>> {
    pub fn len(&self) -> usize {
        todo!()
    }
}

impl<'tx, 'db> Display for TextRef<&'tx Transaction<'db>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<'tx, 'db> TextRef<&'tx mut Transaction<'db>> {
    pub fn insert<S>(&mut self, index: usize, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        todo!()
    }

    pub fn remove_range<R>(&mut self, range: R) -> crate::Result<()>
    where
        R: RangeBounds<usize>,
    {
        todo!()
    }
}

impl<'tx, 'db> Deref for TextRef<&'tx mut Transaction<'db>> {
    type Target = TextRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const TextRef<&'tx Transaction<'db>>) }
    }
}
