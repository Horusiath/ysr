use crate::{Mounted, Transaction};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ops::Deref;

pub type ListRef<Txn> = Mounted<List, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct List;

impl<'tx, 'db> ListRef<&'tx Transaction<'db>> {
    pub fn get<T>(&self, index: usize) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        todo!()
    }

    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn iter<T>(&self) -> Iter<T>
    where
        T: DeserializeOwned,
    {
        todo!()
    }
}

impl<'tx, 'db> ListRef<&'tx mut Transaction<'db>> {
    pub fn insert<T>(&mut self, index: usize, value: T) -> crate::Result<()>
    where
        T: Serialize,
    {
        todo!()
    }

    pub fn push_back<T>(&mut self, value: T) -> crate::Result<()>
    where
        T: Serialize,
    {
        let len = self.len();
        self.insert(len, value)
    }

    pub fn push_front<T>(&mut self, value: T) -> crate::Result<()>
    where
        T: Serialize,
    {
        self.insert(0, value)
    }
}

impl<'tx, 'db> Deref for ListRef<&'tx mut Transaction<'db>> {
    type Target = ListRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const ListRef<&'tx Transaction<'db>>) }
    }
}

pub struct Iter<'a, T> {
    list: &'a ListRef<&'a Transaction<'a>>,
    index: usize,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: DeserializeOwned,
{
    type Item = crate::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
