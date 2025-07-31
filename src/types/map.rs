use crate::node::NodeType;
use crate::types::Capability;
use crate::{Mounted, Transaction};
use std::ops::Deref;

pub type MapRef<Txn> = Mounted<Map, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Map;

impl Capability for Map {
    fn node_type() -> NodeType {
        NodeType::Map
    }
}

impl<'tx, 'db> MapRef<&'tx Transaction<'db>> {
    pub fn get<K, V>(&self, key: K) -> crate::Result<V>
    where
        K: AsRef<str>,
        V: serde::de::DeserializeOwned,
    {
        todo!()
    }

    pub fn len(&self) -> usize {
        self.block.clock_len().get() as usize
    }

    pub fn contains_key<K>(&self, key: K) -> bool
    where
        K: AsRef<str>,
    {
        todo!()
    }

    pub fn iter(&self) -> Iter {
        todo!()
    }
}

impl<'tx, 'db> MapRef<&'tx mut Transaction<'db>> {
    pub fn insert<K, V>(&mut self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<str>,
        V: serde::Serialize,
    {
        todo!()
    }

    pub fn remove<K>(&mut self, key: K) -> crate::Result<Option<()>>
    where
        K: AsRef<str>,
    {
        todo!()
    }
}

impl<'tx, 'db> Deref for MapRef<&'tx mut Transaction<'db>> {
    type Target = MapRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const MapRef<&'tx Transaction<'db>>) }
    }
}

pub struct Iter<'a> {
    map: &'a MapRef<&'a Transaction<'a>>,
}

impl<'a> Iter<'a> {
    pub fn seek(&mut self, key: &str) -> crate::Result<()> {
        todo!()
    }

    pub fn next(&mut self) -> crate::Result<()> {
        todo!()
    }

    pub fn next_back(&mut self) -> crate::Result<()> {
        todo!()
    }

    pub fn key(&self) -> Option<&str> {
        todo!()
    }

    pub fn value<V>(&self) -> crate::Result<V>
    where
        V: serde::de::DeserializeOwned,
    {
        todo!()
    }
}
