use crate::node::NodeID;
use crate::store::lmdb::store::KEY_PREFIX_MAP;
use crate::{ID, Optional};
use lmdb_rs_m::{MdbError, MdbValue, ToMdbValue};
use smallvec::SmallVec;
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
pub struct MapEntriesStore<'tx> {
    cursor: lmdb_rs_m::Cursor<'tx>,
}

impl<'tx> MapEntriesStore<'tx> {
    pub const PREFIX: u8 = KEY_PREFIX_MAP;
    pub fn new(cursor: lmdb_rs_m::Cursor<'tx>) -> Self {
        Self { cursor }
    }

    pub fn current_key(&mut self) -> crate::Result<Option<MapKey<'tx>>> {
        let key: &'tx [u8] = self.cursor.get_key()?;
        match MapKey::parse(key) {
            None => Ok(None),
            Some(key) => Ok(Some(key)),
        }
    }

    pub fn current_entry(&mut self) -> crate::Result<Option<(MapKey<'tx>, &'tx ID)>> {
        match self.current_key()? {
            None => Ok(None),
            Some(key) => {
                let value: &'tx [u8] = self.cursor.get_value()?;
                let id: &'tx ID = ID::parse(value)?;
                Ok(Some((key, id)))
            }
        }
    }

    pub fn insert(&mut self, node_id: &NodeID, key: &str, id: &ID) -> crate::Result<()> {
        let key = entry_key(node_id, key);
        self.cursor.set(&key.as_bytes(), id.as_bytes(), 0)?;
        Ok(())
    }

    pub fn get(&mut self, node_id: &NodeID, key: &str) -> crate::Result<Option<&'tx ID>> {
        let key = entry_key(node_id, key);
        match self.cursor.to_key(&key.as_bytes()) {
            Ok(_) => {
                let value: &'tx [u8] = &self.cursor.get_value()?;
                Ok(Some(ID::parse(value)?))
            }
            Err(MdbError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn entries(&mut self, node_id: &NodeID) -> MapEntries<'tx> {
        MapEntries::new(self, *node_id)
    }

    pub fn remove_all(&mut self, node_id: &NodeID) -> crate::Result<usize> {
        let key = MapEntriesKey::new(*node_id);
        match self.cursor.to_gte_key(&key.as_bytes()) {
            Ok(_) => { /* cursor position set */ }
            Err(MdbError::NotFound) => return Ok(0),
            Err(e) => return Err(e.into()),
        }

        let mut deleted_entries = 0;
        while let Some(current_key) = self.current_key()? {
            if current_key.node_id() != node_id {
                break;
            }

            self.cursor.del()?;
            deleted_entries += 1;
            match self.cursor.to_next_key() {
                Ok(_) => {}
                Err(MdbError::NotFound) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(deleted_entries)
    }

    pub fn iter(&mut self) -> Iter<'tx> {
        Iter { store: self }
    }

    pub fn inspect(&mut self) -> Inspector<'tx> {
        Inspector { store: self }
    }
}

fn entry_key(node_id: &NodeID, key: &str) -> SmallVec<[u8; 16]> {
    let mut key = SmallVec::with_capacity(1 + size_of::<NodeID>() + key.len());
    key.push(KEY_PREFIX_MAP);
    key.extend_from_slice(node_id.as_bytes());
    key.extend_from_slice(key.as_bytes());
    key
}

pub struct MapEntries<'tx> {
    store: &'tx mut MapEntriesStore<'tx>,
    node_id: NodeID,
    initialised: bool,
}

impl<'tx> MapEntries<'tx> {
    pub fn new(store: &'tx mut MapEntriesStore<'tx>, node_id: NodeID) -> Self {
        MapEntries {
            store,
            node_id,
            initialised: false,
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<MapKey<'tx>>> {
        if !self.initialised {
            if !self.initialise()? {
                return Ok(None);
            }
            self.initialised = true;
        } else {
            match self.store.cursor.to_next_key() {
                Ok(_) => {}
                Err(MdbError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        let key = self.store.current_key()?;
        Ok(key)
    }

    pub fn entry(&mut self) -> crate::Result<Option<(MapKey<'tx>, &'tx ID)>> {
        let key = match self.store.current_key()? {
            Some(key) => key,
            None => return Ok(None),
        };
        let value: &'tx [u8] = self.store.cursor.get_value()?;
        let id: &'tx ID = ID::parse(value)?;
        Ok(Some((key, id)))
    }

    fn initialise(&mut self) -> crate::Result<bool> {
        let key = MapEntriesKey::new(self.node_id);
        match self.store.cursor.to_gte_key(&key) {
            Ok(_) => Ok(true),
            Err(MdbError::NotFound) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

#[repr(transparent)]
pub struct MapKey<'tx> {
    data: &'tx [u8],
}

impl<'tx> MapKey<'tx> {
    pub fn parse(bytes: &'tx [u8]) -> Option<MapKey<'tx>> {
        if bytes[0] != KEY_PREFIX_MAP {
            return None;
        }
        Some(MapKey { data: bytes })
    }

    pub fn node_id(&self) -> &NodeID {
        let slice = &self.data[1..(1 + size_of::<NodeID>())];
        NodeID::parse(slice).unwrap()
    }

    pub fn key(&self) -> &'tx str {
        let slice = &self.data[(1 + size_of::<NodeID>())..];
        unsafe { std::str::from_utf8_unchecked(slice) }
    }
}

impl<'tx> Debug for MapKey<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:'{}'", self.node_id(), self.key())
    }
}

#[repr(C, packed)]
#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Debug, PartialEq, Eq)]
pub struct MapEntriesKey {
    tag: u8,
    node_id: NodeID,
}

impl MapEntriesKey {
    pub fn new(node_id: NodeID) -> Self {
        Self {
            tag: KEY_PREFIX_MAP,
            node_id,
        }
    }
}

impl ToMdbValue for MapEntriesKey {
    fn to_mdb_value(&self) -> MdbValue<'_> {
        MdbValue::new_from_sized(self)
    }
}

pub struct Iter<'tx> {
    store: &'tx mut MapEntriesStore<'tx>,
}

impl<'tx> Iter<'tx> {
    pub fn next(&mut self) -> crate::Result<Option<(MapKey<'tx>, &'tx ID)>> {
        match self.store.current_key()? {
            None => Ok(None),
            Some(key) => {
                let value: &'tx [u8] = self.store.cursor.get_value()?;
                let id: &'tx ID = ID::parse(value)?;
                self.store.cursor.to_next_key().optional()?;
                Ok(Some((key, id)))
            }
        }
    }
}

pub struct Inspector<'tx> {
    store: &'tx mut MapEntriesStore<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();
        let mut iter = self.store.iter();
        while let Some((key, value)) = iter.next().map_err(|_| std::fmt::Error)? {
            s.key(&key);
            s.value(value);
        }
        s.finish()
    }
}
