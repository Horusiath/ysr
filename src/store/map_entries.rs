use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::node::NodeID;
use crate::store::KEY_PREFIX_MAP;
use crate::{ID, U32};
use smallvec::{ExtendFromSlice, SmallVec};
use std::fmt::{Debug, Formatter};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct MapEntriesStore<'tx> {
    db: Database<'tx>,
}

impl<'tx> MapEntriesStore<'tx> {
    pub const PREFIX: u8 = KEY_PREFIX_MAP;
    pub fn new(db: Database<'tx>) -> Self {
        Self { db }
    }

    pub fn insert(&self, node_id: &NodeID, key: &str, id: &ID) -> crate::Result<()> {
        let key = MapKey::create(node_id, key);
        self.db.put(key.as_bytes(), id.as_bytes())?;
        Ok(())
    }

    pub fn get(&self, node_id: &NodeID, key: &str) -> crate::Result<Option<&'tx ID>> {
        let key = MapKey::create(node_id, key);
        match self.db.get(key.as_bytes()) {
            Ok(value) => Ok(Some(ID::parse(value)?)),
            Err(LmdbError::NOT_FOUND) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    pub(crate) fn insert_first(
        &self,
        node_id: NodeID,
        hash: U32,
        block_id: &ID,
    ) -> crate::Result<()> {
        let mut cursor = self.db.cursor()?;
        let key = HashKeyPrefix::new(node_id, hash);
        cursor.set_range(key.as_bytes())?;
        cursor.put_current(block_id.as_bytes())?;
        Ok(())
    }

    pub fn keys_for_hash(&self, node_id: NodeID, hash: crate::U32) -> HashKeys<'tx> {
        HashKeys::new(self.db, node_id, hash)
    }

    pub fn entries(&self, node_id: &NodeID) -> MapEntries<'tx> {
        MapEntries::new(self.db, *node_id)
    }

    pub fn remove_all(&self, node_id: &NodeID) -> crate::Result<usize> {
        let key = MapEntriesKey::new(*node_id);
        let mut cursor = self.db.cursor()?;
        match cursor.set_range(key.as_bytes()) {
            Ok(_) => { /* cursor position set */ }
            Err(LmdbError::NOT_FOUND) => return Ok(0),
            Err(e) => return Err(e.into()),
        }

        let mut deleted_entries = 0;
        while let Some(key) = MapKey::parse(cursor.key()?) {
            if key.node_id() != node_id {
                break;
            }

            cursor.del()?;
            deleted_entries += 1;
            match cursor.next() {
                Ok(_) => {}
                Err(LmdbError::NOT_FOUND) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(deleted_entries)
    }

    pub fn iter(&self) -> Iter<'tx> {
        Iter::new(self.db)
    }

    pub fn inspect(&self) -> Inspector<'tx> {
        Inspector { db: self.db }
    }
}

pub struct HashKeys<'tx> {
    prefix: HashKeyPrefix,
    state: HashKeysState<'tx>,
}

#[repr(C, packed)]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes, Immutable, KnownLayout)]
struct HashKeyPrefix {
    tag: u8,
    node_id: NodeID,
    hash: crate::U32,
}

impl HashKeyPrefix {
    pub fn new(node_id: NodeID, hash: crate::U32) -> Self {
        HashKeyPrefix {
            tag: MapEntriesStore::PREFIX,
            node_id,
            hash,
        }
    }
}

enum HashKeysState<'tx> {
    Uninit(Database<'tx>),
    Init(Cursor<'tx>),
    Finished,
}

impl<'tx> HashKeys<'tx> {
    pub fn new(db: Database<'tx>, node_id: NodeID, hash: crate::U32) -> Self {
        let key = HashKeyPrefix::new(node_id, hash);

        HashKeys {
            prefix: key,
            state: HashKeysState::Uninit(db),
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<(&'tx str, &'tx ID)>> {
        match &mut self.state {
            HashKeysState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                match cursor.set_range(self.prefix.as_bytes()) {
                    Ok(_) => {
                        let key: &'tx [u8] = cursor.key()?;
                        if !key.starts_with(self.prefix.as_bytes()) {
                            self.finish()
                        } else {
                            let value: &'tx ID = ID::parse(cursor.value()?)?;
                            let str: &'tx [u8] = &key[size_of::<HashKeyPrefix>()..];
                            let str = unsafe { std::str::from_utf8_unchecked(str) };
                            self.state = HashKeysState::Init(cursor);
                            Ok(Some((str, value)))
                        }
                    }
                    Err(LmdbError::NOT_FOUND) => self.finish(),
                    Err(e) => Err(e.into()),
                }
            }
            HashKeysState::Init(cursor) => match cursor.next() {
                Ok(_) => {
                    let key: &'tx [u8] = cursor.key()?;
                    if !key.starts_with(self.prefix.as_bytes()) {
                        self.finish()
                    } else {
                        let value: &'tx ID = ID::parse(cursor.value()?)?;
                        let str: &'tx [u8] = &key[size_of::<HashKeyPrefix>()..];
                        let str = unsafe { std::str::from_utf8_unchecked(str) };
                        Ok(Some((str, value)))
                    }
                }
                Err(LmdbError::NOT_FOUND) => self.finish(),
                Err(e) => Err(e.into()),
            },
            HashKeysState::Finished => Ok(None),
        }
    }

    fn finish(&mut self) -> crate::Result<Option<(&'tx str, &'tx ID)>> {
        self.state = HashKeysState::Finished;
        Ok(None)
    }
}

pub struct MapEntries<'tx> {
    state: MapEntriesState<'tx>,
    node_id: NodeID,
}

enum MapEntriesState<'tx> {
    Uninit(Database<'tx>),
    Init(Cursor<'tx>),
    Finished,
}

impl<'tx> MapEntries<'tx> {
    pub fn new(db: Database<'tx>, node_id: NodeID) -> Self {
        MapEntries {
            state: MapEntriesState::Uninit(db),
            node_id,
        }
    }

    pub fn block_id(&mut self) -> crate::Result<&'tx ID> {
        if let MapEntriesState::Init(cursor) = &mut self.state {
            let value: &'tx [u8] = cursor.value()?;
            let id: &'tx ID = ID::parse(value)?;
            Ok(id)
        } else {
            Err(crate::Error::NotFound)
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<MapKey<'tx>>> {
        match &mut self.state {
            MapEntriesState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let key = MapEntriesKey::new(self.node_id);
                match cursor.set_range(key.as_bytes()) {
                    Err(LmdbError::NOT_FOUND) => {
                        self.state = MapEntriesState::Finished;
                        Ok(None)
                    }
                    Err(e) => Err(e.into()),
                    Ok(_) => {
                        if let Some(key) = MapKey::parse(cursor.key()?)
                            && key.node_id() == &self.node_id
                        {
                            self.state = MapEntriesState::Init(cursor);
                            Ok(Some(key))
                        } else {
                            self.state = MapEntriesState::Finished;
                            Ok(None)
                        }
                    }
                }
            }
            MapEntriesState::Init(cursor) => match cursor.next() {
                Ok(_) => {
                    if let Some(key) = MapKey::parse(cursor.key()?)
                        && key.node_id() == &self.node_id
                    {
                        Ok(Some(key))
                    } else {
                        self.state = MapEntriesState::Finished;
                        Ok(None)
                    }
                }
                Err(LmdbError::NOT_FOUND) => {
                    self.state = MapEntriesState::Finished;
                    Ok(None)
                }
                Err(e) => Err(e.into()),
            },
            MapEntriesState::Finished => Ok(None),
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

    pub fn key_hash(&self) -> &crate::U32 {
        let slice = &self.data
            [(1 + size_of::<NodeID>())..(1 + size_of::<NodeID>() + size_of::<crate::U32>())];
        crate::U32::ref_from_bytes(slice).unwrap()
    }

    pub fn key(&self) -> &'tx str {
        let slice = &self.data[(1 + size_of::<NodeID>() + size_of::<crate::U32>())..];
        unsafe { std::str::from_utf8_unchecked(slice) }
    }

    fn create(node_id: &NodeID, key: &str) -> SmallVec<[u8; 16]> {
        let hash = crate::U32::new(twox_hash::XxHash32::oneshot(0, key.as_bytes()));
        let mut buf =
            SmallVec::with_capacity(1 + size_of::<NodeID>() + size_of::<crate::U32>() + key.len());
        buf.push(KEY_PREFIX_MAP);
        buf.extend_from_slice(node_id.as_bytes());
        buf.extend_from_slice(hash.as_ref());
        buf.extend_from_slice(key.as_bytes());
        buf
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

pub struct Iter<'tx> {
    state: IterState<'tx>,
}

enum IterState<'tx> {
    Uninit(Database<'tx>),
    Init(Cursor<'tx>),
    Finished,
}

impl<'tx> Iter<'tx> {
    pub fn new(db: Database<'tx>) -> Self {
        Iter {
            state: IterState::Uninit(db),
        }
    }
    pub fn next(&mut self) -> crate::Result<Option<(MapKey<'tx>, &'tx ID)>> {
        match &mut self.state {
            IterState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                match cursor.set_range(&[MapEntriesStore::PREFIX]) {
                    Err(LmdbError::NOT_FOUND) => {
                        self.state = IterState::Finished;
                        Ok(None)
                    }
                    Err(e) => Err(e.into()),
                    Ok(_) => {
                        if let Some(key) = MapKey::parse(cursor.key()?) {
                            let value: &'tx [u8] = cursor.value()?;
                            let id: &'tx ID = ID::parse(value)?;
                            self.state = IterState::Init(cursor);
                            Ok(Some((key, id)))
                        } else {
                            self.state = IterState::Finished;
                            Ok(None)
                        }
                    }
                }
            }
            IterState::Init(cursor) => match cursor.next() {
                Ok(_) => {
                    if let Some(key) = MapKey::parse(cursor.key()?) {
                        let value: &'tx [u8] = cursor.value()?;
                        let id: &'tx ID = ID::parse(value)?;
                        Ok(Some((key, id)))
                    } else {
                        self.state = IterState::Finished;
                        Ok(None)
                    }
                }
                Err(LmdbError::NOT_FOUND) => {
                    self.state = IterState::Finished;
                    Ok(None)
                }
                Err(e) => Err(e.into()),
            },
            IterState::Finished => Ok(None),
        }
    }
}

pub struct Inspector<'tx> {
    db: Database<'tx>,
}

impl<'tx> Debug for Inspector<'tx> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_map();
        let mut iter = Iter::new(self.db);
        while let Some((key, value)) = iter.next().map_err(|_| std::fmt::Error)? {
            s.key(&key);
            s.value(value);
        }
        s.finish()
    }
}
