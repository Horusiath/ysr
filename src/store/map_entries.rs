use crate::lmdb::{Cursor, Database, Error as LmdbError};
use crate::node::NodeID;
use crate::store::KEY_PREFIX_MAP;
use crate::{ID, U32};
use smallvec::SmallVec;
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
        let (found_key, _) = cursor.set_range(key.as_bytes())?;
        cursor.put_current(found_key, block_id.as_bytes())?;
        Ok(())
    }

    pub fn keys_for_hash(&self, node_id: NodeID, hash: crate::U32) -> HashKeys<'tx> {
        HashKeys::new(self.db, node_id, hash)
    }

    pub fn entries(&self, node_id: &NodeID) -> MapEntries<'tx> {
        MapEntries::new(self.db, *node_id)
    }

    #[allow(unused)]
    pub fn remove_all(&self, node_id: &NodeID) -> crate::Result<usize> {
        let key = MapEntriesKey::new(*node_id);
        let mut cursor = self.db.cursor()?;
        let (mut k, _) = match cursor.set_range(key.as_bytes()) {
            Ok(kv) => kv,
            Err(LmdbError::NOT_FOUND) => return Ok(0),
            Err(e) => return Err(e.into()),
        };

        let mut deleted_entries = 0;
        loop {
            match MapKey::parse(k) {
                Some(key) if key.node_id() == node_id => {
                    cursor.del()?;
                    deleted_entries += 1;
                }
                _ => break,
            }
            match cursor.next() {
                Ok((next_key, _)) => k = next_key,
                Err(LmdbError::NOT_FOUND) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(deleted_entries)
    }

    #[allow(unused)]
    pub fn iter(&self) -> Iter<'tx> {
        Iter::new(self.db)
    }

    #[allow(unused)]
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
        let (key, value) = match &mut self.state {
            HashKeysState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let kv = match cursor.set_range(self.prefix.as_bytes()) {
                    Ok(kv) => kv,
                    Err(LmdbError::NOT_FOUND) => return self.finish(),
                    Err(e) => return Err(e.into()),
                };
                self.state = HashKeysState::Init(cursor);
                kv
            }
            HashKeysState::Init(cursor) => match cursor.next() {
                Ok(kv) => kv,
                Err(LmdbError::NOT_FOUND) => return self.finish(),
                Err(e) => return Err(e.into()),
            },
            HashKeysState::Finished => return Ok(None),
        };
        if !key.starts_with(self.prefix.as_bytes()) {
            return self.finish();
        }
        let id: &'tx ID = ID::parse(value)?;
        let str: &'tx [u8] = &key[size_of::<HashKeyPrefix>()..];
        let str = unsafe { std::str::from_utf8_unchecked(str) };
        Ok(Some((str, id)))
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
            let (_, value) = cursor.key_value()?;
            let id: &'tx ID = ID::parse(value)?;
            Ok(id)
        } else {
            Err(crate::Error::NotFound)
        }
    }

    pub fn next(&mut self) -> crate::Result<Option<MapKey<'tx>>> {
        let (k, _) = match &mut self.state {
            MapEntriesState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let key = MapEntriesKey::new(self.node_id);
                let kv = match cursor.set_range(key.as_bytes()) {
                    Ok(kv) => kv,
                    Err(LmdbError::NOT_FOUND) => {
                        self.state = MapEntriesState::Finished;
                        return Ok(None);
                    }
                    Err(e) => return Err(e.into()),
                };
                self.state = MapEntriesState::Init(cursor);
                kv
            }
            MapEntriesState::Init(cursor) => match cursor.next() {
                Ok(kv) => kv,
                Err(LmdbError::NOT_FOUND) => {
                    self.state = MapEntriesState::Finished;
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            },
            MapEntriesState::Finished => return Ok(None),
        };
        if let Some(key) = MapKey::parse(k)
            && key.node_id() == &self.node_id
        {
            Ok(Some(key))
        } else {
            self.state = MapEntriesState::Finished;
            Ok(None)
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

#[allow(unused)]
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

    #[allow(unused)]
    pub fn next(&mut self) -> crate::Result<Option<(MapKey<'tx>, &'tx ID)>> {
        let (k, v) = match &mut self.state {
            IterState::Uninit(db) => {
                let mut cursor = db.cursor()?;
                let kv = match cursor.set_range(&[MapEntriesStore::PREFIX]) {
                    Ok(kv) => kv,
                    Err(LmdbError::NOT_FOUND) => {
                        self.state = IterState::Finished;
                        return Ok(None);
                    }
                    Err(e) => return Err(e.into()),
                };
                self.state = IterState::Init(cursor);
                kv
            }
            IterState::Init(cursor) => match cursor.next() {
                Ok(kv) => kv,
                Err(LmdbError::NOT_FOUND) => {
                    self.state = IterState::Finished;
                    return Ok(None);
                }
                Err(e) => return Err(e.into()),
            },
            IterState::Finished => return Ok(None),
        };
        if let Some(key) = MapKey::parse(k) {
            let id: &'tx ID = ID::parse(v)?;
            Ok(Some((key, id)))
        } else {
            self.state = IterState::Finished;
            Ok(None)
        }
    }
}

#[allow(unused)]
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
