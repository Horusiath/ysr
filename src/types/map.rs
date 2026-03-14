use crate::block::{BlockMut, ID, InsertBlockData};
use crate::content::{Content, ContentType};
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeID, NodeType};
use crate::prelim::Prelim;
use crate::store::Db;
use crate::store::map_entries::MapEntries;
use crate::types::Capability;
use crate::{Clock, Error, In, Mounted, Optional, Transaction, Unmounted, lib0};
use lmdb_rs_m::{Database, MdbError};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use zerocopy::IntoBytes;

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
        V: for<'a> TryFrom<Content<'a>, Error = Error>,
    {
        let db = self.tx.db();
        let mut map_entries = db.map_entries()?;
        let entry_id = *map_entries
            .get(self.block.id(), key.as_ref())?
            .ok_or(Error::NotFound)?;
        let mut blocks = db.blocks()?;
        let block = blocks.seek(entry_id)?.ok_or(Error::NotFound)?;
        if block.is_deleted() {
            Err(Error::NotFound)
        } else {
            let content = match block.try_inline_content() {
                Some(content) => content, // content small enough to fit inline block header
                None => {
                    // we need to reach for the content store
                    let mut content_store = db.contents()?;
                    let content = content_store.seek(*block.id())?.ok_or(Error::NotFound)?;
                    Content::new(block.content_type(), Cow::Borrowed(content))
                }
            };
            V::try_from(content)
        }
    }

    pub fn len(&self) -> crate::Result<usize> {
        let db = self.tx.db();
        let mut map_entries = db.map_entries()?;
        let mut iter = map_entries.entries(self.node_id());
        let mut len = 0;
        while let Some(_) = iter.next()? {
            len += 1;
        }
        Ok(len)
    }

    pub fn contains_key<K>(&self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        let db = self.tx.db();
        let mut map_entries = db.map_entries()?;
        let entry_id = match map_entries.get(self.block.id(), key.as_ref())? {
            None => return Ok(false),
            Some(id) => *id,
        };
        let mut blocks = db.blocks()?;
        match blocks.seek(entry_id)? {
            None => Ok(false),
            Some(block) => Ok(!block.is_deleted()),
        }
    }

    pub fn iter<T>(&self) -> Iter<'tx, T>
    where
        T: TryFrom<Content<'tx>, Error = Error>,
    {
        let db = self.tx.db();
        Iter::new(db, *self.node_id())
    }

    pub fn to_value(&self) -> crate::Result<crate::lib0::Value> {
        let mut map = HashMap::default();
        let iter = self.iter::<crate::lib0::Value>();
        for res in iter {
            let (key, value) = res?;
            map.insert(key.to_string(), value);
        }

        Ok(crate::lib0::Value::Object(map))
    }
}

impl<'tx, 'db> MapRef<&'tx mut Transaction<'db>> {
    pub fn insert<K, V>(&mut self, key: K, value: V) -> crate::Result<()>
    where
        K: AsRef<str>,
        V: Prelim,
    {
        let key = key.as_ref();
        let node_id = *self.node_id();
        let (mut db, state) = self.tx.split_mut();
        let mut map_entries = db.map_entries()?;
        let left_id = map_entries.get(&node_id, key)?;
        let id = state.next_id();
        let mut insert = InsertBlockData::new(
            id,
            Clock::new(1),
            left_id,
            None,
            left_id,
            None,
            Node::Nested(node_id),
            Some(key.as_ref()),
        );
        value.prepare(&mut insert)?;
        let mut blocks = db.blocks()?;
        let mut context = IntegrationContext::create(&mut insert, Clock::new(0), &mut blocks)?;
        insert.integrate(&mut db, state, &mut context)?;
        value.integrate(&mut insert, &mut self.tx)?;
        Ok(())
    }

    pub fn remove<K>(&mut self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        let (mut db, state) = self.tx.split_mut();
        let mut map_entries = db.map_entries()?;
        let block_id = match map_entries.get(self.node_id(), key.as_ref())? {
            None => return Ok(false),
            Some(id) => *id,
        };
        let mut blocks = db.blocks()?;
        let block = match blocks.seek(block_id)? {
            None => return Ok(false),
            Some(block) => block,
        };
        if !block.is_deleted() {
            let mut block: BlockMut = block.into();
            state.delete(&mut blocks, &mut block, false)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn clear(&mut self) -> crate::Result<()> {
        let (mut db, state) = self.tx.split_mut();
        let mut map_entries = db.map_entries()?;
        let mut blocks = db.blocks()?;
        let mut iter = map_entries.entries(self.node_id());
        while let Some(key) = iter.next()? {
            let id = key.block_id();
            if let Some(block) = blocks.seek(*id)? {
                let mut block: BlockMut = block.into();
                state.delete(&mut blocks, &mut block, false)?;
            }
        }
        Ok(())
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

enum IterState<'a> {
    Uninit(Option<Database<'a>>),
    Init(InitializedIterator<'a>),
}

struct InitializedIterator<'a> {
    db: Database<'a>,
    map_entries: crate::store::MapEntriesStore<'a>,
}

impl<'a> InitializedIterator<'a> {
    pub fn init(db: Database<'a>) -> crate::Result<Self> {
        let mut init = InitializedIterator {
            db,
            map_entries: unsafe { MaybeUninit::uninit().assume_init() },
        };
        init.map_entries = init.db.map_entries()?;
        Ok(init)
    }
}

impl<'a> IterState<'a> {
    fn new(db: Database<'a>) -> Self {
        IterState::Uninit(Some(db))
    }
}
pub struct Iter<'a, T> {
    state: IterState<'a>,
    node_id: NodeID,
    _phantom: PhantomData<T>,
}

impl<'a, T> Iter<'a, T>
where
    T: TryFrom<Content<'a>>,
{
    pub fn new(db: Database<'a>, node_id: NodeID) -> Self {
        Iter {
            state: IterState::new(db),
            node_id,
            _phantom: PhantomData,
        }
    }

    fn next_entry(&mut self) -> crate::Result<Option<&mut lmdb_rs_m::Cursor<'a>>> {
        match &mut self.state {
            IterState::Uninit(db) => {
                let db = db.take().unwrap();
                let mut cursor = OwnedCursor::new(db)?;
                if cursor
                    .to_gte_key(&self.prefix.as_ref())
                    .optional()?
                    .is_none()
                {
                    return Ok(None);
                };
                self.state = IterState::Init(cursor);
            }
            IterState::Init(cursor) => {
                if cursor.to_next_key().optional()?.is_none() {
                    return Ok(None);
                }
            }
        }
        match &mut self.state {
            IterState::Init(c) => {
                let key: &[u8] = c.get_key()?;
                if !key.starts_with(self.prefix.as_ref()) {
                    Ok(None)
                } else {
                    Ok(Some(c.deref_mut()))
                }
            }
            _ => unreachable!(),
        }
    }

    fn prev_entry(&mut self) -> crate::Result<Option<&mut lmdb_rs_m::Cursor<'a>>> {
        if let IterState::Uninit(db) = &mut self.state {
            let db = db.take().unwrap();
            let mut cursor = OwnedCursor::new(db)?;
            if cursor
                .to_gte_key(&self.prefix.as_ref())
                .optional()?
                .is_none()
            {
                return Ok(None);
            };
            self.state = IterState::Init(cursor);
        }
        match &mut self.state {
            IterState::Init(cursor) => {
                if cursor.to_prev_key().optional()?.is_none() {
                    return Ok(None);
                }
                let key: &[u8] = cursor.get_key()?;
                if !key.starts_with(self.prefix.as_ref()) {
                    return Ok(None);
                }
                Ok(Some(cursor.deref_mut()))
            }
            _ => unreachable!(),
        }
    }

    fn cursor(&mut self) -> &mut lmdb_rs_m::Cursor<'a> {
        match &mut self.state {
            IterState::Init(cursor) => cursor.deref_mut(),
            _ => unreachable!(),
        }
    }

    fn move_next(&mut self) -> crate::Result<Option<(&'a str, T)>> {
        let cursor = match self.next_entry()? {
            Some(cursor) => cursor,
            None => return Ok(None),
        };

        let rollback_key: &[u8] = cursor.get_key()?;
        let key = unsafe { std::str::from_utf8_unchecked(&rollback_key[1 + 8 + 4..]) };
        let id = *ID::parse(cursor.get_value()?)?;
        cursor.to_key(&BlockKey::new(id))?;
        let block = cursor.get_block()?;

        if block.is_deleted() {
            cursor.to_key(&rollback_key)?;
            self.move_next()
        } else {
            let content = match block.content_type() {
                ContentType::Node => BlockContentRef::NODE,
                ContentType::Deleted => BlockContentRef::DELETED,
                content_type => {
                    cursor.to_key(&BlockContentKey::new(*block.id()))?;
                    BlockContentRef::new(cursor.get_value()?)?
                }
            };
            let value = T::try_from_content(block, content)?;
            cursor.to_key(&rollback_key)?;
            Ok(Some((key, value)))
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: TryFrom<Content<'a>>,
{
    type Item = crate::Result<(&'a str, T)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.move_next() {
            Ok(None) => None,
            Ok(Some((key, value))) => Some(Ok((key, value))),
            Err(error) => Some(Err(error)),
        }
    }
}

struct RawIter<'a> {
    prefix: [u8; 9],
    cursor: Option<OwnedCursor<'a>>,
}

impl<'a> RawIter<'a> {
    fn new(db: Database<'a>, prefix: [u8; 9]) -> crate::Result<Self> {
        let mut cursor = OwnedCursor::new(db)?;
        match cursor.to_gte_key(&prefix.as_ref()) {
            Ok(()) => Ok(RawIter {
                cursor: Some(cursor),
                prefix,
            }),
            Err(MdbError::NotFound) => Ok(RawIter {
                cursor: None,
                prefix,
            }),
            Err(e) => return Err(Error::Lmdb(e)),
        }
    }

    pub fn next(&mut self) -> crate::Result<bool> {
        if let Some(cursor) = &mut self.cursor {
            match cursor.to_next_key() {
                Ok(()) => {
                    let key: &[u8] = cursor.get_key()?;
                    Ok(key.starts_with(self.prefix.as_ref()))
                }
                Err(MdbError::NotFound) => Ok(false),
                Err(e) => Err(Error::Lmdb(e)),
            }
        } else {
            Ok(false)
        }
    }

    pub fn next_back(&mut self) -> crate::Result<bool> {
        if let Some(cursor) = &mut self.cursor {
            match cursor.to_prev_key() {
                Ok(()) => {
                    let key: &[u8] = cursor.get_key()?;
                    Ok(key.starts_with(self.prefix.as_ref()))
                }
                Err(MdbError::NotFound) => Ok(false),
                Err(e) => Err(Error::Lmdb(e)),
            }
        } else {
            Ok(false)
        }
    }

    pub fn key(&mut self) -> Option<&'a [u8]> {
        let cursor = self.cursor.as_deref_mut()?;
        cursor.get_key().ok()
    }

    pub fn block_id(&mut self) -> crate::Result<Option<&'a ID>> {
        if let Some(cursor) = &mut self.cursor {
            let value: &[u8] = cursor.get_value()?;
            let id = ID::parse(value)?;
            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MapPrelim(BTreeMap<String, In>);

impl Prelim for MapPrelim {
    type Return = Unmounted<Map>;

    #[inline]
    fn clock_len(&self) -> Clock {
        Clock::new(1) // the map object itself is 1 element
    }

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()> {
        let block = insert.as_block_mut();
        block.set_content_type(ContentType::Node);
        block.set_node_type(NodeType::Map);
        Ok(())
    }

    fn integrate(
        self,
        insert: &mut InsertBlockData,
        tx: &mut Transaction,
    ) -> crate::Result<Self::Return> {
        let unmounted: Unmounted<Map> = Unmounted::nested(*insert.block.id());
        if !self.0.is_empty() {
            let mut mounted = unmounted.mount(tx)?;
            for (key, value) in self.0 {
                mounted.insert(key, value)?;
            }
        }
        Ok(unmounted)
    }
}

impl Deref for MapPrelim {
    type Target = BTreeMap<String, In>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MapPrelim {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BTreeMap<String, In>> for MapPrelim {
    fn from(value: BTreeMap<String, In>) -> Self {
        MapPrelim(value)
    }
}

impl FromIterator<(String, In)> for MapPrelim {
    fn from_iter<T: IntoIterator<Item = (String, In)>>(iter: T) -> Self {
        MapPrelim(iter.into_iter().collect())
    }
}

#[cfg(test)]
mod test {
    use crate::lib0::Value;
    use crate::read::DecoderV1;

    use crate::test_util::{multi_doc, sync};
    use crate::{
        In, List, ListPrelim, ListRef, Map, MapPrelim, Optional, StateVector, Unmounted, lib0,
    };
    use serde::Deserialize;
    use std::collections::HashMap;

    #[test]
    fn basic() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();

        let mut m1 = map.mount_mut(&mut t1).unwrap();
        m1.insert("number", 1.1).unwrap();

        let update = t1.diff_update(&StateVector::default()).unwrap();
        t1.commit(None).unwrap();

        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();
        let m2 = map.mount_mut(&mut t2).unwrap();
        assert_eq!(m2.to_value().unwrap(), lib0!({"number": 1.1}));
    }

    #[test]
    fn map_basic() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();

        let mut m1 = map.mount_mut(&mut t1).unwrap();

        m1.insert("number", 1.1).unwrap();
        m1.insert("string", "hello Y").unwrap();
        m1.insert("object", {
            let mut v = HashMap::new();
            v.insert("key2".to_owned(), "value");

            let mut map = HashMap::new();
            map.insert("key".to_owned(), v);
            map // { key: { key2: 'value' } }
        })
        .unwrap();
        m1.insert("boolean1", true).unwrap();
        m1.insert("boolean0", false).unwrap();

        let expected = lib0!({
            "number": 1.1,
            "string": "hello Y",
            "object": {
                "key": {
                    "key2": "value"
                }
            },
            "boolean1": true,
            "boolean0": false
        });

        let v1 = m1.to_value().unwrap();
        assert_eq!(v1, expected);

        let update = t1.diff_update(&StateVector::default()).unwrap();

        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();
        let m2 = map.mount_mut(&mut t2).unwrap();

        let v2 = m2.to_value().unwrap();
        assert_eq!(v2, expected);
    }

    #[test]
    fn map_get_set() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut m1 = map.mount_mut(&mut t1).unwrap();

        m1.insert("stuff", "stuffy").unwrap();
        m1.insert("null", None as Option<String>).unwrap();

        let update = t1.diff_update(&StateVector::default()).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();

        let m2 = map.mount_mut(&mut t2).unwrap();

        assert_eq!(
            m2.get("stuff").optional().unwrap(),
            Some(Value::String("stuffy".into()))
        );
        assert_eq!(m2.get("null").optional().unwrap(), Some(Value::Null));
    }

    #[test]
    fn map_get_set_sync_with_conflicts() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut m1 = map.mount_mut(&mut t1).unwrap();

        let mut t2 = d2.transact_mut("test").unwrap();
        let mut m2 = map.mount_mut(&mut t2).unwrap();

        m1.insert("stuff", "c0").unwrap();
        m2.insert("stuff", "c1").unwrap();

        sync([&mut t1, &mut t2]);

        let m1 = map.mount(&t1).unwrap();
        let m2 = map.mount(&t2).unwrap();

        assert_eq!(
            m1.get::<_, Value>("stuff").unwrap(),
            Value::String("c1".into())
        );
        assert_eq!(
            m2.get::<_, Value>("stuff").unwrap(),
            Value::String("c1".into())
        );

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn map_len_remove() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut m1 = map.mount_mut(&mut t1).unwrap();

        let key1 = "stuff".to_owned();
        let key2 = "other-stuff".to_owned();

        m1.insert(key1.clone(), "c0").unwrap();
        m1.insert(key2.clone(), "c1").unwrap();
        assert_eq!(m1.len().unwrap(), 2);

        // remove 'stuff'
        m1.remove(&key1).unwrap();
        assert_eq!(m1.len().unwrap(), 1);

        // remove 'stuff' again - nothing should happen
        m1.remove(&key1).unwrap();
        assert_eq!(m1.len().unwrap(), 1);

        // remove 'other-stuff'
        m1.remove(&key2).unwrap();
        assert_eq!(m1.len().unwrap(), 0);

        t1.commit(None).unwrap();
    }

    #[test]
    fn map_clear() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut m1 = map.mount_mut(&mut t1).unwrap();

        m1.insert("key1", "c0").unwrap();
        m1.insert("key2", "c1").unwrap();
        m1.clear().unwrap();

        assert_eq!(m1.len().unwrap(), 0);
        assert_eq!(m1.get::<_, Value>("key1").optional().unwrap(), None);
        assert_eq!(m1.get::<_, Value>("key2").optional().unwrap(), None);

        let u1 = t1.diff_update(&StateVector::default()).unwrap();

        t1.commit(None).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut m2 = map.mount_mut(&mut t2).unwrap();

        assert_eq!(m2.len().unwrap(), 0);
        assert_eq!(m2.get::<_, Value>("key1").optional().unwrap(), None);
        assert_eq!(m2.get::<_, Value>("key2").optional().unwrap(), None);
    }

    #[test]
    fn map_clear_sync() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);
        let (d4, _) = multi_doc(4);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();
        let mut t4 = d4.transact_mut("test").unwrap();

        {
            let mut m1 = map.mount_mut(&mut t1).unwrap();
            let mut m2 = map.mount_mut(&mut t2).unwrap();
            let mut m3 = map.mount_mut(&mut t3).unwrap();

            m1.insert("key1", "c0").unwrap();
            m2.insert("key1", "c1").unwrap();
            m2.insert("key1", "c2").unwrap();
            m3.insert("key1", "c3").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3, &mut t4]);

        {
            let mut m1 = map.mount_mut(&mut t1).unwrap();
            let mut m2 = map.mount_mut(&mut t2).unwrap();
            let mut m3 = map.mount_mut(&mut t3).unwrap();

            m1.insert("key2", "c0").unwrap();
            m2.insert("key2", "c1").unwrap();
            m2.insert("key2", "c2").unwrap();
            m3.insert("key2", "c3").unwrap();
            m3.clear().unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3, &mut t4]);

        for mut tx in [t1, t2, t3, t4] {
            let map = map.mount_mut(&mut tx).unwrap();

            assert_eq!(map.get::<_, Value>("key1").optional().unwrap(), None);
            assert_eq!(map.get::<_, Value>("key2").optional().unwrap(), None);
            assert_eq!(map.len().unwrap(), 0);

            tx.commit(None).unwrap();
        }
    }

    #[test]
    fn map_get_set_with_3_way_conflicts() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();

        {
            let mut m1 = map.mount_mut(&mut t1).unwrap();
            let mut m2 = map.mount_mut(&mut t2).unwrap();
            let mut m3 = map.mount_mut(&mut t3).unwrap();

            m1.insert("stuff", "c0").unwrap();
            m2.insert("stuff", "c1").unwrap();
            m2.insert("stuff", "c2").unwrap();
            m3.insert("stuff", "c3").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3]);

        for mut tx in [t1, t2, t3] {
            let map = map.mount_mut(&mut tx).unwrap();

            assert_eq!(map.get::<_, Value>("stuff").unwrap(), Value::from("c3"));
            tx.commit(None).unwrap();
        }
    }

    #[test]
    fn map_get_set_remove_with_3_way_conflicts() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);
        let (d4, _) = multi_doc(4);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();
        let mut t4 = d4.transact_mut("test").unwrap();

        {
            let mut m1 = map.mount_mut(&mut t1).unwrap();
            let mut m2 = map.mount_mut(&mut t2).unwrap();
            let mut m3 = map.mount_mut(&mut t3).unwrap();

            m1.insert("key1", "c0").unwrap();
            m2.insert("key1", "c1").unwrap();
            m2.insert("key1", "c2").unwrap();
            m3.insert("key1", "c3").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3, &mut t4]);

        {
            let mut m1 = map.mount_mut(&mut t1).unwrap();
            let mut m2 = map.mount_mut(&mut t2).unwrap();
            let mut m3 = map.mount_mut(&mut t3).unwrap();
            let mut m4 = map.mount_mut(&mut t4).unwrap();

            m1.insert("key1", "deleteme").unwrap();
            m2.insert("key1", "c1").unwrap();
            m3.insert("key1", "c2").unwrap();
            m4.insert("key1", "c3").unwrap();
            m4.remove("key1").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3, &mut t4]);

        for tx in [t1, t2, t3] {
            let map = map.mount(&tx).unwrap();
            assert_eq!(map.get::<_, Value>("key1").optional().unwrap(), None);
            tx.commit(None).unwrap();
        }
    }

    #[test]
    fn get_value() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct Order {
            shipment_address: String,
            items: HashMap<String, OrderItem>,
            #[serde(default)]
            comment: Option<String>,
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct OrderItem {
            name: String,
            price: f64,
            quantity: u32,
        }

        let map: Unmounted<Map> = Unmounted::root("map");

        let (doc, _) = multi_doc(1);
        let mut tx = doc.transact_mut("test").unwrap();
        let mut map = map.mount_mut(&mut tx).unwrap();

        map.insert(
            "orders",
            ListPrelim::from(vec![In::from(MapPrelim::from_iter([
                ("shipment_address".to_string(), In::from("123 Main St")),
                (
                    "items".into(),
                    In::from(MapPrelim::from_iter([
                        (
                            "item1".to_string(),
                            In::from(MapPrelim::from_iter([
                                ("name".to_string(), In::from("item1")),
                                ("price".into(), In::from(1.99)),
                                ("quantity".into(), In::from(2)),
                            ])),
                        ),
                        (
                            "item2".to_string(),
                            In::from(MapPrelim::from_iter([
                                ("name".to_string(), In::from("item2")),
                                ("price".into(), In::from(2.99)),
                                ("quantity".into(), In::from(1)),
                            ])),
                        ),
                    ])),
                ),
            ]))]),
        )
        .unwrap();

        let expected = Order {
            comment: None,
            shipment_address: "123 Main St".to_string(),
            items: HashMap::from([
                (
                    "item1".to_string(),
                    OrderItem {
                        name: "item1".to_string(),
                        price: 1.99,
                        quantity: 2,
                    },
                ),
                (
                    "item2".to_string(),
                    OrderItem {
                        name: "item2".to_string(),
                        price: 2.99,
                        quantity: 1,
                    },
                ),
            ]),
        };

        let orders: Unmounted<List> = map.get("orders").unwrap();
        let orders: ListRef<_> = orders.mount_mut(&mut tx).unwrap();
        let actual: Vec<Order> = lib0::from_value(&orders.to_value().unwrap()).unwrap();
        assert_eq!(actual, vec![expected]);

        tx.commit(None).unwrap();
    }
}
