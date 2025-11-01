use crate::block::{Block, BlockMut, InsertBlockData, ID};
use crate::content::{BlockContent, ContentType, TryFromContent};
use crate::integrate::IntegrationContext;
use crate::node::{Node, NodeID, NodeType};
use crate::prelim::Prelim;
use crate::store::lmdb::store::{
    map_key, BlockContentKey, BlockKey, CursorExt, OwnedCursor, KEY_PREFIX_MAP,
};
use crate::store::lmdb::BlockStore;
use crate::types::Capability;
use crate::{lib0, Clock, Error, In, Mounted, Optional, Transaction, Unmounted};
use lmdb_rs_m::{Database, MdbError};
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use zerocopy::{FromBytes, IntoBytes};

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
        V: TryFromContent,
    {
        let db = self.tx.db();
        let id = *db.entry(*self.block.id(), key.as_ref())?;
        let block = db.fetch_block(id, false)?;
        if block.is_deleted() {
            Err(crate::Error::NotFound)
        } else {
            let content_type = block.content_type();
            let content = db.block_content(id, content_type)?;
            V::try_from_content(block, content)
        }
    }

    pub fn len(&self) -> crate::Result<usize> {
        let prefix = self.map_prefix();
        let db = self.tx.db();
        let mut iter = Iter::<lib0::Value>::new(db, prefix);
        let mut len = 0;
        while iter.next_entry()?.is_some() {
            len += 1;
        }
        Ok(len)
    }

    fn map_prefix(&self) -> [u8; 9] {
        let mut prefix = [0u8; 1 + size_of::<NodeID>()];
        prefix[0] = KEY_PREFIX_MAP;
        prefix[1..].copy_from_slice(&self.node_id().as_bytes());
        prefix
    }

    pub fn contains_key<K>(&self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        let db = self.tx.db();
        let mut cursor = db.new_cursor()?;
        let key = map_key(*self.block.id(), key.as_ref());
        match cursor.to_key(&key.as_ref()) {
            Ok(()) => Ok(true),
            Err(MdbError::NotFound) => Ok(false),
            Err(e) => Err(Error::Lmdb(e)),
        }
    }

    pub fn iter<T>(&self) -> Iter<'tx, T>
    where
        T: TryFromContent,
    {
        let prefix = self.map_prefix();
        let db = self.tx.db();
        Iter::new(db, prefix)
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
        let node_id = *self.node_id();
        let db = self.tx.db();
        let left_id = if let Some(id) = db.entry(*self.block.id(), key.as_ref()).optional()? {
            let block = db.fetch_block(*id, false)?;
            Some(block.last_id())
        } else {
            None
        };
        let (mut db, state) = self.tx.split_mut();
        let id = state.next_id();
        let mut insert = InsertBlockData::new(
            id,
            Clock::new(1),
            left_id.as_ref(),
            None,
            left_id.as_ref(),
            None,
            Node::Nested(node_id),
            Some(key.as_ref()),
        );
        value.prepare(&mut insert)?;
        let mut context = IntegrationContext::create(&mut insert, Clock::new(0), &mut db)?;
        insert.integrate(&mut db, state, &mut context)?;
        value.integrate(&mut insert, &mut self.tx)?;
        Ok(())
    }

    pub fn remove<K>(&mut self, key: K) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        let (mut db, state) = self.tx.split_mut();
        let id = *db.entry(*self.block.id(), key.as_ref())?;
        let block = db.fetch_block(id, false)?;
        if !block.is_deleted() {
            let mut block: BlockMut = block.into();
            state.delete(&mut db, &mut block, false)?;
        }
        Ok(())
    }

    pub fn clear(&mut self) -> crate::Result<()> {
        let node_id = *self.node_id();
        let (mut db, state) = self.tx.split_mut();
        let mut cursor = db.new_cursor()?;
        let mut to_delete = Vec::new();
        for res in cursor.entries(node_id) {
            let (key, id) = res?;
            to_delete.push(*id);
        }
        for id in to_delete {
            cursor.to_key(&BlockKey::new(id))?;
            let mut block: BlockMut = cursor.get_block()?.into();
            cursor.delete_current(state, &mut block, false)?;
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
    Init(OwnedCursor<'a>),
}

impl<'a> IterState<'a> {
    fn new(db: Database<'a>) -> Self {
        IterState::Uninit(Some(db))
    }
}
pub struct Iter<'a, T> {
    state: IterState<'a>,
    prefix: [u8; 9],
    _phantom: PhantomData<T>,
}

impl<'a, T> Iter<'a, T>
where
    T: TryFromContent,
{
    pub fn new(db: Database<'a>, prefix: [u8; 9]) -> Self {
        Iter {
            state: IterState::new(db),
            prefix,
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
                ContentType::Node => BlockContent::Node,
                ContentType::Deleted => BlockContent::Deleted,
                content_type => {
                    cursor.to_key(&BlockContentKey::new(*block.id()))?;
                    BlockContent::new(content_type, cursor.get_value()?)?
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
    T: TryFromContent,
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

    fn prepare(&self, insert: &mut InsertBlockData) -> crate::Result<()> {
        insert.init_content(BlockContent::Node);
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
    use crate::store::lmdb::BlockStore;
    use crate::test_util::{multi_doc, sync};
    use crate::{
        lib0, In, List, ListPrelim, ListRef, Map, MapPrelim, Optional, StateVector, Unmounted,
    };
    use serde::Deserialize;
    use std::collections::HashMap;

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
