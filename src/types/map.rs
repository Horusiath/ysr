use crate::block::{BlockMut, ID, InsertBlockData};
use crate::content::{Content, ContentType};
use crate::integrate::IntegrationContext;
use crate::lmdb::Database;
use crate::node::{Node, NodeID, NodeType};
use crate::prelim::Prelim;
use crate::store::map_entries::{MapEntries, MapKey};
use crate::store::{Db, MapEntriesStore};
use crate::types::Capability;
use crate::{Clock, Error, In, Mounted, Optional, Transaction, Unmounted, lib0};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

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
        let map_entries = db.map_entries();
        let entry_id = *map_entries
            .get(self.block.id(), key.as_ref())?
            .ok_or(Error::NotFound)?;
        let blocks = db.blocks();
        let block = blocks.get(entry_id)?;
        if block.is_deleted() {
            Err(Error::NotFound)
        } else {
            let content = match block.try_inline_content() {
                Some(content) => content, // content small enough to fit inline block header
                None => {
                    // we need to reach for the content store
                    let content_store = db.contents();
                    let content = content_store.get(*block.id())?;
                    Content::new(block.content_type(), Cow::Borrowed(content))
                }
            };
            V::try_from(content)
        }
    }

    pub fn len(&self) -> crate::Result<usize> {
        let db = self.tx.db();
        let map_entries = db.map_entries();
        let blocks = db.blocks();
        let mut blocks_cursor = blocks.cursor()?;
        let mut iter = map_entries.entries(self.node_id());
        let mut len = 0;
        while let Some(_) = iter.next()? {
            // we only need a direct seek, since `seek_containing` would catch at best deleted blocks
            // that we don't care about here
            if let Some(block) = blocks_cursor.seek(*iter.block_id()?).optional()? {
                if !block.is_deleted() {
                    len += 1;
                }
            }
        }
        Ok(len)
    }

    pub fn contains_key<K>(&self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        let db = self.tx.db();
        let map_entries = db.map_entries();
        let entry_id = match map_entries.get(self.block.id(), key.as_ref())? {
            None => return Ok(false),
            Some(id) => *id,
        };
        let blocks = db.blocks();
        match blocks.get(entry_id).optional()? {
            None => Ok(false),
            Some(block) => Ok(!block.is_deleted()),
        }
    }

    pub fn iter(&self) -> Iter<'tx> {
        let db = self.tx.db();
        Iter::new(db, *self.node_id())
    }

    pub fn to_value(&self) -> crate::Result<crate::lib0::Value> {
        let mut map = HashMap::default();
        let mut iter = self.iter();
        while let Some(e) = iter.next()? {
            let key = e.key().to_owned();
            let value: lib0::Value = e.value()?;
            map.insert(key, value);
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
        let map_entries = db.map_entries();
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
        let blocks = db.blocks();
        let mut ctx = IntegrationContext::create(&mut insert, Clock::new(0), &blocks)?;
        insert.integrate(&mut db, state, &mut ctx)?;
        value.integrate(&mut insert, &mut self.tx)?;
        self.block = ctx.parent.unwrap();
        Ok(())
    }

    pub fn remove<K>(&mut self, key: K) -> crate::Result<bool>
    where
        K: AsRef<str>,
    {
        let parent_id = *self.node_id();
        let (db, state) = self.tx.split_mut();
        let map_entries = db.map_entries();
        let block_id = match map_entries.get(&parent_id, key.as_ref())? {
            None => return Ok(false),
            Some(id) => *id,
        };
        let blocks = db.blocks();
        let mut block_cursor = blocks.cursor()?;
        let block = match block_cursor.seek(block_id).optional()? {
            None => return Ok(false),
            Some(block) => block,
        };
        if !block.is_deleted() {
            let mut block: BlockMut = block.into();
            state.delete(&mut block, false, &mut block_cursor, &map_entries)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn clear(&mut self) -> crate::Result<()> {
        let parent_id = *self.node_id();
        let (db, state) = self.tx.split_mut();
        let map_entries = db.map_entries();
        let blocks = db.blocks();
        let mut cursor = blocks.cursor()?;
        let mut iter = map_entries.entries(&parent_id);
        while let Some(_) = iter.next()? {
            let id = iter.block_id()?;
            if let Some(block) = cursor.seek(*id).optional()? {
                let mut block: BlockMut = block.into();
                state.delete(&mut block, false, &mut cursor, &map_entries)?;
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
    Uninit(Database<'a>, NodeID),
    Init(InitIterState<'a>),
    Finished,
}

impl<'a> IterState<'a> {
    #[inline]
    fn new(db: Database<'a>, node_id: NodeID) -> Self {
        IterState::Uninit(db, node_id)
    }
}

struct InitIterState<'a> {
    db: Pin<Box<Database<'a>>>,
    // all fields bellow are referencing the database above which is provided by its pinned address
    // they won't outlive it
    node_entries: MapEntries<'static>,
    map_entries: MapEntriesStore<'static>,
}

impl<'a> InitIterState<'a> {
    fn new(db: Database<'a>, node_id: NodeID) -> crate::Result<Self> {
        let db = Box::pin(db);

        let map_entries: MapEntriesStore<'static> =
            unsafe { std::mem::transmute(db.map_entries()) };
        let node_entries: MapEntries<'static> = map_entries.entries(&node_id);
        Ok(InitIterState {
            db,
            node_entries,
            map_entries,
        })
    }
}

pub struct Entry<'a, 'db> {
    key: MapKey<'a>,
    block_id: ID,
    db: &'a Database<'db>,
}

impl<'a, 'db> Entry<'a, 'db> {
    pub fn new(key: MapKey<'a>, block_id: ID, db: &'a Database<'db>) -> Self {
        Entry { key, block_id, db }
    }

    pub fn key(&self) -> &'a str {
        self.key.key()
    }

    pub fn value<T>(&self) -> crate::Result<T>
    where
        T: for<'b> TryFrom<Content<'b>, Error = crate::Error>,
    {
        let blocks = self.db.blocks();
        let block = blocks.get(self.block_id)?;
        if !block.is_deleted() {
            let content = match block.try_inline_content() {
                Some(content) => content,
                None => {
                    let contents = self.db.contents();
                    let data = contents.get(self.block_id)?;
                    Content::new(block.content_type(), Cow::Borrowed(data))
                }
            };
            T::try_from(content)
        } else {
            T::try_from(Content::DELETED)
        }
    }
}

pub struct Iter<'a> {
    state: IterState<'a>,
}

impl<'db> Iter<'db> {
    pub fn new(db: Database<'db>, node_id: NodeID) -> Self {
        Iter {
            state: IterState::new(db, node_id),
        }
    }

    fn ensure_init(&mut self) -> crate::Result<()> {
        self.state = match std::mem::replace(&mut self.state, IterState::Finished) {
            IterState::Uninit(db, node_id) => IterState::Init(InitIterState::new(db, node_id)?),
            other => other,
        };
        Ok(())
    }

    pub fn next<'b>(&'b mut self) -> crate::Result<Option<Entry<'b, 'db>>> {
        self.ensure_init()?;
        let inner = match &mut self.state {
            IterState::Init(inner) => inner,
            _ => return Ok(None),
        };
        let result = inner.node_entries.next()?;
        match result {
            None => Ok(None),
            Some(map_key) => {
                let block_id = *inner.node_entries.block_id()?;
                let e = Entry::new(map_key, block_id, &inner.db);
                Ok(Some(e))
            }
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
            let mut mounted = unmounted.mount_mut(tx)?;
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
