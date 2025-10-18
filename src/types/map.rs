use crate::block::InsertBlockData;
use crate::node::NodeType;
use crate::prelim::Prelim;
use crate::types::Capability;
use crate::{In, Mounted, Transaction, Unmounted};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

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

    pub fn iter(&self) -> Iter<'_> {
        todo!()
    }

    pub fn to_value(&self) -> crate::Result<crate::lib0::Value> {
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

    pub fn remove<K>(&mut self, key: K) -> crate::Result<()>
    where
        K: AsRef<str>,
    {
        todo!()
    }

    pub fn clear(&mut self) -> crate::Result<()> {
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

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MapPrelim(BTreeMap<String, In>);

impl Prelim for MapPrelim {
    type Return = Unmounted<Map>;

    fn prepare(
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
    use crate::{lib0, In, ListPrelim, Map, MapPrelim, Optional, StateVector, Unmounted};
    use serde::Deserialize;
    use std::collections::HashMap;

    #[test]
    fn map_basic() {
        let map: Unmounted<Map> = Unmounted::root("map");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();

        let mut m1 = map.mount_mut(&mut t1).unwrap();

        m1.insert("number", 1).unwrap();
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
            "number": 1.0,
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
        let mut m2 = map.mount_mut(&mut t2).unwrap();

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
        assert_eq!(m1.len(), 2);

        // remove 'stuff'
        m1.remove(&key1).unwrap();
        assert_eq!(m1.len(), 1);

        // remove 'stuff' again - nothing should happen
        m1.remove(&key1).unwrap();
        assert_eq!(m1.len(), 1);

        // remove 'other-stuff'
        m1.remove(&key2).unwrap();
        assert_eq!(m1.len(), 0);

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

        assert_eq!(m1.len(), 0);
        assert_eq!(m1.get::<_, Value>("key1").optional().unwrap(), None);
        assert_eq!(m1.get::<_, Value>("key2").optional().unwrap(), None);

        let u1 = t1.diff_update(&StateVector::default()).unwrap();

        t1.commit(None).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut m2 = map.mount_mut(&mut t2).unwrap();

        assert_eq!(m2.len(), 0);
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
            assert_eq!(map.len(), 0);

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

        let actual: Vec<Order> = map.get("orders").unwrap();
        assert_eq!(actual, vec![expected]);

        tx.commit(None).unwrap();
    }
}
