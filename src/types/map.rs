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

#[cfg(test)]
mod test {
    use crate::lib0::Value;
    use crate::read::DecoderV1;
    use crate::test_util::{multi_doc, sync};
    use crate::{lib0, Map, Optional, StateVector, Unmounted};
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

        let update = t1.create_update(&StateVector::default()).unwrap();

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

        let update = t1.create_update(&StateVector::default()).unwrap();

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

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut m1 = map.mount_mut(&mut t1).unwrap();

        let (d2, _) = multi_doc(2);

        let mut t2 = d2.transact_mut("test").unwrap();
        let mut m2 = map.mount_mut(&mut t2).unwrap();

        m1.insert("stuff", "c0").unwrap();
        m2.insert("stuff", "c1").unwrap();

        sync([&mut t1, &mut t2]);

        let mut m1 = map.mount_mut(&mut t1).unwrap();
        let mut m2 = map.mount_mut(&mut t2).unwrap();

        assert_eq!(m1.get("stuff").unwrap(), Value::String("c1".into()));
        assert_eq!(m2.get("stuff").unwrap(), Value::String("c1".into()));
    }

    #[test]
    fn map_len_remove() {
        let mut d1 = Doc::with_client_id(1);
        let m1 = d1.get_or_insert_map("map");
        let mut t1 = d1.transact_mut();

        let key1 = "stuff".to_owned();
        let key2 = "other-stuff".to_owned();

        m1.insert(&mut t1, key1.clone(), "c0");
        m1.insert(&mut t1, key2.clone(), "c1");
        assert_eq!(m1.len(&t1), 2);

        // remove 'stuff'
        assert_eq!(m1.remove(&mut t1, &key1), Some(Out::from("c0")));
        assert_eq!(m1.len(&t1), 1);

        // remove 'stuff' again - nothing should happen
        assert_eq!(m1.remove(&mut t1, &key1), None);
        assert_eq!(m1.len(&t1), 1);

        // remove 'other-stuff'
        assert_eq!(m1.remove(&mut t1, &key2), Some(Out::from("c1")));
        assert_eq!(m1.len(&t1), 0);
    }

    #[test]
    fn map_clear() {
        let mut d1 = Doc::with_client_id(1);
        let m1 = d1.get_or_insert_map("map");
        let mut t1 = d1.transact_mut();

        m1.insert(&mut t1, "key1".to_owned(), "c0");
        m1.insert(&mut t1, "key2".to_owned(), "c1");
        m1.clear(&mut t1);

        assert_eq!(m1.len(&t1), 0);
        assert_eq!(m1.get::<Out>(&t1, &"key1".to_owned()), None);
        assert_eq!(m1.get::<Out>(&t1, &"key2".to_owned()), None);

        let mut d2 = Doc::with_client_id(2);
        let m2 = d2.get_or_insert_map("map");
        let mut t2 = d2.transact_mut();

        let u1 = t1.encode_state_as_update_v1(&StateVector::default());
        t2.apply_update(Update::decode_v1(u1.as_slice()).unwrap())
            .unwrap();

        assert_eq!(m2.len(&t2), 0);
        assert_eq!(m2.get::<Out>(&t2, &"key1".to_owned()), None);
        assert_eq!(m2.get::<Out>(&t2, &"key2".to_owned()), None);
    }

    #[test]
    fn map_clear_sync() {
        let mut d1 = Doc::with_client_id(1);
        let mut d2 = Doc::with_client_id(2);
        let mut d3 = Doc::with_client_id(3);
        let mut d4 = Doc::with_client_id(4);

        {
            let m1 = d1.get_or_insert_map("map");
            let m2 = d2.get_or_insert_map("map");
            let m3 = d3.get_or_insert_map("map");

            let mut t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();
            let mut t3 = d3.transact_mut();

            m1.insert(&mut t1, "key1".to_owned(), "c0");
            m2.insert(&mut t2, "key1".to_owned(), "c1");
            m2.insert(&mut t2, "key1".to_owned(), "c2");
            m3.insert(&mut t3, "key1".to_owned(), "c3");
        }

        exchange_updates([&mut d1, &mut d2, &mut d3, &mut d4]);

        {
            let m1 = d1.get_or_insert_map("map");
            let m2 = d2.get_or_insert_map("map");
            let m3 = d3.get_or_insert_map("map");

            let mut t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();
            let mut t3 = d3.transact_mut();

            m1.insert(&mut t1, "key2".to_owned(), "c0");
            m2.insert(&mut t2, "key2".to_owned(), "c1");
            m2.insert(&mut t2, "key2".to_owned(), "c2");
            m3.insert(&mut t3, "key2".to_owned(), "c3");
            m3.clear(&mut t3);
        }

        exchange_updates([&mut d1, &mut d2, &mut d3, &mut d4]);

        for doc in [d1, d2, d3, d4] {
            let map: MapRef = doc.get("map").unwrap();

            assert_eq!(
                map.get::<Out>(&doc.transact(), &"key1".to_owned()),
                None,
                "'key1' entry for peer {} should be removed",
                doc.client_id()
            );
            assert_eq!(
                map.get::<Out>(&doc.transact(), &"key2".to_owned()),
                None,
                "'key2' entry for peer {} should be removed",
                doc.client_id()
            );
            assert_eq!(
                map.len(&doc.transact()),
                0,
                "all entries for peer {} should be removed",
                doc.client_id()
            );
        }
    }

    #[test]
    fn map_get_set_with_3_way_conflicts() {
        let mut d1 = Doc::with_client_id(1);
        let mut d2 = Doc::with_client_id(2);
        let mut d3 = Doc::with_client_id(3);

        {
            let m1 = d1.get_or_insert_map("map");
            let m2 = d2.get_or_insert_map("map");
            let m3 = d3.get_or_insert_map("map");

            let mut t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();
            let mut t3 = d3.transact_mut();

            m1.insert(&mut t1, "stuff".to_owned(), "c0");
            m2.insert(&mut t2, "stuff".to_owned(), "c1");
            m2.insert(&mut t2, "stuff".to_owned(), "c2");
            m3.insert(&mut t3, "stuff".to_owned(), "c3");
        }

        exchange_updates([&mut d1, &mut d2, &mut d3]);

        for mut doc in [d1, d2, d3] {
            let map = doc.get_or_insert_map("map");

            assert_eq!(
                map.get(&doc.transact(), &"stuff".to_owned()),
                Some(Out::from("c3")),
                "peer {} - map entry resolved to unexpected value",
                doc.client_id()
            );
        }
    }

    #[test]
    fn map_get_set_remove_with_3_way_conflicts() {
        let mut d1 = Doc::with_client_id(1);
        let mut d2 = Doc::with_client_id(2);
        let mut d3 = Doc::with_client_id(3);
        let mut d4 = Doc::with_client_id(4);

        {
            let m1 = d1.get_or_insert_map("map");
            let m2 = d2.get_or_insert_map("map");
            let m3 = d3.get_or_insert_map("map");

            let mut t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();
            let mut t3 = d3.transact_mut();

            m1.insert(&mut t1, "key1".to_owned(), "c0");
            m2.insert(&mut t2, "key1".to_owned(), "c1");
            m2.insert(&mut t2, "key1".to_owned(), "c2");
            m3.insert(&mut t3, "key1".to_owned(), "c3");
        }

        exchange_updates([&mut d1, &mut d2, &mut d3, &mut d4]);

        {
            let m1 = d1.get_or_insert_map("map");
            let m2 = d2.get_or_insert_map("map");
            let m3 = d3.get_or_insert_map("map");
            let m4 = d4.get_or_insert_map("map");

            let mut t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();
            let mut t3 = d3.transact_mut();
            let mut t4 = d4.transact_mut();

            m1.insert(&mut t1, "key1".to_owned(), "deleteme");
            m2.insert(&mut t2, "key1".to_owned(), "c1");
            m3.insert(&mut t3, "key1".to_owned(), "c2");
            m4.insert(&mut t4, "key1".to_owned(), "c3");
            m4.remove(&mut t4, &"key1".to_owned());
        }

        exchange_updates([&mut d1, &mut d2, &mut d3, &mut d4]);

        for doc in [d1, d2, d3, d4] {
            let map: MapRef = doc.get("map").unwrap();

            assert_eq!(
                map.get::<Out>(&doc.transact(), &"key1".to_owned()),
                None,
                "entry 'key1' on peer {} should be removed",
                doc.client_id()
            );
        }
    }

    #[test]
    fn insert_and_remove_events() {
        let mut d1 = Doc::with_client_id(1);
        let m1 = d1.get_or_insert_map("map");

        let entries = Arc::new(ArcSwapOption::default());
        let entries_c = entries.clone();
        let _sub = m1.observe(move |_, e| {
            let keys = e.keys();
            entries_c.store(Some(Arc::new(keys.clone())));
        });

        // insert new entry
        {
            let mut txn = d1.transact_mut();
            m1.insert(&mut txn, "a", 1);
            // txn is committed at the end of this scope
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "a".into(),
                EntryChange::Inserted(Any::Number(1.0).into())
            )])))
        );

        // update existing entry once
        {
            let mut txn = d1.transact_mut();
            m1.insert(&mut txn, "a", 2);
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "a".into(),
                EntryChange::Updated(Any::Number(1.0).into(), Any::Number(2.0).into())
            )])))
        );

        // update existing entry twice
        {
            let mut txn = d1.transact_mut();
            m1.insert(&mut txn, "a", 3);
            m1.insert(&mut txn, "a", 4);
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "a".into(),
                EntryChange::Updated(Any::Number(2.0).into(), Any::Number(4.0).into())
            )])))
        );

        // remove existing entry
        {
            let mut txn = d1.transact_mut();
            m1.remove(&mut txn, "a");
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "a".into(),
                EntryChange::Removed(Any::Number(4.0).into())
            )])))
        );

        // add another entry and update it
        {
            let mut txn = d1.transact_mut();
            m1.insert(&mut txn, "b", 1);
            m1.insert(&mut txn, "b", 2);
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "b".into(),
                EntryChange::Inserted(Any::Number(2.0).into())
            )])))
        );

        // add and remove an entry
        {
            let mut txn = d1.transact_mut();
            m1.insert(&mut txn, "c", 1);
            m1.remove(&mut txn, "c");
        }
        assert_eq!(entries.swap(None), Some(HashMap::new().into()));

        // copy updates over
        let mut d2 = Doc::with_client_id(2);
        let m2 = d2.get_or_insert_map("map");

        let entries = Arc::new(ArcSwapOption::default());
        let entries_c = entries.clone();
        let _sub = m2.observe(move |_, e| {
            let keys = e.keys();
            entries_c.store(Some(Arc::new(keys.clone())));
        });

        {
            let t1 = d1.transact_mut();
            let mut t2 = d2.transact_mut();

            let sv = t2.state_vector();
            let mut encoder = EncoderV1::new();
            t1.encode_diff(&sv, &mut encoder);
            t2.apply_update(Update::decode_v1(encoder.to_vec().as_slice()).unwrap())
                .unwrap();
        }
        assert_eq!(
            entries.swap(None),
            Some(Arc::new(HashMap::from([(
                "b".into(),
                EntryChange::Inserted(Any::Number(2.0).into())
            )])))
        );
    }

    fn map_transactions() -> [Box<dyn Fn(&mut Doc, &mut Rng)>; 3] {
        fn set(doc: &mut Doc, rng: &mut Rng) {
            let map = doc.get_or_insert_map("map");
            let mut txn = doc.transact_mut();
            let key = rng.choice(["one", "two"]).unwrap();
            let value: String = rng.random_string();
            map.insert(&mut txn, key.to_string(), value);
        }

        fn set_type(doc: &mut Doc, rng: &mut Rng) {
            let map = doc.get_or_insert_map("map");
            let mut txn = doc.transact_mut();
            let key = rng.choice(["one", "two", "three"]).unwrap();
            if rng.f32() <= 0.33 {
                map.insert(
                    &mut txn,
                    key.to_string(),
                    ArrayPrelim::from(vec![1, 2, 3, 4]),
                );
            } else if rng.f32() <= 0.33 {
                map.insert(&mut txn, key.to_string(), TextPrelim::new("deeptext"));
            } else {
                map.insert(
                    &mut txn,
                    key.to_string(),
                    MapPrelim::from([("deepkey".to_owned(), "deepvalue")]),
                );
            }
        }

        fn delete(doc: &mut Doc, rng: &mut Rng) {
            let map = doc.get_or_insert_map("map");
            let mut txn = doc.transact_mut();
            let key = rng.choice(["one", "two"]).unwrap();
            map.remove(&mut txn, key);
        }
        [Box::new(set), Box::new(set_type), Box::new(delete)]
    }

    fn fuzzy(iterations: usize) {
        run_scenario(0, &map_transactions(), 5, iterations)
    }

    #[test]
    fn fuzzy_test_6() {
        fuzzy(6)
    }

    #[test]
    fn observe_deep() {
        let mut doc = Doc::with_client_id(1);
        let map = doc.get_or_insert_map("map");

        let paths = Arc::new(Mutex::new(vec![]));
        let calls = Arc::new(AtomicU32::new(0));
        let paths_copy = paths.clone();
        let calls_copy = calls.clone();
        let _sub = map.observe_deep(move |_txn, e| {
            let path: Vec<Path> = e.iter().map(Event::path).collect();
            paths_copy.lock().unwrap().push(path);
            calls_copy.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });

        let nested = map.insert(&mut doc.transact_mut(), "map", MapPrelim::default());
        nested.insert(
            &mut doc.transact_mut(),
            "array",
            ArrayPrelim::from(Vec::<String>::default()),
        );
        let nested2: ArrayRef = nested.get(&doc.transact(), "array").unwrap();
        nested2.insert(&mut doc.transact_mut(), 0, "content");

        let nested_text = nested.insert(&mut doc.transact_mut(), "text", TextPrelim::new("text"));
        nested_text.push(&mut doc.transact_mut(), "!");

        assert_eq!(calls.load(Ordering::Relaxed), 5);
        let actual = paths.lock().unwrap();
        assert_eq!(
            actual.as_slice(),
            &[
                vec![Path::from(vec![])],
                vec![Path::from(vec![PathSegment::Key("map".into())])],
                vec![Path::from(vec![
                    PathSegment::Key("map".into()),
                    PathSegment::Key("array".into())
                ])],
                vec![Path::from(vec![PathSegment::Key("map".into()),])],
                vec![Path::from(vec![
                    PathSegment::Key("map".into()),
                    PathSegment::Key("text".into()),
                ])],
            ]
        );
    }

    #[test]
    fn get_or_init() {
        let mut doc = Doc::with_client_id(1);
        let mut txn = doc.transact_mut();
        let map = txn.get_or_insert_map("map");

        let m: MapRef = map.get_or_init(&mut txn, "nested");
        m.insert(&mut txn, "key", 1);
        let m: MapRef = map.get_or_init(&mut txn, "nested");
        assert_eq!(m.get(&txn, "key"), Some(Out::from(1)));

        let m: ArrayRef = map.get_or_init(&mut txn, "nested");
        m.insert(&mut txn, 0, 1);
        let m: ArrayRef = map.get_or_init(&mut txn, "nested");
        assert_eq!(m.get(&txn, 0), Some(Out::from(1)));

        let m: TextRef = map.get_or_init(&mut txn, "nested");
        m.insert(&mut txn, 0, "a");
        let m: TextRef = map.get_or_init(&mut txn, "nested");
        assert_eq!(m.get_string(&txn), "a".to_string());

        let m: XmlFragmentRef = map.get_or_init(&mut txn, "nested");
        m.insert(&mut txn, 0, XmlTextPrelim::new("b"));
        let m: XmlFragmentRef = map.get_or_init(&mut txn, "nested");
        assert_eq!(m.get_string(&txn), "b".to_string());

        let m: XmlTextRef = map.get_or_init(&mut txn, "nested");
        m.insert(&mut txn, 0, "c");
        let m: XmlTextRef = map.get_or_init(&mut txn, "nested");
        assert_eq!(m.get_string(&txn), "c".to_string());
    }

    #[test]
    fn try_update() {
        let mut doc = Doc::new();
        let mut txn = doc.transact_mut();
        let map = txn.get_or_insert_map("map");

        assert!(map.try_update(&mut txn, "key", 1), "new entry");
        assert_eq!(map.get(&txn, "key"), Some(Out::from(1)));

        assert!(
            !map.try_update(&mut txn, "key", 1),
            "unchanged entry shouldn't trigger update"
        );
        assert_eq!(map.get(&txn, "key"), Some(Out::from(1)));

        assert!(map.try_update(&mut txn, "key", 2), "entry should change");
        assert_eq!(map.get(&txn, "key"), Some(Out::from(2)));

        map.remove(&mut txn, "key");
        assert!(
            map.try_update(&mut txn, "key", 2),
            "removed entry should trigger update"
        );
        assert_eq!(map.get(&txn, "key"), Some(Out::from(2)));
    }

    #[test]
    fn get_as() {
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

        let mut doc = Doc::new();
        let mut txn = doc.transact_mut();
        let map = txn.get_or_insert_map("map");

        map.insert(
            &mut txn,
            "orders",
            ArrayPrelim::from([In::from(MapPrelim::from([
                ("shipment_address", In::from("123 Main St")),
                (
                    "items",
                    In::from(MapPrelim::from([
                        (
                            "item1",
                            In::from(MapPrelim::from([
                                ("name", In::from("item1")),
                                ("price", In::from(1.99)),
                                ("quantity", In::from(2)),
                            ])),
                        ),
                        (
                            "item2",
                            In::from(MapPrelim::from([
                                ("name", In::from("item2")),
                                ("price", In::from(2.99)),
                                ("quantity", In::from(1)),
                            ])),
                        ),
                    ])),
                ),
            ]))]),
        );

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

        let actual: Vec<Order> = map.get_as(&txn, "orders").unwrap();
        assert_eq!(actual, vec![expected]);
    }

    #[cfg(feature = "sync")]
    #[test]
    fn multi_threading() {
        use std::sync::{Arc, RwLock};
        use std::thread::{sleep, spawn};
        use std::time::Duration;

        let doc = Arc::new(RwLock::new(Doc::with_client_id(1)));

        let d2 = doc.clone();
        let h2 = spawn(move || {
            for _ in 0..10 {
                let millis = fastrand::u64(1..20);
                sleep(Duration::from_millis(millis));

                let mut doc = d2.write().unwrap();
                let map = doc.get_or_insert_map("test");
                let mut txn = doc.transact_mut();
                map.insert(&mut txn, "key", 1);
            }
        });

        let d3 = doc.clone();
        let h3 = spawn(move || {
            for _ in 0..10 {
                let millis = fastrand::u64(1..20);
                sleep(Duration::from_millis(millis));

                let mut doc = d3.write().unwrap();
                let map = doc.get_or_insert_map("test");
                let mut txn = doc.transact_mut();
                map.insert(&mut txn, "key", 2);
            }
        });

        h3.join().unwrap();
        h2.join().unwrap();

        let doc = doc.read().unwrap();
        let map: MapRef = doc.get("test").unwrap();
        let txn = doc.transact();
        let value: u32 = map.get(&txn, "key").unwrap();

        assert!(value == 1 || value == 2)
    }
}
