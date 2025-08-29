use crate::node::NodeType;
use crate::types::Capability;
use crate::{Mounted, Transaction};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ops::{Deref, RangeBounds};

pub type ListRef<Txn> = Mounted<List, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct List;

impl Capability for List {
    fn node_type() -> NodeType {
        NodeType::List
    }
}

impl<'tx, 'db> ListRef<&'tx Transaction<'db>> {
    pub fn get<T>(&self, index: usize) -> crate::Result<T>
    where
        T: DeserializeOwned,
    {
        todo!()
    }

    pub fn len(&self) -> usize {
        self.block.clock_len().get() as usize
    }

    pub fn iter<T>(&self) -> Iter<'_, T>
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

    pub fn insert_range<T, I>(&mut self, index: usize, values: I) -> crate::Result<()>
    where
        T: Serialize,
        I: IntoIterator<Item = T>,
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

    pub fn remove_range<R>(&mut self, range: R) -> crate::Result<()>
    where
        R: RangeBounds<usize>,
    {
        todo!()
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

#[cfg(test)]
mod test {
    use crate::lib0::Value;
    use crate::read::DecoderV1;
    use crate::test_util::{multi_doc, sync};
    use crate::{lib0, List, StateVector, Transaction, Unmounted};
    use std::collections::HashMap;

    #[test]
    fn push_back() {
        let arr: Unmounted<List> = Unmounted::root("type");

        let (doc, _) = multi_doc(1);
        let mut tx = doc.transact_mut("test").unwrap();

        let mut a = arr.mount_mut(&mut tx).unwrap();

        a.push_back("a").unwrap();
        a.push_back("b").unwrap();
        a.push_back("c").unwrap();

        let actual: Vec<_> = a.iter::<String>().map(Result::unwrap).collect();
        assert_eq!(actual, vec!["a".to_owned(), "b".into(), "c".into()]);

        tx.commit(None).unwrap();
    }

    #[test]
    fn push_front() {
        let arr: Unmounted<List> = Unmounted::root("type");

        let (doc, _) = multi_doc(1);
        let mut tx = doc.transact_mut("test").unwrap();

        let mut a = arr.mount_mut(&mut tx).unwrap();

        a.push_front("c").unwrap();
        a.push_front("b").unwrap();
        a.push_front("a").unwrap();

        let actual: Vec<_> = a.iter::<String>().map(Result::unwrap).collect();
        assert_eq!(actual, vec!["a".to_owned(), "b".into(), "c".into()]);

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert() {
        let arr: Unmounted<List> = Unmounted::root("type");

        let (doc, _) = multi_doc(1);
        let mut tx = doc.transact_mut("test").unwrap();

        let mut a = arr.mount_mut(&mut tx).unwrap();

        a.insert(0, "a").unwrap();
        a.insert(1, "c").unwrap();
        a.insert(1, "b").unwrap();

        let actual: Vec<_> = a.iter::<String>().map(Result::unwrap).collect();
        assert_eq!(actual, vec!["a".to_owned(), "b".into(), "c".into()]);

        tx.commit(None).unwrap();
    }

    #[test]
    fn basic() {
        let arr: Unmounted<List> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();

        let mut a1 = arr.mount_mut(&mut t1).unwrap();

        a1.insert(0, "Hi").unwrap();
        let update = t1.create_update(&StateVector::default()).unwrap();

        t1.commit(None).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();

        let a2 = arr.mount(&mut t2).unwrap();
        let actual: Vec<_> = a2.iter::<String>().map(Result::unwrap).collect();

        assert_eq!(actual, vec!["Hi".to_string()]);
    }

    #[test]
    fn len() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d, _) = multi_doc(1);

        {
            let mut tx = d.transact_mut("test").unwrap();
            let mut a = arr.mount_mut(&mut tx).unwrap();

            a.push_back(0).unwrap(); // len: 1
            a.push_back(1).unwrap(); // len: 2
            a.push_back(2).unwrap(); // len: 3
            a.push_back(3).unwrap(); // len: 4

            a.remove_range(0..1).unwrap(); // len: 3
            a.insert(0, 0).unwrap(); // len: 4

            assert_eq!(a.len(), 4);

            tx.commit(None).unwrap();
        }
        {
            let mut tx = d.transact_mut("test").unwrap();
            let mut a = arr.mount_mut(&mut tx).unwrap();

            a.remove_range(1..2).unwrap(); // len: 3
            assert_eq!(a.len(), 3);

            a.insert(1, 1).unwrap(); // len: 4
            assert_eq!(a.len(), 4);

            a.remove_range(2..3).unwrap(); // len: 3
            assert_eq!(a.len(), 3);

            a.insert(2, 2).unwrap(); // len: 4
            assert_eq!(a.len(), 4);

            tx.commit(None).unwrap();
        }

        let mut tx = d.transact_mut("test").unwrap();
        let mut a = arr.mount_mut(&mut tx).unwrap();

        assert_eq!(a.len(), 4);

        a.remove_range(1..2).unwrap();
        assert_eq!(a.len(), 3);

        a.insert(1, 1).unwrap();
        assert_eq!(a.len(), 4);

        tx.commit(None).unwrap();
    }

    #[test]
    fn remove_insert() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d, _) = multi_doc(1);
        let mut t1 = d.transact_mut("test").unwrap();
        let mut a1 = arr.mount_mut(&mut t1).unwrap();

        a1.insert(0, "A").unwrap();
        a1.remove_range(1..1).unwrap();

        t1.commit(None).unwrap();
    }

    #[test]
    fn insert_3_elements_try_re_get() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        {
            let mut t1 = d1.transact_mut("test").unwrap();
            let mut a1 = arr.mount_mut(&mut t1).unwrap();

            a1.push_back(1).unwrap();
            a1.push_back(true).unwrap();
            a1.push_back(false).unwrap();
            let actual: Vec<_> = a1.iter::<Value>().map(Result::unwrap).collect();
            assert_eq!(
                actual,
                vec![Value::Float(1.0), Value::Bool(true), Value::Bool(false)]
            );

            t1.commit(None).unwrap();
        }

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        sync([&mut t1, &mut t2]);

        t1.commit(None).unwrap();

        let a2 = arr.mount_mut(&mut t2).unwrap();

        let actual: Vec<_> = a2.iter::<Value>().map(Result::unwrap).collect();
        assert_eq!(
            actual,
            vec![Value::Float(1.0), Value::Bool(true), Value::Bool(false)]
        );

        t2.commit(None).unwrap();
    }

    #[test]
    fn concurrent_insert_with_3_conflicts() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);

        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut a = arr.mount_mut(&mut txn).unwrap();
            a.insert(0, 0).unwrap();
            txn.commit(None).unwrap();
        }

        {
            let mut txn = d2.transact_mut("test").unwrap();
            let mut a = arr.mount_mut(&mut txn).unwrap();
            a.insert(0, 1).unwrap();
            txn.commit(None).unwrap();
        }

        {
            let mut txn = d3.transact_mut("test").unwrap();
            let mut a = arr.mount_mut(&mut txn).unwrap();
            a.insert(0, 2).unwrap();
            txn.commit(None).unwrap();
        }

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();

        sync([&mut t1, &mut t2, &mut t3]);

        let a1 = to_array(&mut t1);
        let a2 = to_array(&mut t2);
        let a3 = to_array(&mut t3);

        assert_eq!(a1, a2, "Peer 1 and peer 2 states are different");
        assert_eq!(a2, a3, "Peer 2 and peer 3 states are different");
    }

    fn to_array(tx: &mut Transaction<'_>) -> Vec<Value> {
        let arr: Unmounted<List> = Unmounted::root("array");
        let a = arr.mount(tx).unwrap();
        a.iter::<Value>().map(Result::unwrap).collect()
    }

    #[test]
    fn concurrent_insert_remove_with_3_conflicts() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();

        {
            let mut a = arr.mount_mut(&mut t1).unwrap();
            a.insert_range(0, ["x", "y", "z"]).unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3]);

        {
            // start state: [x,y,z]
            let mut a1 = arr.mount_mut(&mut t1).unwrap();
            let mut a2 = arr.mount_mut(&mut t2).unwrap();
            let mut a3 = arr.mount_mut(&mut t3).unwrap();

            a1.insert(1, 0).unwrap(); // [x,0,y,z]
            a2.remove_range(0..1).unwrap(); // [y,z]
            a2.remove_range(1..2).unwrap(); // [y]
            a3.insert(1, 2).unwrap(); // [x,2,y,z]
        }

        sync([&mut t1, &mut t2, &mut t3]);
        // after exchange expected: [0,2,y]

        let a1 = to_array(&mut t1);
        let a2 = to_array(&mut t2);
        let a3 = to_array(&mut t3);

        assert_eq!(a1, a2, "Peer 1 and peer 2 states are different");
        assert_eq!(a2, a3, "Peer 2 and peer 3 states are different");
    }

    #[test]
    fn insertions_in_late_sync() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);
        let (d3, _) = multi_doc(3);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut t3 = d3.transact_mut("test").unwrap();

        {
            let mut a = arr.mount_mut(&mut t1).unwrap();
            a.push_back("x").unwrap();
            a.push_back("y").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3]);

        {
            let mut a1 = arr.mount_mut(&mut t1).unwrap();
            let mut a2 = arr.mount_mut(&mut t2).unwrap();
            let mut a3 = arr.mount_mut(&mut t3).unwrap();

            a1.insert(1, "user0").unwrap();
            a2.insert(1, "user1").unwrap();
            a3.insert(1, "user2").unwrap();
        }

        sync([&mut t1, &mut t2, &mut t3]);

        let a1 = to_array(&mut t1);
        let a2 = to_array(&mut t2);
        let a3 = to_array(&mut t3);

        assert_eq!(a1, a2, "Peer 1 and peer 2 states are different");
        assert_eq!(a2, a3, "Peer 2 and peer 3 states are different");
    }

    #[test]
    fn removals_in_late_sync() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        {
            let mut a = arr.mount_mut(&mut t1).unwrap();
            a.push_back("x").unwrap();
            a.push_back("y").unwrap();
        }

        sync([&mut t1, &mut t2]);

        {
            let mut a1 = arr.mount_mut(&mut t1).unwrap();
            let mut a2 = arr.mount_mut(&mut t2).unwrap();

            a2.remove_range(1..2).unwrap();
            a1.remove_range(0..2).unwrap();
        }

        sync([&mut t1, &mut t2]);

        let a1 = to_array(&mut t1);
        let a2 = to_array(&mut t2);

        assert_eq!(a1, a2, "Peer 1 and peer 2 states are different");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn insert_then_merge_delete_on_sync() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        {
            let mut a = arr.mount_mut(&mut t1).unwrap();

            a.push_back("x").unwrap();
            a.push_back("y").unwrap();
            a.push_back("z").unwrap();
        }

        sync([&mut t1, &mut t2]);

        {
            let mut a2 = arr.mount_mut(&mut t2).unwrap();

            a2.remove_range(0..3).unwrap();
        }

        sync([&mut t1, &mut t2]);

        let a1 = to_array(&mut t1);
        let a2 = to_array(&mut t2);

        assert_eq!(a1, a2, "Peer 1 and peer 2 states are different");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn iter_array_containing_types() {
        let arr: Unmounted<List> = Unmounted::root("array");
        let (d, _) = multi_doc(1);
        let mut tx = d.transact_mut("test").unwrap();

        let mut a = arr.mount_mut(&mut tx).unwrap();

        for i in 0..10 {
            let mut m = HashMap::new();
            m.insert("value".to_owned(), i);
            a.push_back(MapPrelim::from_iter(m));
        }

        for (i, value) in a.iter().map(Result::unwrap).enumerate() {
            match value {
                Out::Map(_) => {
                    assert_eq!(value.to_json(&txn), lib0!({"value": (i as f64) }))
                }
                _ => panic!("Value of array at index {} was no YMap", i),
            }
        }
    }

    #[test]
    fn insert_and_remove_events() {
        let mut d = Doc::with_client_id(1);
        let array = d.get_or_insert_array("array");
        let happened = Arc::new(AtomicBool::new(false));
        let happened_clone = happened.clone();
        let _sub = array.observe(move |_, _| {
            happened_clone.store(true, Ordering::Relaxed);
        });

        {
            let mut txn = d.transact_mut();
            array.insert_range(&mut txn, 0, [0, 1, 2]);
            // txn is committed at the end of this scope
        }
        assert!(
            happened.swap(false, Ordering::Relaxed),
            "insert of [0,1,2] should trigger event"
        );

        {
            let mut txn = d.transact_mut();
            array.remove_range(&mut txn, 0, 1);
            // txn is committed at the end of this scope
        }
        assert!(
            happened.swap(false, Ordering::Relaxed),
            "removal of [0] should trigger event"
        );

        {
            let mut txn = d.transact_mut();
            array.remove_range(&mut txn, 0, 2);
            // txn is committed at the end of this scope
        }
        assert!(
            happened.swap(false, Ordering::Relaxed),
            "removal of [1,2] should trigger event"
        );
    }

    #[test]
    fn insert_and_remove_event_changes() {
        let mut d1 = Doc::with_client_id(1);
        let array = d1.get_or_insert_array("array");
        let added = Arc::new(ArcSwapOption::default());
        let removed = Arc::new(ArcSwapOption::default());
        let delta = Arc::new(ArcSwapOption::default());

        let (added_c, removed_c, delta_c) = (added.clone(), removed.clone(), delta.clone());
        let _sub = array.observe(move |_, e| {
            added_c.store(Some(Arc::new(e.inserts().clone())));
            removed_c.store(Some(Arc::new(e.removes().clone())));
            delta_c.store(Some(Arc::new(e.delta().to_vec())));
        });

        {
            let mut txn = d1.transact_mut();
            array.push_back(&mut txn, 4);
            array.push_back(&mut txn, "dtrn");
            // txn is committed at the end of this scope
        }
        assert_eq!(
            added.swap(None),
            Some(HashSet::from([ID::new(1, 0), ID::new(1, 1)]).into())
        );
        assert_eq!(removed.swap(None), Some(HashSet::new().into()));
        assert_eq!(
            delta.swap(None),
            Some(
                vec![Change::Added(vec![
                    Any::Number(4.0).into(),
                    Any::String("dtrn".into()).into()
                ])]
                .into()
            )
        );

        {
            let mut txn = d1.transact_mut();
            array.remove_range(&mut txn, 0, 1);
        }
        assert_eq!(added.swap(None), Some(HashSet::new().into()));
        assert_eq!(
            removed.swap(None),
            Some(HashSet::from([ID::new(1, 0)]).into())
        );
        assert_eq!(delta.swap(None), Some(vec![Change::Removed(1)].into()));

        {
            let mut txn = d1.transact_mut();
            array.insert(&mut txn, 1, 0.5);
        }
        assert_eq!(
            added.swap(None),
            Some(HashSet::from([ID::new(1, 2)]).into())
        );
        assert_eq!(removed.swap(None), Some(HashSet::new().into()));
        assert_eq!(
            delta.swap(None),
            Some(
                vec![
                    Change::Retain(1),
                    Change::Added(vec![Any::Number(0.5).into()])
                ]
                .into()
            )
        );

        let mut d2 = Doc::with_client_id(2);
        let array2 = d2.get_or_insert_array("array");
        let (added_c, removed_c, delta_c) = (added.clone(), removed.clone(), delta.clone());
        let _sub = array2.observe(move |_, e| {
            added_c.store(Some(e.inserts().clone().into()));
            removed_c.store(Some(e.removes().clone().into()));
            delta_c.store(Some(e.delta().to_vec().into()));
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
            added.swap(None),
            Some(HashSet::from([ID::new(1, 1)]).into())
        );
        assert_eq!(removed.swap(None), Some(HashSet::new().into()));
        assert_eq!(
            delta.swap(None),
            Some(
                vec![Change::Added(vec![
                    Any::String("dtrn".into()).into(),
                    Any::Number(0.5).into(),
                ])]
                .into()
            )
        );
    }

    #[test]
    fn target_on_local_and_remote() {
        let mut d1 = Doc::with_client_id(1);
        let mut d2 = Doc::with_client_id(2);
        let a1 = d1.get_or_insert_array("array");
        let a2 = d2.get_or_insert_array("array");

        let c1 = Arc::new(ArcSwapOption::default());
        let c1c = c1.clone();
        let _s1 = a1.observe(move |_, e| {
            c1c.store(Some(e.target().hook().into()));
        });
        let c2 = Arc::new(ArcSwapOption::default());
        let c2c = c2.clone();
        let _s2 = a2.observe(move |_, e| {
            c2c.store(Some(e.target().hook().into()));
        });

        {
            let mut t1 = d1.transact_mut();
            a1.insert_range(&mut t1, 0, [1, 2]);
        }
        sync([&mut d1, &mut d2]);

        assert_eq!(c1.swap(None), Some(Arc::new(a1.hook())));
        assert_eq!(c2.swap(None), Some(Arc::new(a2.hook())));
    }

    static UNIQUE_NUMBER: AtomicI64 = AtomicI64::new(0);

    fn get_unique_number() -> i64 {
        UNIQUE_NUMBER.fetch_add(1, Ordering::SeqCst)
    }

    fn array_transactions() -> [Box<dyn Fn(&mut Doc, &mut Rng)>; 5] {
        fn move_one(doc: &mut Doc, rng: &mut Rng) {
            let yarray = doc.get_or_insert_array("array");
            let mut txn = doc.transact_mut();
            if yarray.len(&txn) != 0 {
                let pos = rng.between(0, yarray.len(&txn) - 1);
                let len = 1;
                let new_pos_adjusted = rng.between(0, yarray.len(&txn) - 1);
                let new_pos = new_pos_adjusted + if new_pos_adjusted > pos { len } else { 0 };
                if let Any::Array(expected) = yarray.to_json(&txn) {
                    let mut expected = Vec::from(expected.as_ref());
                    let moved = expected.remove(pos as usize);
                    let insert_pos = if pos < new_pos {
                        new_pos - len
                    } else {
                        new_pos
                    } as usize;
                    expected.insert(insert_pos, moved);

                    yarray.move_to(&mut txn, pos, new_pos);

                    let actual = yarray.to_json(&txn);
                    assert_eq!(actual, Any::from(expected))
                } else {
                    panic!("should not happen")
                }
            }
        }
        fn insert(doc: &mut Doc, rng: &mut Rng) {
            let yarray = doc.get_or_insert_array("array");
            let mut txn = doc.transact_mut();
            let unique_number = get_unique_number();
            let len = rng.between(1, 4);
            let content: Vec<_> = (0..len)
                .into_iter()
                .map(|_| Any::BigInt(unique_number))
                .collect();
            let mut pos = rng.between(0, yarray.len(&txn)) as usize;
            if let Any::Array(expected) = yarray.to_json(&txn) {
                let mut expected = Vec::from(expected.as_ref());
                yarray.insert_range(&mut txn, pos as u32, content.clone());

                for any in content {
                    expected.insert(pos, any);
                    pos += 1;
                }
                let actual = yarray.to_json(&txn);
                assert_eq!(actual, Any::from(expected))
            } else {
                panic!("should not happen")
            }
        }

        fn insert_type_array(doc: &mut Doc, rng: &mut Rng) {
            let yarray = doc.get_or_insert_array("array");
            let mut txn = doc.transact_mut();
            let pos = rng.between(0, yarray.len(&txn));
            let array2 = yarray.insert(&mut txn, pos, ArrayPrelim::from([1, 2, 3, 4]));
            let expected: Arc<[Any]> = (1..=4).map(|i| Any::Number(i as f64)).collect();
            assert_eq!(array2.to_json(&txn), Any::Array(expected));
        }

        fn insert_type_map(doc: &mut Doc, rng: &mut Rng) {
            let yarray = doc.get_or_insert_array("array");
            let mut txn = doc.transact_mut();
            let pos = rng.between(0, yarray.len(&txn));
            let map = yarray.insert(&mut txn, pos, MapPrelim::default());
            map.insert(&mut txn, "someprop".to_string(), 42);
            map.insert(&mut txn, "someprop".to_string(), 43);
            map.insert(&mut txn, "someprop".to_string(), 44);
        }

        fn delete(doc: &mut Doc, rng: &mut Rng) {
            let yarray = doc.get_or_insert_array("array");
            let mut txn = doc.transact_mut();
            let len = yarray.len(&txn);
            if len > 0 {
                let pos = rng.between(0, len - 1);
                let del_len = rng.between(1, 2.min(len - pos));
                if rng.bool() {
                    if let Out::Array(array2) = yarray.get(&txn, pos).unwrap() {
                        let pos = rng.between(0, array2.len(&txn) - 1);
                        let del_len = rng.between(0, 2.min(array2.len(&txn) - pos));
                        array2.remove_range(&mut txn, pos, del_len);
                    }
                } else {
                    if let Any::Array(old_content) = yarray.to_json(&txn) {
                        let mut old_content = Vec::from(old_content.as_ref());
                        yarray.remove_range(&mut txn, pos, del_len);
                        old_content.drain(pos as usize..(pos + del_len) as usize);
                        assert_eq!(yarray.to_json(&txn), Any::from(old_content));
                    } else {
                        panic!("should not happen")
                    }
                }
            }
        }

        [
            Box::new(insert),
            Box::new(insert_type_array),
            Box::new(insert_type_map),
            Box::new(delete),
            Box::new(move_one),
        ]
    }

    fn fuzzy(iterations: usize) {
        run_scenario(0, &array_transactions(), 5, iterations)
    }

    #[test]
    fn fuzzy_test_6() {
        fuzzy(6)
    }

    #[test]
    fn fuzzy_test_300() {
        fuzzy(300)
    }

    #[test]
    fn get_at_removed_index() {
        let mut d1 = Doc::with_client_id(1);
        let a1 = d1.get_or_insert_array("array");
        let mut t1 = d1.transact_mut();

        a1.insert_range(&mut t1, 0, ["A"]);
        a1.remove(&mut t1, 0);

        let actual: Option<Out> = a1.get(&t1, 0);
        assert_eq!(actual, None);
    }

    #[test]
    fn observe_deep_event_order() {
        let mut doc = Doc::with_client_id(1);
        let array = doc.get_or_insert_array("array");

        let paths = Arc::new(Mutex::new(vec![]));
        let paths_copy = paths.clone();

        let _sub = array.observe_deep(move |_txn, e| {
            let path: Vec<Path> = e.iter().map(Event::path).collect();
            paths_copy.lock().unwrap().push(path);
        });

        array.insert(&mut doc.transact_mut(), 0, MapPrelim::default());

        {
            let mut txn = doc.transact_mut();
            let map: MapRef = array.get(&txn, 0).unwrap();
            map.insert(&mut txn, "a", "a");
            array.insert(&mut txn, 0, 0);
        }

        let expected = &[
            vec![Path::default()],
            vec![Path::default(), Path::from([PathSegment::Index(1)])],
        ];
        let actual = paths.lock().unwrap();
        assert_eq!(actual.as_slice(), expected);
    }

    #[test]
    fn move_1() {
        let mut d1 = Doc::with_client_id(1);
        let a1 = d1.get_or_insert_array("array");

        let mut d2 = Doc::with_client_id(2);
        let a2 = d2.get_or_insert_array("array");

        let e1 = Arc::new(ArcSwapOption::default());
        let inner = e1.clone();
        let _s1 = a1.observe(move |_, e| {
            inner.store(Some(Arc::new(e.delta().to_vec())));
        });

        let e2 = Arc::new(ArcSwapOption::default());
        let inner = e2.clone();
        let _s2 = a2.observe(move |_, e| {
            inner.store(Some(Arc::new(e.delta().to_vec())));
        });

        {
            let mut txn = d1.transact_mut();
            a1.insert_range(&mut txn, 0, [1, 2, 3]);
            a1.move_to(&mut txn, 1, 0);
        }
        assert_eq!(a1.to_json(&d1.transact()), vec![2, 1, 3].into());

        sync([&mut d1, &mut d2]);

        assert_eq!(a2.to_json(&d2.transact()), vec![2, 1, 3].into());
        let actual = e2.load_full();
        assert_eq!(
            actual,
            Some(Arc::new(vec![Change::Added(vec![
                2.into(),
                1.into(),
                3.into()
            ])]))
        );

        a1.move_to(&mut d1.transact_mut(), 0, 2);

        assert_eq!(a1.to_json(&d1.transact()), vec![1, 2, 3].into());
        let actual = e1.load_full();
        assert_eq!(
            actual,
            Some(Arc::new(vec![
                Change::Removed(1),
                Change::Retain(1),
                Change::Added(vec![2.into()])
            ]))
        )
    }

    #[test]
    fn move_2() {
        let mut d1 = Doc::with_client_id(1);
        let a1 = d1.get_or_insert_array("array");

        let mut d2 = Doc::with_client_id(2);
        let a2 = d2.get_or_insert_array("array");

        let e1 = Arc::new(ArcSwapOption::default());
        let inner = e1.clone();
        let _s1 = a1.observe(move |_, e| {
            inner.store(Some(Arc::new(e.delta().to_vec())));
        });

        let e2 = Arc::new(ArcSwapOption::default());
        let inner = e2.clone();
        let _s2 = a2.observe(move |_, e| {
            inner.store(Some(Arc::new(e.delta().to_vec())));
        });

        a1.insert_range(&mut d1.transact_mut(), 0, [1, 2]);
        a1.move_to(&mut d1.transact_mut(), 1, 0);
        assert_eq!(a1.to_json(&d1.transact()), vec![2, 1].into());
        {
            let actual = e1.load_full();
            assert_eq!(
                actual,
                Some(Arc::new(vec![
                    Change::Added(vec![2.into()]),
                    Change::Retain(1),
                    Change::Removed(1)
                ]))
            );
        }

        sync([&mut d1, &mut d2]);

        assert_eq!(a2.to_json(&d2.transact()), vec![2, 1].into());
        {
            let actual = e2.load_full();
            assert_eq!(
                actual,
                Some(Arc::new(vec![Change::Added(vec![2.into(), 1.into()])]))
            );
        }

        a1.move_to(&mut d1.transact_mut(), 0, 2);
        assert_eq!(a1.to_json(&d1.transact()), vec![1, 2].into());
        {
            let actual = e1.load_full();
            assert_eq!(
                actual,
                Some(Arc::new(vec![
                    Change::Removed(1),
                    Change::Retain(1),
                    Change::Added(vec![2.into()])
                ]))
            );
        }
    }

    #[test]
    fn move_cycles() {
        let mut d1 = Doc::with_client_id(1);
        let a1 = d1.get_or_insert_array("array");

        let mut d2 = Doc::with_client_id(2);
        let a2 = d2.get_or_insert_array("array");

        a1.insert_range(&mut d1.transact_mut(), 0, [1, 2, 3, 4]);
        sync([&mut d1, &mut d2]);

        a1.move_range_to(&mut d1.transact_mut(), 0, Assoc::After, 1, Assoc::Before, 3);
        assert_eq!(a1.to_json(&d1.transact()), vec![3, 1, 2, 4].into());

        a2.move_range_to(&mut d2.transact_mut(), 2, Assoc::After, 3, Assoc::Before, 1);
        assert_eq!(a2.to_json(&d2.transact()), vec![1, 3, 4, 2].into());

        sync([&mut d1, &mut d2]);
        sync([&mut d1, &mut d2]); // move cycles may not be detected within a single update exchange

        assert_eq!(a1.len(&d1.transact()), 4);
        assert_eq!(a1.to_json(&d1.transact()), a2.to_json(&d2.transact()));
    }

    #[test]
    #[ignore] //TODO: investigate (see: https://github.com/y-crdt/y-crdt/pull/266)
    fn move_range_to() {
        let mut doc = Doc::with_client_id(1);
        let arr = doc.get_or_insert_array("array");
        // Move 1-2 to 4
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            1,
            Assoc::After,
            2,
            Assoc::Before,
            4,
        );
        assert_eq!(arr.to_json(&doc.transact()), vec![0, 3, 1, 2].into());

        // Move 0-0 to 10
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            0,
            Assoc::After,
            0,
            Assoc::Before,
            10,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0].into()
        );

        // Move 0-1 to 10
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            0,
            Assoc::After,
            1,
            Assoc::Before,
            10,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![2, 3, 4, 5, 6, 7, 8, 9, 0, 1].into()
        );

        // Move 3-5 to 7
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            3,
            Assoc::After,
            5,
            Assoc::Before,
            7,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![0, 1, 2, 6, 3, 4, 5, 7, 8, 9].into()
        );

        // Move 1-0 to 10
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            1,
            Assoc::After,
            0,
            Assoc::Before,
            10,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into()
        );

        // Move 3-5 to 5
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            3,
            Assoc::After,
            5,
            Assoc::Before,
            5,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into()
        );

        // Move 9-9 to 0
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            9,
            Assoc::After,
            9,
            Assoc::Before,
            0,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![9, 0, 1, 2, 3, 4, 5, 6, 7, 8].into()
        );

        // Move 8-9 to 0
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            8,
            Assoc::After,
            9,
            Assoc::Before,
            0,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![8, 9, 0, 1, 2, 3, 4, 5, 6, 7].into()
        );

        // Move 4-6 to 3
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            4,
            Assoc::After,
            6,
            Assoc::Before,
            3,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![0, 1, 2, 4, 5, 6, 3, 7, 8, 9].into()
        );

        // Move 3-5 to 3
        {
            let mut txn = doc.transact_mut();
            let arr_len = arr.len(&txn);
            arr.remove_range(&mut txn, 0, arr_len);
            let arr_len = arr.len(&txn);
            assert_eq!(arr_len, 0);
            arr.insert_range(&mut txn, arr_len, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
        arr.move_range_to(
            &mut doc.transact_mut(),
            3,
            Assoc::After,
            5,
            Assoc::Before,
            3,
        );
        assert_eq!(
            arr.to_json(&doc.transact()),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into()
        );
    }

    #[cfg(feature = "sync")]
    #[test]
    fn multi_threading() {
        use crate::ArrayRef;
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
                let array = doc.get_or_insert_array("test");
                let mut txn = doc.transact_mut();
                array.push_back(&mut txn, "a");
            }
        });

        let d3 = doc.clone();
        let h3 = spawn(move || {
            for _ in 0..10 {
                let millis = fastrand::u64(1..20);
                sleep(Duration::from_millis(millis));

                let mut doc = d3.write().unwrap();
                let array = doc.get_or_insert_array("test");
                let mut txn = doc.transact_mut();
                array.push_back(&mut txn, "b");
            }
        });

        h3.join().unwrap();
        h2.join().unwrap();

        let doc = doc.read().unwrap();
        let array: ArrayRef = doc.get("test").unwrap();
        let len = array.len(&doc.transact());
        assert_eq!(len, 20);
    }

    #[test]
    fn move_last_elem_iter() {
        // https://github.com/y-crdt/y-crdt/issues/186

        let mut doc = Doc::with_client_id(1);
        let array = doc.get_or_insert_array("array");
        let mut txn = doc.transact_mut();
        array.insert_range(&mut txn, 0, [1, 2, 3]);
        drop(txn);

        let mut txn = doc.transact_mut();
        array.move_to(&mut txn, 2, 0);

        let mut iter = array.iter(&txn);
        let v = iter.next();
        assert_eq!(v, Some(3.into()));
        let v = iter.next();
        assert_eq!(v, Some(1.into()));
        let v = iter.next();
        assert_eq!(v, Some(2.into()));
        let v = iter.next();
        assert_eq!(v, None);
    }

    #[test]
    fn insert_empty_range() {
        let mut doc = Doc::with_client_id(1);
        let mut txn = doc.transact_mut();
        let array = txn.get_or_insert_array("array");

        array.insert(&mut txn, 0, 1);
        array.insert_range::<_, Any>(&mut txn, 1, []);
        array.push_back(&mut txn, 2);

        assert_eq!(
            array.iter(&txn).collect::<Vec<_>>(),
            vec![1.into(), 2.into()]
        );

        let data = txn.encode_state_as_update_v1(&StateVector::default());

        let mut doc2 = Doc::with_client_id(2);
        let mut txn = doc2.transact_mut();
        let array = txn.get_or_insert_array("array");
        txn.apply_update(Update::decode_v1(&data).unwrap()).unwrap();

        assert_eq!(
            array.iter(&txn).collect::<Vec<_>>(),
            vec![1.into(), 2.into()]
        );
    }
}
