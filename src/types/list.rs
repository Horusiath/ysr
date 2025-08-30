use crate::lib0::Value;
use crate::node::NodeType;
use crate::types::Capability;
use crate::{In, Mounted, Transaction};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut, RangeBounds};

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

    pub fn to_value(&self) -> crate::Result<Value> {
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

    pub fn remove(&mut self, index: usize) -> crate::Result<()> {
        //TODO: optimize?
        self.remove_range(index..index + 1)
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

#[repr(transparent)]
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ListPrelim(Vec<In>);

impl Deref for ListPrelim {
    type Target = Vec<In>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ListPrelim {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<In>> for ListPrelim {
    fn from(value: Vec<In>) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod test {
    use crate::lib0::Value;
    use crate::read::DecoderV1;
    use crate::test_util::{multi_doc, sync};
    use crate::{lib0, In, List, MapPrelim, Optional, StateVector, Transaction, Unmounted};
    use std::collections::{BTreeMap, HashMap};

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
            let mut m = BTreeMap::new();
            m.insert("value".to_owned(), In::from(i));
            a.push_back(MapPrelim::from(m)).unwrap();
        }

        for (i, value) in a.iter::<Value>().map(Result::unwrap).enumerate() {
            assert_eq!(value, lib0!({"value": (i as f64) }))
        }
    }

    #[test]
    fn get_at_removed_index() {
        let arr: Unmounted<List> = Unmounted::root("array");
        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();

        let mut a1 = arr.mount_mut(&mut t1).unwrap();

        a1.insert_range(0, ["A"]).unwrap();
        a1.remove(0).unwrap();

        let actual: Option<Value> = a1.get(0).optional().unwrap();
        assert_eq!(actual, None);
    }

    #[test]
    fn insert_empty_range() {
        let arr: Unmounted<List> = Unmounted::root("array");

        let (doc, _) = multi_doc(1);
        let mut tx = doc.transact_mut("test").unwrap();

        let mut array = arr.mount_mut(&mut tx).unwrap();

        let empty: [i32; 0] = [];
        array.insert(0, 1).unwrap();
        array.insert_range(1, empty).unwrap();
        array.push_back(2).unwrap();

        let actual: Vec<_> = array.iter::<Value>().map(Result::unwrap).collect();
        assert_eq!(actual, vec![1.into(), 2.into()]);

        let data = tx.create_update(&StateVector::default()).unwrap();

        tx.commit(None).unwrap();

        let (doc, _) = multi_doc(2);
        let mut tx = doc.transact_mut("test").unwrap();
        tx.apply_update(&mut DecoderV1::from_slice(&data)).unwrap();

        let array = arr.mount_mut(&mut tx).unwrap();

        let actual: Vec<_> = array.iter::<Value>().map(Result::unwrap).collect();
        assert_eq!(actual, vec![1.into(), 2.into()]);
    }
}
