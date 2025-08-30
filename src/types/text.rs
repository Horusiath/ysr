use crate::content::BlockContent;
use crate::lib0::Value;
use crate::node::NodeType;
use crate::store::lmdb::BlockStore;
use crate::types::Capability;
use crate::{In, Mounted, Out, Transaction};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, RangeBounds};

pub type TextRef<Txn> = Mounted<Text, Txn>;

#[derive(Clone, Debug, Default, Eq, Ord, PartialOrd, PartialEq)]
pub struct Text;

impl Capability for Text {
    fn node_type() -> NodeType {
        NodeType::Text
    }
}

impl<'tx, 'db> TextRef<&'tx Transaction<'db>> {
    pub fn len(&self) -> usize {
        self.block.clock_len().get() as usize
    }

    pub fn chunks(&self) -> impl Iterator<Item = crate::Result<(Value, Option<Box<Attrs>>)>> {
        todo!()
    }
}

impl<'tx, 'db> Display for TextRef<&'tx Transaction<'db>> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let Ok(BlockContent::Node(node)) = self.block.content() else {
            return Err(std::fmt::Error);
        };
        let mut next = node.header().start().cloned();
        let db = self.tx.db();
        while let Some(id) = next {
            let Ok(block) = db.block_containing(id, false) else {
                return Err(std::fmt::Error);
            };
            if block.is_countable() && !block.is_deleted() {
                let Ok(BlockContent::Text(chunk)) = block.content() else {
                    return Err(std::fmt::Error);
                };
                write!(f, "{}", chunk)?;
            }
            next = block.right().cloned();
        }

        Ok(())
    }
}

impl<'tx, 'db> TextRef<&'tx mut Transaction<'db>> {
    pub fn insert<S>(&mut self, index: usize, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        todo!()
    }

    pub fn insert_with<S1, S2, A, V>(
        &mut self,
        index: usize,
        chunk: S1,
        attrs: A,
    ) -> crate::Result<()>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
        V: Serialize,
        A: IntoIterator<Item = (S2, V)>,
    {
        todo!()
    }

    pub fn insert_embed<V>(&mut self, index: usize, value: V) -> crate::Result<()> {
        todo!()
    }

    pub fn insert_embed_with<S, A, V1, V2>(
        &mut self,
        index: usize,
        chunk: S,
        attrs: A,
    ) -> crate::Result<()>
    where
        S: AsRef<str>,
        V2: Serialize,
        A: IntoIterator<Item = (S, V2)>,
    {
        todo!()
    }

    pub fn push<S>(&mut self, chunk: S) -> crate::Result<()>
    where
        S: AsRef<str>,
    {
        todo!()
    }

    pub fn remove_range<R>(&mut self, range: R) -> crate::Result<()>
    where
        R: RangeBounds<usize>,
    {
        todo!()
    }

    pub fn format<A, S, V>(&mut self, start: usize, end: usize, attrs: A) -> crate::Result<()>
    where
        S: AsRef<str>,
        V: Serialize,
        A: IntoIterator<Item = (S, V)>,
    {
        todo!()
    }

    pub fn apply_delta<I>(&mut self, delta: I) -> crate::Result<()>
    where
        I: IntoIterator<Item = Delta<In>>,
    {
        todo!()
    }
}

impl<'tx, 'db> Deref for TextRef<&'tx mut Transaction<'db>> {
    type Target = TextRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const TextRef<&'tx Transaction<'db>>) }
    }
}

pub type Attrs = BTreeMap<String, Value>;

/// A single change done over a text-like types: [Text] or [XmlText].
#[derive(Debug, Clone, PartialEq)]
pub enum Delta<T = Out> {
    /// Determines a change that resulted in insertion of a piece of text, which optionally could
    /// have been formatted with provided set of attributes.
    Inserted(T, Option<Box<Attrs>>),

    /// Determines a change that resulted in removing a consecutive range of characters.
    Deleted(u32),

    /// Determines a number of consecutive unchanged characters. Used to recognize non-edited spaces
    /// between [Delta::Inserted] and/or [Delta::Deleted] chunks. Can contain an optional set of
    /// attributes, which have been used to format an existing piece of text.
    Retain(u32, Option<Box<Attrs>>),
}

impl<T> Delta<T> {
    pub fn map<U, F>(self, f: F) -> Delta<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Delta::Inserted(value, attrs) => Delta::Inserted(f(value), attrs),
            Delta::Deleted(len) => Delta::Deleted(len),
            Delta::Retain(len, attrs) => Delta::Retain(len, attrs),
        }
    }
}

impl Delta<In> {
    pub fn retain(len: u32) -> Self {
        Delta::Retain(len, None)
    }

    pub fn insert<T: Into<In>>(value: T) -> Self {
        Delta::Inserted(value.into(), None)
    }

    pub fn insert_with<T: Into<In>>(value: T, attrs: Attrs) -> Self {
        Delta::Inserted(value.into(), Some(Box::new(attrs)))
    }

    pub fn delete(len: u32) -> Self {
        Delta::Deleted(len)
    }
}

#[cfg(test)]
mod test {
    use crate::lib0::Value;
    use crate::read::{Decode, DecoderV1};
    use crate::test_util::{multi_doc, sync};
    use crate::types::text::{Attrs, Delta};
    use crate::write::Encode;
    use crate::{lib0, ListPrelim, MapPrelim, StateVector, Text, Unmounted};

    #[test]
    fn insert_empty_string() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        assert_eq!(txt.to_string(), "");

        txt.push("").unwrap();
        assert_eq!(txt.to_string(), "");

        txt.push("abc").unwrap();
        txt.push("").unwrap();
        assert_eq!(txt.to_string(), "abc");

        tx.commit(None).unwrap();
    }

    #[test]
    fn append_single_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(1, "b").unwrap();
        txt.insert(2, "c").unwrap();

        assert_eq!(txt.to_string(), "abc");

        tx.commit(None).unwrap();
    }

    #[test]
    fn append_mutli_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(5, " ").unwrap();
        txt.insert(6, "world").unwrap();

        assert_eq!(txt.to_string(), "hello world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn prepend_single_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(0, "b").unwrap();
        txt.insert(0, "c").unwrap();

        assert_eq!(txt.to_string(), "cba");

        tx.commit(None).unwrap();
    }

    #[test]
    fn prepend_mutli_character_blocks() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(0, " ").unwrap();
        txt.insert(0, "world").unwrap();

        assert_eq!(txt.to_string(), "world hello");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_after_block() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello").unwrap();
        txt.insert(5, " ").unwrap();
        txt.insert(6, "world").unwrap();
        txt.insert(6, "beautiful ").unwrap();

        assert_eq!(txt.to_string(), "hello beautiful world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_inside_of_block() {
        let (mdoc, _dir) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "it was expected").unwrap();
        txt.insert(6, " not").unwrap();

        assert_eq!(txt.to_string(), "it was not expected");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_concurrent_root() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "hello ").unwrap();

        let (d2, _) = multi_doc(1);
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut txt2 = txt.mount_mut(&mut t2).unwrap();

        txt2.insert(0, "world").unwrap();

        drop(txt1);
        drop(txt2);

        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let d2_sv = t2.state_vector().unwrap().encode().unwrap();

        let u1 = t1
            .create_update(&StateVector::decode(&d2_sv).unwrap())
            .unwrap();
        let u2 = t2
            .create_update(&StateVector::decode(&d1_sv).unwrap())
            .unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let a = txt.mount(&t1).unwrap().to_string();
        let b = txt.mount(&t2).unwrap().to_string();

        assert_eq!(a, b);
        assert_eq!(a.as_str(), "hello world");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn insert_concurrent_in_the_middle() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "I expect that").unwrap();
        assert_eq!(txt1.to_string(), "I expect that");

        let (d2, _) = multi_doc(1);
        let mut t2 = d2.transact_mut("test").unwrap();

        drop(txt1);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .create_update(&StateVector::decode(&d2_sv).unwrap())
            .unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "I expect that");

        txt2.insert(1, " have").unwrap();
        txt2.insert(13, "ed").unwrap();
        assert_eq!(txt2.to_string(), "I have expected that");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(1, " didn't").unwrap();
        assert_eq!(txt1.to_string(), "I didn't expect that");

        drop(txt1);
        drop(txt2);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .create_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .create_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
            .unwrap();
        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();

        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a, b);
        assert_eq!(a.as_str(), "I didn't have expected that");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn append_concurrent() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "aaa").unwrap();
        assert_eq!(txt1.to_string(), "aaa");

        drop(txt1);

        let (d2, _) = multi_doc(1);
        let mut t2 = d2.transact_mut("test").unwrap();

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .create_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "aaa");

        txt2.insert(3, "bbb").unwrap();
        txt2.insert(6, "bbb").unwrap();
        assert_eq!(txt2.to_string(), "aaabbbbbb");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(3, "aaa").unwrap();
        assert_eq!(txt1.to_string(), "aaaaaa");

        drop(txt1);
        drop(txt2);

        let d2_sv = t2.state_vector().unwrap().encode().unwrap();
        let d1_sv = t1.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .create_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .create_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
            .unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();

        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a.as_str(), "aaaaaabbbbbb");
        assert_eq!(a, b);

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn delete_single_block_start() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "bbb").unwrap();
        txt.insert(0, "aaa").unwrap();
        txt.remove_range(0..3).unwrap();

        assert_eq!(txt.len(), 3);
        assert_eq!(txt.to_string(), "bbb");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_single_block_end() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "bbb").unwrap();
        txt.insert(0, "aaa").unwrap();
        txt.remove_range(3..=3).unwrap();

        assert_eq!(txt.to_string(), "aaa");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_multiple_whole_blocks() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "a").unwrap();
        txt.insert(1, "b").unwrap();
        txt.insert(2, "c").unwrap();

        txt.remove_range(1..=1).unwrap();
        assert_eq!(txt.to_string(), "ac");

        txt.remove_range(1..=1).unwrap();
        assert_eq!(txt.to_string(), "a");

        txt.remove_range(0..1).unwrap();
        assert_eq!(txt.to_string(), "");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_slice_of_block() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "abc").unwrap();
        txt.remove_range(1..=1).unwrap();

        assert_eq!(txt.to_string(), "ac");

        tx.commit(None).unwrap();
    }

    #[test]
    fn delete_multiple_blocks_with_slicing() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello ").unwrap();
        txt.insert(6, "beautiful").unwrap();
        txt.insert(15, " world").unwrap();

        txt.remove_range(5..16).unwrap();
        assert_eq!(txt.to_string(), "helloworld");

        tx.commit(None).unwrap();
    }

    #[test]
    fn insert_after_delete() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (mdoc, _) = multi_doc(1);
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();

        txt.insert(0, "hello ").unwrap();
        txt.remove_range(0..5).unwrap();
        txt.insert(1, "world").unwrap();

        assert_eq!(txt.to_string(), " world");

        tx.commit(None).unwrap();
    }

    #[test]
    fn concurrent_insert_delete() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "hello world").unwrap();
        assert_eq!(txt1.to_string(), "hello world");

        drop(txt1);

        let u1 = t1.create_update(&StateVector::default()).unwrap();

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "hello world");

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(5, " beautiful").unwrap();
        txt1.insert(21, "!").unwrap();
        txt1.remove_range(0..5).unwrap();
        assert_eq!(txt1.to_string(), " beautiful world!");

        txt2.remove_range(5..10).unwrap();
        txt2.remove_range(0..1).unwrap();
        txt2.insert(0, "H").unwrap();
        assert_eq!(txt2.to_string(), "Hellod");

        drop(txt1);
        drop(txt2);

        let sv1 = t1.state_vector().unwrap().encode().unwrap();
        let sv2 = t2.state_vector().unwrap().encode().unwrap();
        let u1 = t1
            .create_update(&StateVector::decode(&sv2).unwrap())
            .unwrap();
        let u2 = t2
            .create_update(&StateVector::decode(&sv1).unwrap())
            .unwrap();

        t1.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();

        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();
        let a = txt1.to_string();
        let b = txt2.to_string();

        assert_eq!(a, b);
        assert_eq!(a, "H beautifuld!".to_owned());
    }

    #[test]
    fn basic_format() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        let delta1 = Arc::new(ArcSwapOption::default());
        let delta_clone = delta1.clone();
        let _sub1 = txt1.observe(move |_, e| delta_clone.store(Some(Arc::new(e.delta().into()))));

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();

        let delta2 = Arc::new(ArcSwapOption::default());
        let delta_clone = delta2.clone();
        let _sub2 = txt2.observe(move |_, e| delta_clone.store(Some(Arc::new(e.delta().into()))));

        let a: Attrs = HashMap::from([("bold".into(), Any::Bool(true))]);

        // step 1
        {
            let mut txn = d1.transact_mut();
            txt1.insert_with(0, "abc", a.clone()).unwrap();
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Inserted(
                "abc".into(),
                Some(Box::new(a.clone())),
            )]));

            assert_eq!(txt1.to_string(), "abc".to_string());
            assert_eq!(
                txt1.diff(&d1.transact(), YChange::identity),
                vec![Diff::new("abc".into(), Some(Box::new(a.clone())))]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "abc".to_string());
            assert_eq!(delta2.swap(None), expected);
        }

        // step 2
        {
            let mut txn = d1.transact_mut();
            txt1.remove_range(&mut txn, 0, 1);
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Deleted(1)]));

            assert_eq!(txt1.get_string(&d1.transact()), "bc".to_string());
            assert_eq!(
                txt1.diff(&d1.transact(), YChange::identity),
                vec![Diff::new("bc".into(), Some(Box::new(a.clone())))]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "bc".to_string());
            assert_eq!(delta2.swap(None), expected);
        }

        // step 3
        {
            let mut txn = d1.transact_mut();
            txt1.remove_range(1..2).unwrap();
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Retain(1, None), Delta::Deleted(1)]));

            assert_eq!(txt1.get_string(&d1.transact()), "b".to_string());
            assert_eq!(
                txt1.diff(&d1.transact(), YChange::identity),
                vec![Diff::new("b".into(), Some(Box::new(a.clone())))]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "b".to_string());
            assert_eq!(delta2.swap(None), expected);
        }

        // step 4
        {
            let mut txn = d1.transact_mut();
            txt1.insert_with(&mut txn, 0, "z", a.clone());
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Inserted(
                "z".into(),
                Some(Box::new(a.clone())),
            )]));

            assert_eq!(txt1.get_string(&d1.transact()), "zb".to_string());
            assert_eq!(
                txt1.diff(&mut d1.transact_mut(), YChange::identity),
                vec![Diff::new("zb".into(), Some(Box::new(a.clone())))]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "zb".to_string());
            assert_eq!(delta2.swap(None), expected);
        }

        // step 5
        {
            let mut txn = d1.transact_mut();
            txt1.insert(&mut txn, 0, "y");
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Inserted("y".into(), None)]));

            assert_eq!(txt1.get_string(&d1.transact()), "yzb".to_string());
            assert_eq!(
                txt1.diff(&mut d1.transact_mut(), YChange::identity),
                vec![
                    Diff::new("y".into(), None),
                    Diff::new("zb".into(), Some(Box::new(a.clone())))
                ]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "yzb".to_string());
            assert_eq!(delta2.swap(None), expected);
        }

        // step 6
        {
            let mut txn = d1.transact_mut();
            let b: Attrs = HashMap::from([("bold".into(), Any::Null)]);
            txt1.format(&mut txn, 0, 2, b.clone());
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![
                Delta::Retain(1, None),
                Delta::Retain(1, Some(Box::new(b))),
            ]));

            assert_eq!(txt1.to_string(), "yzb");
            assert_eq!(
                txt1.diff(&mut d1.transact_mut(), YChange::identity),
                vec![
                    Diff::new("yz".into(), None),
                    Diff::new("b".into(), Some(Box::new(a.clone())))
                ]
            );
            assert_eq!(delta1.swap(None), expected);

            let mut txn = d2.transact_mut();
            txn.apply_update(Update::decode_slice(update.as_slice()).unwrap())
                .unwrap();
            drop(txn);

            assert_eq!(txt2.get_string(&d2.transact()), "yzb".to_string());
            assert_eq!(delta2.swap(None), expected);
        }
    }

    #[test]
    fn embed_with_attributes() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        let delta1 = Arc::new(ArcSwapOption::default());
        let delta_clone = delta1.clone();
        let _sub1 = txt1.observe(move |_, e| {
            delta_clone.store(Some(Arc::new(e.delta().into())));
        });

        let a1: Attrs = HashMap::from([("bold".into(), true.into())]);
        let embed = lib0!({
            "image": "imageSrc.png"
        });

        let (update_v1, update_v2) = {
            let mut txn = d1.transact_mut();
            txt1.insert_with(&mut txn, 0, "ab", a1.clone());

            let a2: Attrs = HashMap::from([("width".into(), Any::Number(100.0))]);

            txt1.insert_embed_with_attributes(&mut txn, 1, embed.clone(), a2.clone());
            drop(txn);

            let a1 = Some(Box::new(a1.clone()));
            let a2 = Some(Box::new(a2.clone()));

            let expected = Some(Arc::new(vec![
                Delta::Inserted("a".into(), a1.clone()),
                Delta::Inserted(embed.clone().into(), a2.clone()),
                Delta::Inserted("b".into(), a1.clone()),
            ]));
            assert_eq!(delta1.swap(None), expected);

            let expected = vec![
                Diff::new("a".into(), a1.clone()),
                Diff::new(embed.clone().into(), a2),
                Diff::new("b".into(), a1.clone()),
            ];
            let mut txn = d1.transact_mut();
            assert_eq!(txt1.diff(&mut txn, YChange::identity), expected);

            let update_v1 = txn.create_update(&StateVector::default()).unwrap();
            let update_v2 = txn.create_update(&StateVector::default()).unwrap();
            (update_v1, update_v2)
        };

        let a1 = Some(Box::new(a1));
        let a2 = Some(Box::new(HashMap::from([(
            "width".into(),
            Any::Number(100.0),
        )])));

        let expected = vec![
            Diff::new("a".into(), a1.clone()),
            Diff::new(embed.into(), a2),
            Diff::new("b".into(), a1.clone()),
        ];

        let mut d2 = Doc::new();
        let txt2 = d2.get_or_insert_text("text");
        {
            let txn = &mut d2.transact_mut();
            let update = Update::decode_slice(&update_v1).unwrap();
            txn.apply_update(update).unwrap();
            assert_eq!(txt2.diff(txn, YChange::identity), expected);
        }

        let mut d3 = Doc::new();
        let txt3 = d3.get_or_insert_text("text");
        {
            let txn = &mut d3.transact_mut();
            let update = Update::decode_v2(&update_v2).unwrap();
            txn.apply_update(update).unwrap();
            let actual = txt3.diff(txn, YChange::identity);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn issue_101() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        let delta = Arc::new(ArcSwapOption::default());
        let delta_copy = delta.clone();

        let attrs: Attrs = HashMap::from([("bold".into(), true.into())]);

        txt1.insert(&mut d1.transact_mut(), 0, "abcd");

        let _sub = txt1.observe(move |_, e| {
            delta_copy.store(Some(e.delta().to_vec().into()));
        });
        txt1.format(&mut d1.transact_mut(), 1, 2, attrs.clone());

        let expected = Arc::new(vec![
            Delta::Retain(1, None),
            Delta::Retain(2, Some(Box::new(attrs))),
        ]);
        let actual = delta.load_full();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn yrs_delete() {
        let mut doc = Doc::with_options(Options {
            offset_kind: OffsetKind::Utf16,
            ..Default::default()
        });

        let text1 = r#"
		Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Eleifend mi in nulla posuere sollicitudin. Lorem mollis aliquam ut porttitor. Enim ut sem viverra aliquet eget sit amet. Sed turpis tincidunt id aliquet risus feugiat in ante metus. Accumsan lacus vel facilisis volutpat. Non consectetur a erat nam at lectus urna. Enim diam vulputate ut pharetra sit amet. In dictum non consectetur a erat. Bibendum at varius vel pharetra vel turpis nunc eget lorem. Blandit cursus risus at ultrices. Sed lectus vestibulum mattis ullamcorper velit sed ullamcorper. Sagittis nisl rhoncus mattis rhoncus.

		Sed vulputate odio ut enim. Erat pellentesque adipiscing commodo elit at imperdiet dui. Ultricies tristique nulla aliquet enim tortor at auctor urna nunc. Tincidunt eget nullam non nisi est sit amet. Sed adipiscing diam donec adipiscing tristique risus nec. Risus commodo viverra maecenas accumsan lacus vel facilisis volutpat est. Donec enim diam vulputate ut pharetra sit amet aliquam id. Netus et malesuada fames ac turpis egestas sed tempus urna. Augue mauris augue neque gravida. Tellus orci ac auctor augue mauris augue. Ante metus dictum at tempor. Feugiat in ante metus dictum at. Vitae elementum curabitur vitae nunc sed velit dignissim. Non arcu risus quis varius quam quisque id diam vel. Fermentum leo vel orci porta non. Donec adipiscing tristique risus nec feugiat in fermentum posuere. Duis convallis convallis tellus id interdum velit laoreet id. Vel eros donec ac odio tempor orci dapibus ultrices in. At varius vel pharetra vel turpis nunc eget lorem. Blandit aliquam etiam erat velit scelerisque in.
		"#;

        let text2 = r#"test"#;

        {
            let text = doc.get_or_insert_text("content");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, text1);
            txn.commit();
        }

        {
            let text = doc.get_or_insert_text("content");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 100, text2);
            txn.commit();
        }

        {
            let text = doc.get_or_insert_text("content");
            let mut txn = doc.transact_mut();

            let c1 = text1.chars().count();
            let c2 = text2.chars().count();
            let count = c1 as u32 + c2 as u32;

            let _observer =
                text.observe(move |_, e| assert_eq!(e.delta()[0], Delta::Deleted(count)));

            text.remove_range(&mut txn, 0, count);
            txn.commit();
        }

        {
            let text = doc.get_or_insert_text("content");
            assert_eq!(text.get_string(&doc.transact()), "");
        }
    }

    #[test]
    fn text_diff_adjacent() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        let attrs1 = [("a".to_string(), Value::from("a"))];
        txt.insert_with(0, "abc", attrs1.clone()).unwrap();
        let attrs2 = [
            ("a".to_string(), Value::from("a")),
            ("b".into(), "b".into()),
        ];
        txt.insert_with(3, "def", attrs2.clone()).unwrap();

        let diff = txt.diff(&mut txn, YChange::identity);
        let expected = vec![
            Diff::new("abc".into(), Some(Box::new(attrs1))),
            Diff::new("def".into(), Some(Box::new(attrs2))),
        ];
        assert_eq!(diff, expected);

        txn.commit(None).unwrap();
    }

    #[test]
    fn text_remove_4_byte_range() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "😭😊").unwrap();

        sync([&mut t1, &mut t2]);

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.remove_range(0.."😭".len()).unwrap();
        assert_eq!(txt1.to_string(), "😊");

        sync([&mut t1, &mut t2]);
        let mut txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "😊");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn text_remove_3_byte_range() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(0, "⏰⏳").unwrap();

        sync([&mut t1, &mut t2]);

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.remove_range(0.."⏰".len()).unwrap();
        assert_eq!(txt1.to_string(), "⏳");

        sync([&mut t1, &mut t2]);
        let txt2 = txt.mount(&t1).unwrap();
        assert_eq!(txt2.to_string(), "⏳");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn delete_4_byte_character_from_middle() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "😊😭").unwrap();
        // uncomment the following line will pass the test
        // txt.format(&mut txn, 0, "😊".len() as u32, HashMap::new());
        let start = "😊".len();
        let end = start + "😭".len();
        txt.remove_range(start..end).unwrap();

        assert_eq!(txt.to_string(), "😊");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_1() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "⏰⏳").unwrap();
        // uncomment the following line will pass the test
        // txt.format(&mut txn, 0, "⏰".len() as u32, HashMap::new());
        let start = "⏰".len();
        let end = start + "⏳".len();
        txt.remove_range(start..end).unwrap();

        assert_eq!(txt.to_string(), "⏰");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_2() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "👯🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨").unwrap();

        let start = "👯".len();
        let end = start + "🙇‍♀️🙇‍♀️".len();
        txt.format(start, end, []).unwrap();
        let start = "👯🙇‍♀️🙇‍♀️".len();
        let end = start + "⏰".len();
        txt.remove_range(start..end).unwrap(); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(txt.to_string(), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_3_byte_character_from_middle_after_insert_and_format() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨").unwrap();
        txt.insert(0, "👯").unwrap();
        let start = "👯".len();
        let end = start + "🙇‍♀️🙇‍♀️".len();
        txt.format(start, end, []).unwrap();

        // will delete ⏰ and 👩‍❤️‍💋‍👨
        let start = "👯🙇‍♀️🙇‍♀️".len();
        let end = start + "⏰".len();
        txt.remove_range(start..end).unwrap(); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(&txt.to_string(), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn delete_multi_byte_character_from_middle_after_insert_and_format() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        txt.insert(0, "❤️❤️🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨👩‍❤️‍💋‍👨").unwrap();
        txt.insert(0, "👯").unwrap();
        let start = "👯".len();
        let end = start + "❤️❤️🙇‍♀️🙇‍♀️⏰".len();
        txt.format(start, end, Attrs::new()).unwrap();
        txt.insert("👯❤️❤️🙇‍♀️🙇‍♀️⏰".len(), "⏰").unwrap();
        let start = "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰".len();
        let end = start + "👩‍❤️‍💋‍👨".len();
        txt.format(start, end, Attrs::new()).unwrap();
        let start = "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👩".len();
        let end = start + "👩‍❤️‍💋‍👨".len();
        txt.remove_range(start..end).unwrap();
        assert_eq!(txt.to_string(), "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👨");

        txn.commit(None).unwrap();
    }

    #[test]
    fn insert_string_with_no_attribute() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        let attrs = Attrs::from([("a".into(), "a".into())]);
        txt.insert_with(0, "ac", attrs.clone()).unwrap();
        txt.insert_with(1, "b", Attrs::new()).unwrap();

        let expect: Vec<(Value, Option<Box<Attrs>>)> = vec![
            ("a".into(), Some(Box::new(attrs.clone()))),
            ("b".into(), None),
            ("c".into(), Some(Box::new(attrs.clone()))),
        ];

        let actual: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(actual, expect);
        txn.commit(None).unwrap();
    }

    #[test]
    fn insert_empty_string_with_attributes() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let attrs = [("a".to_string(), Value::from("a"))];
        txt.insert(0, "abc").unwrap();
        txt.insert(1, "").unwrap(); // nothing changes
        txt.insert_with(1, "", attrs).unwrap(); // nothing changes

        assert_eq!(txt.to_string(), "abc");

        let bin = txn.create_update(&StateVector::default()).unwrap();

        txn.commit(None).unwrap();

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();

        txn.apply_update(&mut DecoderV1::from_slice(&bin)).unwrap();

        let txt = root.mount(&txn).unwrap();
        assert_eq!(txt.to_string(), "abc");

        txn.commit(None).unwrap();
    }

    #[test]
    fn snapshots() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();

        text.insert(0, "hello").unwrap();
        let prev = doc.transact_mut().snapshot();
        text.insert(&mut doc.transact_mut(), 5, " world");
        let next = doc.transact_mut().snapshot();
        let diff = text.diff_range(Some(&next), Some(&prev), YChange::identity);

        assert_eq!(
            diff,
            vec![
                Diff::new("hello".into(), None),
                Diff::with_change(
                    " world".into(),
                    None,
                    Some(YChange::new(ChangeKind::Added, ID::new(1, 5)))
                )
            ]
        );
        txn.commit(None).unwrap();
    }

    #[test]
    fn diff_with_embedded_items() {
        let txt: Unmounted<Text> = Unmounted::root("article");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();

        let bold = [("b", true)];
        let italic = [("i", true)];

        text.insert_with(0, "hello world", italic.clone()).unwrap(); // "<i>hello world</i>"
        text.format(6, 11, bold.clone()).unwrap(); // "<i>hello <b>world</b></i>"
        let image = vec![0, 0, 0, 0];
        text.insert_embed(5, image.clone()).unwrap(); // insert binary after "hello"
        let array = text.insert_embed(5, ListPrelim::default()).unwrap(); // insert array ref after "hello"

        let italic_and_bold = Attrs::from([("b".into(), true.into()), ("i".into(), true.into())]);
        let chunks = text.chunks();
        assert_eq!(
            chunks,
            vec![
                Diff::new("hello".into(), Some(Box::new(italic.clone()))),
                Diff::new(Out::Array(array), Some(Box::new(italic.clone()))),
                Diff::new(image.into(), Some(Box::new(italic.clone()))),
                Diff::new(" ".into(), Some(Box::new(italic))),
                Diff::new("world".into(), Some(Box::new(italic_and_bold))),
            ]
        );
    }

    #[test]
    fn multiline_format() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let bold = [("bold", true)];
        txt.insert(0, "Test\nMulti-line\nFormatting").unwrap();
        txt.apply_delta([
            Delta::Retain(4, bold.clone()),
            Delta::retain(1), // newline character
            Delta::Retain(10, bold.clone()),
            Delta::retain(1), // newline character
            Delta::Retain(10, bold.clone()),
        ])
        .unwrap();
        let delta: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(
            delta,
            vec![
                ("Test".into(), bold.clone()),
                ("\n".into(), None),
                ("Multi-line".into(), bold.clone()),
                ("\n".into(), None),
                ("Formatting".into(), bold),
            ]
        );

        txn.commit(None).unwrap();
    }

    #[test]
    fn delta_with_embeds() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let linebreak = lib0!({
            "linebreak": "s"
        });
        txt.apply_delta([Delta::insert(linebreak.clone())]).unwrap();
        let delta: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(delta, vec![(linebreak.into(), None)]);
    }

    #[test]
    fn delta_with_shared_ref() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut txt1 = root.mount_mut(&mut t1).unwrap();

        txt1.apply_delta([Delta::insert(MapPrelim::from_iter([("key", "val")]))]);
        let delta = txt1.diff(&txn1, YChange::identity);
        let d: MapRef = delta[0].insert.clone().cast(&txn1).unwrap();
        assert_eq!(d.get::<Out>(&txn1, "key").unwrap(), Out::Any("val".into()));

        let triggered = Arc::new(AtomicBool::new(false));
        let _sub = {
            let triggered = triggered.clone();
            txt1.observe(move |txn, e| {
                let delta = e.delta().to_vec();
                let d: MapRef = match &delta[0] {
                    Delta::Inserted(insert, _) => insert.clone().cast(txn).unwrap(),
                    _ => unreachable!("unexpected delta"),
                };
                assert_eq!(d.get::<Out>(txn, "key").unwrap(), Out::Any("val".into()));
                triggered.store(true, Ordering::Relaxed);
            })
        };

        let update = Update::decode_slice(&txn1.encode_update_v1()).unwrap();
        txn2.apply_update(update).unwrap();
        drop(txn1);
        drop(txn2);

        assert!(triggered.load(Ordering::Relaxed), "fired event");

        let txn = d2.transact();
        let delta = txt2.chunks();
        assert_eq!(delta.len(), 1);
        let d: MapRef = delta[0].insert.clone().cast(&txn).unwrap();
        assert_eq!(d.get::<Out>(&txn, "key").unwrap(), Out::Any("val".into()));

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn delta_snapshots() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        txt.apply_delta(&mut txn, [Delta::insert("abcd")]);
        let snapshot1 = txn.snapshot(); // 'abcd'
        txt.apply_delta([Delta::retain(1), Delta::insert("x"), Delta::delete(1)])
            .unwrap();
        let snapshot2 = txn.snapshot(); // 'axcd'
        txt.apply_delta([
            Delta::retain(2),   // ax^cd
            Delta::delete(1),   // ax^d
            Delta::insert("x"), // axx^d
            Delta::delete(1),   // axx^
        ])
        .unwrap();
        let state1 = txt.diff_range(&mut txn, Some(&snapshot1), None, YChange::identity);
        assert_eq!(state1, vec![Diff::new("abcd".into(), None)]);
        let state2 = txt.diff_range(&mut txn, Some(&snapshot2), None, YChange::identity);
        assert_eq!(state2, vec![Diff::new("axcd".into(), None)]);
        let state2_diff = txt.diff_range(
            &mut txn,
            Some(&snapshot2),
            Some(&snapshot1),
            YChange::identity,
        );
        assert_eq!(
            state2_diff,
            vec![
                Diff {
                    insert: "a".into(),
                    attributes: None,
                    ychange: None
                },
                Diff {
                    insert: "x".into(),
                    attributes: None,
                    ychange: Some(YChange {
                        kind: ChangeKind::Added,
                        id: ID {
                            client: 1,
                            clock: 4
                        }
                    })
                },
                Diff {
                    insert: "b".into(),
                    attributes: None,
                    ychange: Some(YChange {
                        kind: ChangeKind::Removed,
                        id: ID {
                            client: 1,
                            clock: 1
                        }
                    })
                },
                Diff {
                    insert: "cd".into(),
                    attributes: None,
                    ychange: None
                }
            ]
        );
    }

    #[test]
    fn snapshot_delete_after() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        txt.apply_delta([Delta::insert("abcd")]).unwrap();
        let snapshot1 = txn.snapshot();
        txt.apply_delta([Delta::retain(4), Delta::insert("e")])
            .unwrap();
        let state1 = txt.diff_range(&mut txn, Some(&snapshot1), None, YChange::identity);
        assert_eq!(state1, vec![Diff::new("abcd".into(), None)]);
    }

    #[test]
    fn empty_delta_chunks() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let delta = vec![
            Delta::insert("a"),
            Delta::Inserted(
                "".into(),
                Some(Box::new(Attrs::from([("bold".into(), true.into())]))),
            ),
            Delta::insert("b"),
        ];

        txt.apply_delta(delta).unwrap();
        assert_eq!(txt.to_string(), "ab");

        let bin = txn.create_update(&StateVector::default()).unwrap();

        txn.commit(None).unwrap();

        let (mdoc, _) = multi_doc(2);
        let mut txn = mdoc.transact_mut("test").unwrap();

        txn.apply_update(&mut DecoderV1::from_slice(&bin)).unwrap();

        let txt = root.mount(&txn).unwrap();
        assert_eq!(txt.to_string(), "ab");
    }
}
