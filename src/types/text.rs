use crate::block::ID;
use crate::content::BlockContent;
use crate::lib0::Value;
use crate::node::NodeType;
use crate::prelim::Prelim;
use crate::state_vector::Snapshot;
use crate::store::lmdb::BlockStore;
use crate::types::Capability;
use crate::{In, Mounted, Out, Transaction};
use serde::{Deserialize, Serialize};
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

    /// Returns an iterator over uncommitted changes (deltas) made to this text type
    /// within its current transaction scope.
    pub fn uncommitted(&self) -> impl Iterator<Item = crate::Result<Delta>> {
        yield Ok(Chunk::new(""));
    }

    /// Returns an iterator over all text and embedded chunks grouped by their applied attributes.
    pub fn chunks(&self) -> impl Iterator<Item = crate::Result<Chunk>> {
        todo!()
    }

    /// Returns an iterator over all text and embedded chunks grouped by their applied attributes,
    /// scoped between two provided snapshots.
    pub fn chunks_between(
        &self,
        from: Option<&Snapshot>,
        to: Option<&Snapshot>,
    ) -> impl Iterator<Item = crate::Result<Chunk>> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    pub insert: Out,
    pub attributes: Option<Box<Attrs>>,
    pub id: Option<ID>,
}

impl Chunk {
    pub fn new<O: Into<Out>>(insert: O) -> Self {
        Self {
            insert: insert.into(),
            attributes: None,
            id: None,
        }
    }

    pub fn with_attrs(self, attrs: Attrs) -> Self {
        Self {
            id: self.id,
            insert: self.insert,
            attributes: Some(Box::new(attrs)),
        }
    }

    pub fn with_id(mut self, id: ID) -> Self {
        self.id = Some(id);
        self
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

    pub fn insert_embed<V>(&mut self, index: usize, value: V) -> crate::Result<V::Return>
    where
        V: Prelim,
    {
        todo!()
    }

    pub fn insert_embed_with<S, A, V1, V2>(
        &mut self,
        index: usize,
        value: V1,
        attrs: A,
    ) -> crate::Result<()>
    where
        S: AsRef<str>,
        V1: Serialize,
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
    use crate::block::ID;
    use crate::lib0::Value;
    use crate::read::{Decode, DecoderV1};
    use crate::test_util::{multi_doc, sync};
    use crate::types::text::{Attrs, Chunk, Delta};
    use crate::write::Encode;
    use crate::{lib0, ListPrelim, Map, MapPrelim, MapRef, Out, StateVector, Text, Unmounted};

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
            .diff_update(&StateVector::decode(&d2_sv).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv).unwrap())
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
            .diff_update(&StateVector::decode(&d2_sv).unwrap())
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
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
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
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
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
            .diff_update(&StateVector::decode(&d2_sv.as_slice()).unwrap())
            .unwrap();
        let u2 = t2
            .diff_update(&StateVector::decode(&d1_sv.as_slice()).unwrap())
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

        let u1 = t1.diff_update(&StateVector::default()).unwrap();

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
        let u1 = t1.diff_update(&StateVector::decode(&sv2).unwrap()).unwrap();
        let u2 = t2.diff_update(&StateVector::decode(&sv1).unwrap()).unwrap();

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
        let (d2, _) = multi_doc(2);

        let a = Attrs::from([("bold".into(), Value::Bool(true))]);

        // step 1
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert_with(0, "abc", a.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Inserted("abc".into(), Some(Box::new(a.clone())))];

            assert_eq!(txt1.to_string(), "abc".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("abc").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();

            assert_eq!(txt2.to_string(), "abc");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 2
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.remove_range(0..1).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();
            let expected = vec![Delta::Deleted(1)];

            assert_eq!(txt1.to_string(), "bc");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("bc").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();

            assert_eq!(txt2.to_string(), "bc");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 3
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.remove_range(1..2).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Retain(1, None), Delta::Deleted(1)];

            assert_eq!(txt1.to_string(), "b");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("b").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);

            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();

            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "b");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 4
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert_with(0, "z", a.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Inserted(Out::from("z"), Some(Box::new(a.clone())))];

            assert_eq!(txt1.to_string(), "zb".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("zb").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "zb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 5
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            txt1.insert(0, "y").unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Inserted("y".into(), None)];

            assert_eq!(txt1.to_string(), "yzb".to_string());
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("y"), Chunk::new("zb").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();

            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "yzb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }

        // step 6
        {
            let mut txn = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut txn).unwrap();
            let b = Attrs::from([("bold".into(), Value::Null)]);
            txt1.format(0, 2, b.clone()).unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![Delta::Retain(1, None), Delta::Retain(1, Some(Box::new(b)))];

            assert_eq!(txt1.to_string(), "yzb");
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                vec![Chunk::new("yz"), Chunk::new("b").with_attrs(a.clone())]
            );
            assert_eq!(uncommitted, expected);
            let update = txn.incremental_update().unwrap();
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&update))
                .unwrap();
            let mut txt2 = txt.mount_mut(&mut txn).unwrap();
            let uncommitted: Vec<_> = txt2.uncommitted().map(Result::unwrap).collect();
            assert_eq!(txt2.to_string(), "yzb");
            assert_eq!(uncommitted, expected);
            txn.commit(None).unwrap();
        }
    }

    #[test]
    fn embed_with_attributes() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);

        let a1 = Attrs::from([("bold".into(), true.into())]);
        let embed = lib0!({
            "image": "imageSrc.png"
        });

        let update_v1 = {
            let mut t1 = d1.transact_mut("test").unwrap();
            let mut txt1 = txt.mount_mut(&mut t1).unwrap();

            txt1.insert_with(0, "ab", a1.clone()).unwrap();

            let a2 = Attrs::from([("width".into(), Value::from(100.0))]);

            txt1.insert_embed_with(1, embed.clone(), a2.clone())
                .unwrap();
            let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();

            let expected = vec![
                Delta::Inserted("a".into(), Some(Box::new(a1.clone()))),
                Delta::Inserted(embed.clone().into(), Some(Box::new(a2.clone()))),
                Delta::Inserted("b".into(), Some(Box::new(a1.clone()))),
            ];
            assert_eq!(uncommitted, expected);
            t1.commit(None).unwrap();

            let expected = vec![
                Chunk::new("a").with_attrs(a1.clone()),
                Chunk::new(embed.clone()).with_attrs(a2),
                Chunk::new("b").with_attrs(a1.clone()),
            ];
            let t1 = d1.transact_mut("test").unwrap();
            let txt1 = txt.mount(&t1).unwrap();
            assert_eq!(
                txt1.chunks().map(Result::unwrap).collect::<Vec<_>>(),
                expected
            );

            let update_v1 = t1.diff_update(&StateVector::default()).unwrap();
            update_v1
        };

        let a2 = Attrs::from([("width".into(), Value::from(100.0))]);

        let expected = vec![
            Chunk::new("a").with_attrs(a1.clone()),
            Chunk::new(embed).with_attrs(a2),
            Chunk::new("b").with_attrs(a1),
        ];

        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update_v1))
            .unwrap();
        let txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(
            txt2.chunks().map(Result::unwrap).collect::<Vec<_>>(),
            expected
        );
        t2.commit(None).unwrap();
    }

    #[test]
    fn issue_101() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        let attrs = Attrs::from([("bold".into(), true.into())]);

        txt1.insert(0, "abcd").unwrap();
        t1.commit(None).unwrap();

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.format(1, 2, attrs.clone()).unwrap();

        let uncommitted: Vec<_> = txt1.uncommitted().map(Result::unwrap).collect();
        let expected = vec![
            Delta::Retain(1, None),
            Delta::Retain(2, Some(Box::new(attrs))),
        ];
        assert_eq!(uncommitted, expected);
        t1.commit(None).unwrap();
    }

    #[test]
    fn text_diff_adjacent() {
        let txt: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut txn).unwrap();

        let attrs1 = Attrs::from_iter([("a".to_string(), Value::from("a"))]);
        txt.insert_with(0, "abc", attrs1.clone()).unwrap();
        let attrs2 = Attrs::from_iter([
            ("a".to_string(), Value::from("a")),
            ("b".into(), "b".into()),
        ]);
        txt.insert_with(3, "def", attrs2.clone()).unwrap();

        let diff: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        let expected = vec![
            Chunk::new("abc").with_attrs(attrs1),
            Chunk::new("def").with_attrs(attrs2),
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
        txt.format(start, end, Attrs::default()).unwrap();
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
        txt.format(start, end, Attrs::default()).unwrap();

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

        let expect = vec![
            Chunk::new("a").with_attrs(attrs.clone()),
            Chunk::new("b"),
            Chunk::new("c").with_attrs(attrs.clone()),
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

        let bin = txn.diff_update(&StateVector::default()).unwrap();

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
        let prev = txn.snapshot().unwrap();
        let mut text = txt.mount_mut(&mut txn).unwrap();
        text.insert(5, " world").unwrap();
        let next = txn.snapshot().unwrap();
        let text = txt.mount(&txn).unwrap();
        let diff: Vec<_> = text
            .chunks_between(Some(&next), Some(&prev))
            .map(Result::unwrap)
            .collect();

        assert_eq!(
            diff,
            vec![
                Chunk::new("hello"),
                Chunk::new(" world").with_id(ID::new(1.into(), 5.into()))
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

        let bold = Attrs::from_iter([("b".into(), true.into())]);
        let italic = Attrs::from_iter([("i".into(), true.into())]);

        text.insert_with(0, "hello world", italic.clone()).unwrap(); // "<i>hello world</i>"
        text.format(6, 11, bold.clone()).unwrap(); // "<i>hello <b>world</b></i>"
        let image = vec![0, 0, 0, 0];
        text.insert_embed(5, Value::from(image.clone())).unwrap(); // insert binary after "hello"
        let array = text.insert_embed(5, ListPrelim::default()).unwrap(); // insert array ref after "hello"

        let italic_and_bold = Attrs::from([("b".into(), true.into()), ("i".into(), true.into())]);
        let chunks: Vec<_> = text.chunks().map(Result::unwrap).collect();
        assert_eq!(
            chunks,
            vec![
                Chunk::new("hello").with_attrs(italic.clone()),
                Chunk::new(array).with_attrs(italic.clone()),
                Chunk::new(image).with_attrs(italic.clone()),
                Chunk::new(" ").with_attrs(italic),
                Chunk::new("world").with_attrs(italic_and_bold),
            ]
        );
    }

    #[test]
    fn multiline_format() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();

        let bold = Attrs::from_iter([("bold".into(), true.into())]);
        txt.insert(0, "Test\nMulti-line\nFormatting").unwrap();
        txt.apply_delta([
            Delta::Retain(4, Some(Box::new(bold.clone()))),
            Delta::retain(1), // newline character
            Delta::Retain(10, Some(Box::new(bold.clone()))),
            Delta::retain(1), // newline character
            Delta::Retain(10, Some(Box::new(bold.clone()))),
        ])
        .unwrap();
        let delta: Vec<_> = txt.chunks().map(Result::unwrap).collect();
        assert_eq!(
            delta,
            vec![
                Chunk::new("Test").with_attrs(bold.clone()),
                Chunk::new("\n"),
                Chunk::new("Multi-line").with_attrs(bold.clone()),
                Chunk::new("\n"),
                Chunk::new("Formatting").with_attrs(bold),
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
        assert_eq!(delta, vec![Chunk::new(linebreak)]);
    }

    #[test]
    fn delta_with_shared_ref() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (d1, _) = multi_doc(1);
        let (d2, _) = multi_doc(2);

        let mut t1 = d1.transact_mut("test").unwrap();
        let mut t2 = d2.transact_mut("test").unwrap();
        let mut txt1 = root.mount_mut(&mut t1).unwrap();

        txt1.apply_delta([Delta::insert(MapPrelim::from_iter([(
            "key".into(),
            "val".into(),
        )]))])
        .unwrap();
        let delta: Vec<_> = txt1.chunks().map(Result::unwrap).collect();
        let node = delta[0].insert.as_node().cloned().unwrap();
        let map: Unmounted<Map> = Unmounted::nested(node);
        let map = map.mount(&t1).unwrap();
        assert_eq!(map.get("key").unwrap(), Value::from("val"));

        let update = t1.incremental_update().unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&update))
            .unwrap();
        t1.commit(None).unwrap();
        t2.commit(None).unwrap();

        let t2 = d2.transact_mut("test").unwrap();
        let txt2 = root.mount(&t2).unwrap();
        let delta: Vec<_> = txt2.chunks().map(Result::unwrap).collect();
        assert_eq!(delta.len(), 1);
        let node = delta[0].insert.clone().as_node().cloned().unwrap();
        let map: Unmounted<Map> = Unmounted::nested(node);
        let map = map.mount(&t2).unwrap();
        assert_eq!(map.get("key").unwrap(), Value::from("val"));
    }

    #[test]
    fn delta_snapshots() {
        let root: Unmounted<Text> = Unmounted::root("text");

        let (mdoc, _) = multi_doc(1);
        let mut txn = mdoc.transact_mut("test").unwrap();

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::insert("abcd")]).unwrap();
        let snapshot1 = txn.snapshot().unwrap(); // 'abcd'

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::retain(1), Delta::insert("x"), Delta::delete(1)])
            .unwrap();
        let snapshot2 = txn.snapshot().unwrap(); // 'axcd'

        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([
            Delta::retain(2),   // ax^cd
            Delta::delete(1),   // ax^d
            Delta::insert("x"), // axx^d
            Delta::delete(1),   // axx^
        ])
        .unwrap();
        let state1: Vec<_> = txt
            .chunks_between(Some(&snapshot1), None)
            .map(Result::unwrap)
            .collect();
        assert_eq!(state1, vec![Chunk::new("abcd")]);
        let state2: Vec<_> = txt
            .chunks_between(Some(&snapshot2), None)
            .map(Result::unwrap)
            .collect();
        assert_eq!(state2, vec![Chunk::new("axcd")]);
        let state2_diff: Vec<_> = txt
            .chunks_between(Some(&snapshot2), Some(&snapshot1))
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            state2_diff,
            vec![
                Chunk::new("a"),
                Chunk::new("x").with_id(ID::new(1.into(), 4.into())),
                Chunk::new("bcd").with_id(ID::new(1.into(), 1.into())),
                Chunk::new("cd"),
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
        let snapshot1 = txn.snapshot().unwrap();
        let mut txt = root.mount_mut(&mut txn).unwrap();
        txt.apply_delta([Delta::retain(4), Delta::insert("e")])
            .unwrap();
        let state1: Vec<_> = txt
            .chunks_between(Some(&snapshot1), None)
            .map(Result::unwrap)
            .collect();
        assert_eq!(state1, vec![Chunk::new("abcd")]);
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

        let bin = txn.diff_update(&StateVector::default()).unwrap();

        txn.commit(None).unwrap();

        let (mdoc, _) = multi_doc(2);
        let mut txn = mdoc.transact_mut("test").unwrap();

        txn.apply_update(&mut DecoderV1::from_slice(&bin)).unwrap();

        let txt = root.mount(&txn).unwrap();
        assert_eq!(txt.to_string(), "ab");
    }
}
