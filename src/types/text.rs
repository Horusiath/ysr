use crate::content::BlockContent;
use crate::node::NodeType;
use crate::store::lmdb::BlockStore;
use crate::types::Capability;
use crate::{Mounted, Transaction};
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
}

impl<'tx, 'db> Deref for TextRef<&'tx mut Transaction<'db>> {
    type Target = TextRef<&'tx Transaction<'db>>;

    fn deref(&self) -> &Self::Target {
        // Assuming that the mutable reference can be dereferenced to an immutable reference
        // This is a common pattern in Rust to allow shared access to the same data
        unsafe { &*(self as *const _ as *const TextRef<&'tx Transaction<'db>>) }
    }
}

#[cfg(test)]
mod test {
    use crate::read::{Decode, DecoderV1};
    use crate::test_util::multi_doc;
    use crate::write::Encode;
    use crate::{lib0, StateVector, Text, Unmounted};

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
            txt1.insert_with_attributes(&mut txn, 0, "abc", a.clone());
            let update = txn.encode_update_v1();
            drop(txn);

            let expected = Some(Arc::new(vec![Delta::Inserted(
                "abc".into(),
                Some(Box::new(a.clone())),
            )]));

            assert_eq!(txt1.get_string(&d1.transact()), "abc".to_string());
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
            txt1.remove_range(&mut txn, 1, 1);
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
            txt1.insert_with_attributes(&mut txn, 0, "z", a.clone());
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

            assert_eq!(txt1.get_string(&d1.transact()), "yzb".to_string());
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
        let mut d1 = Doc::with_client_id(1);
        let txt1 = d1.get_or_insert_text("text");

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
            txt1.insert_with_attributes(&mut txn, 0, "ab", a1.clone());

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

            let update_v1 = txn.encode_state_as_update_v1(&StateVector::default());
            let update_v2 = txn.encode_state_as_update_v2(&StateVector::default());
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
        let mut d1 = Doc::with_client_id(1);
        let txt1 = d1.get_or_insert_text("text");
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
        let mut doc = Doc::with_client_id(1);
        let txt = doc.get_or_insert_text("text");
        let mut txn = doc.transact_mut();
        let attrs1 = Attrs::from([("a".into(), "a".into())]);
        txt.insert_with_attributes(&mut txn, 0, "abc", attrs1.clone());
        let attrs2 = Attrs::from([("a".into(), "a".into()), ("b".into(), "b".into())]);
        txt.insert_with_attributes(&mut txn, 3, "def", attrs2.clone());

        let diff = txt.diff(&mut txn, YChange::identity);
        let expected = vec![
            Diff::new("abc".into(), Some(Box::new(attrs1))),
            Diff::new("def".into(), Some(Box::new(attrs2))),
        ];
        assert_eq!(diff, expected);
    }

    #[test]
    fn text_remove_4_byte_range() {
        let mut d1 = Doc::new();
        let txt = d1.get_or_insert_text("test");

        txt.insert(&mut d1.transact_mut(), 0, "😭😊");

        let mut d2 = Doc::new();
        exchange_updates([&mut d1, &mut d2]);

        txt.remove_range(&mut d1.transact_mut(), 0, "😭".len() as u32);
        assert_eq!(txt.get_string(&d1.transact()).as_str(), "😊");

        exchange_updates([&mut d1, &mut d2]);
        let txt = d2.get_or_insert_text("test");
        assert_eq!(txt.get_string(&d2.transact()).as_str(), "😊");
    }

    #[test]
    fn text_remove_3_byte_range() {
        let mut d1 = Doc::new();
        let txt = d1.get_or_insert_text("test");

        txt.insert(&mut d1.transact_mut(), 0, "⏰⏳");

        let mut d2 = Doc::new();
        exchange_updates([&mut d1, &mut d2]);

        txt.remove_range(&mut d1.transact_mut(), 0, "⏰".len() as u32);
        assert_eq!(txt.get_string(&d1.transact()).as_str(), "⏳");

        exchange_updates([&mut d1, &mut d2]);
        let txt = d2.get_or_insert_text("test");
        assert_eq!(txt.get_string(&d2.transact()).as_str(), "⏳");
    }
    #[test]
    fn delete_4_byte_character_from_middle() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        txt.insert(&mut txn, 0, "😊😭");
        // uncomment the following line will pass the test
        // txt.format(&mut txn, 0, "😊".len() as u32, HashMap::new());
        txt.remove_range(&mut txn, "😊".len() as u32, "😭".len() as u32);

        assert_eq!(txt.get_string(&txn).as_str(), "😊");
    }

    #[test]
    fn delete_3_byte_character_from_middle_1() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        txt.insert(&mut txn, 0, "⏰⏳");
        // uncomment the following line will pass the test
        // txt.format(&mut txn, 0, "⏰".len() as u32, HashMap::new());
        txt.remove_range(&mut txn, "⏰".len() as u32, "⏳".len() as u32);

        assert_eq!(txt.get_string(&txn).as_str(), "⏰");
    }

    #[test]
    fn delete_3_byte_character_from_middle_2() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        txt.insert(&mut txn, 0, "👯🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨");

        txt.format(
            &mut txn,
            "👯".len() as u32,
            "🙇‍♀️🙇‍♀️".len() as u32,
            HashMap::new(),
        );
        txt.remove_range(&mut txn, "👯🙇‍♀️🙇‍♀️".len() as u32, "⏰".len() as u32); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(txt.get_string(&txn).as_str(), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");
    }

    #[test]
    fn delete_3_byte_character_from_middle_after_insert_and_format() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        txt.insert(&mut txn, 0, "🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨");
        txt.insert(&mut txn, 0, "👯");
        txt.format(
            &mut txn,
            "👯".len() as u32,
            "🙇‍♀️🙇‍♀️".len() as u32,
            HashMap::new(),
        );

        // will delete ⏰ and 👩‍❤️‍💋‍👨
        txt.remove_range(&mut txn, "👯🙇‍♀️🙇‍♀️".len() as u32, "⏰".len() as u32); // will delete ⏰ and 👩‍❤️‍💋‍👨

        assert_eq!(&txt.get_string(&txn), "👯🙇‍♀️🙇‍♀️👩‍❤️‍💋‍👨");
    }

    #[test]
    fn delete_multi_byte_character_from_middle_after_insert_and_format() {
        let mut doc = Doc::with_client_id(1);
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        txt.insert(&mut txn, 0, "❤️❤️🙇‍♀️🙇‍♀️⏰👩‍❤️‍💋‍👨👩‍❤️‍💋‍👨");
        txt.insert(&mut txn, 0, "👯");
        txt.format(
            &mut txn,
            "👯".len() as u32,
            "❤️❤️🙇‍♀️🙇‍♀️⏰".len() as u32,
            HashMap::new(),
        );
        txt.insert(&mut txn, "👯❤️❤️🙇‍♀️🙇‍♀️⏰".len() as u32, "⏰");
        txt.format(
            &mut txn,
            "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰".len() as u32,
            "👩‍❤️‍💋‍👨".len() as u32,
            HashMap::new(),
        );
        txt.remove_range(&mut txn, "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👩".len() as u32, "👩‍❤️‍💋‍👨".len() as u32);
        assert_eq!(txt.get_string(&txn).as_str(), "👯❤️❤️🙇‍♀️🙇‍♀️⏰⏰👩‍❤️‍💋‍👨");
    }

    #[test]
    fn insert_string_with_no_attribute() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        let attrs = Attrs::from([("a".into(), "a".into())]);
        txt.insert_with_attributes(&mut txn, 0, "ac", attrs.clone());
        txt.insert_with_attributes(&mut txn, 1, "b", Attrs::new());

        let expect = vec![
            Diff::new("a".into(), Some(Box::new(attrs.clone()))),
            Diff::new("b".into(), None),
            Diff::new("c".into(), Some(Box::new(attrs.clone()))),
        ];

        assert!(txt.diff(&mut txn, YChange::identity).eq(&expect))
    }

    #[test]
    fn insert_empty_string_with_attributes() {
        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();

        let attrs = Attrs::from([("a".into(), "a".into())]);
        txt.insert(&mut txn, 0, "abc");
        txt.insert(&mut txn, 1, ""); // nothing changes
        txt.insert_with_attributes(&mut txn, 1, "", attrs); // nothing changes

        assert_eq!(txt.get_string(&txn).as_str(), "abc");

        let bin = txn.encode_state_as_update_v1(&StateVector::default());

        let mut doc = Doc::new();
        let txt = doc.get_or_insert_text("test");
        let mut txn = doc.transact_mut();
        let update = Update::decode_slice(bin.as_slice()).unwrap();
        txn.apply_update(update).unwrap();

        assert_eq!(txt.get_string(&txn).as_str(), "abc");
    }

    #[test]
    fn snapshots() {
        let mut doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text("text");
        text.insert(&mut doc.transact_mut(), 0, "hello");
        let prev = doc.transact_mut().snapshot();
        text.insert(&mut doc.transact_mut(), 5, " world");
        let next = doc.transact_mut().snapshot();
        let diff = text.diff_range(
            &mut doc.transact_mut(),
            Some(&next),
            Some(&prev),
            YChange::identity,
        );

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
        )
    }

    #[test]
    fn diff_with_embedded_items() {
        let mut doc = Doc::new();
        let text = doc.get_or_insert_text("article");
        let mut txn = doc.transact_mut();

        let bold = Attrs::from([("b".into(), true.into())]);
        let italic = Attrs::from([("i".into(), true.into())]);

        text.insert_with_attributes(&mut txn, 0, "hello world", italic.clone()); // "<i>hello world</i>"
        text.format(&mut txn, 6, 5, bold.clone()); // "<i>hello <b>world</b></i>"
        let image = vec![0, 0, 0, 0];
        text.insert_embed(&mut txn, 5, image.clone()); // insert binary after "hello"
        let array = text.insert_embed(&mut txn, 5, ArrayPrelim::default()); // insert array ref after "hello"

        let italic_and_bold = Attrs::from([("b".into(), true.into()), ("i".into(), true.into())]);
        let chunks = text.diff(&txn, YChange::identity);
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
                let txt = doc.get_or_insert_text("test");
                let mut txn = doc.transact_mut();
                txt.push(&mut txn, "a");
            }
        });

        let d3 = doc.clone();
        let h3 = spawn(move || {
            for _ in 0..10 {
                let millis = fastrand::u64(1..20);
                sleep(Duration::from_millis(millis));

                let mut doc = d3.write().unwrap();
                let txt = doc.get_or_insert_text("test");
                let mut txn = doc.transact_mut();
                txt.push(&mut txn, "b");
            }
        });

        h3.join().unwrap();
        h2.join().unwrap();

        let doc = doc.read().unwrap();
        let txt: crate::TextRef = doc.get("test").unwrap();
        let len = txt.len(&doc.transact());
        assert_eq!(len, 20);
    }

    #[test]
    fn multiline_format() {
        let mut doc = Doc::with_client_id(1);
        let mut txn = doc.transact_mut();
        let txt = txn.get_or_insert_text("text");
        let bold: Option<Box<Attrs>> = Some(Box::new(Attrs::from([("bold".into(), true.into())])));
        txt.insert(&mut txn, 0, "Test\nMulti-line\nFormatting");
        txt.apply_delta(
            &mut txn,
            [
                Delta::Retain(4, bold.clone()),
                Delta::retain(1), // newline character
                Delta::Retain(10, bold.clone()),
                Delta::retain(1), // newline character
                Delta::Retain(10, bold.clone()),
            ],
        );
        let delta = txt.diff(&txn, YChange::identity);
        assert_eq!(
            delta,
            vec![
                Diff::new("Test".into(), bold.clone()),
                Diff::new("\n".into(), None),
                Diff::new("Multi-line".into(), bold.clone()),
                Diff::new("\n".into(), None),
                Diff::new("Formatting".into(), bold),
            ]
        );
    }

    #[test]
    fn delta_with_embeds() {
        let mut doc = Doc::with_client_id(1);
        let mut txn = doc.transact_mut();
        let txt = txn.get_or_insert_text("text");
        let linebreak = lib0!({
            "linebreak": "s"
        });
        txt.apply_delta(&mut txn, [Delta::insert(linebreak.clone())]);
        let delta = txt.diff(&txn, YChange::identity);
        assert_eq!(delta, vec![Diff::new(linebreak.into(), None)]);
    }

    #[test]
    fn delta_with_shared_ref() {
        let mut d1 = Doc::with_client_id(1);
        let mut txn1 = d1.transact_mut();
        let txt1 = txn1.get_or_insert_text("text");
        txt1.apply_delta(
            &mut txn1,
            [Delta::insert(MapPrelim::from([("key", "val")]))],
        );
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

        let mut d2 = Doc::with_client_id(2);
        let mut txn2 = d2.transact_mut();
        let txt2 = txn2.get_or_insert_text("text");
        let update = Update::decode_slice(&txn1.encode_update_v1()).unwrap();
        txn2.apply_update(update).unwrap();
        drop(txn1);
        drop(txn2);

        assert!(triggered.load(Ordering::Relaxed), "fired event");

        let txn = d2.transact();
        let delta = txt2.diff(&txn, YChange::identity);
        assert_eq!(delta.len(), 1);
        let d: MapRef = delta[0].insert.clone().cast(&txn).unwrap();
        assert_eq!(d.get::<Out>(&txn, "key").unwrap(), Out::Any("val".into()));
    }

    #[test]
    fn delta_snapshots() {
        let mut doc = Doc::with_options(Options {
            client_id: 1,
            skip_gc: true,
            ..Default::default()
        });
        let mut txn = doc.transact_mut();
        let txt = txn.get_or_insert_text("text");
        txt.apply_delta(&mut txn, [Delta::insert("abcd")]);
        let snapshot1 = txn.snapshot(); // 'abcd'
        txt.apply_delta(
            &mut txn,
            [Delta::retain(1), Delta::insert("x"), Delta::delete(1)],
        );
        let snapshot2 = txn.snapshot(); // 'axcd'
        txt.apply_delta(
            &mut txn,
            [
                Delta::retain(2),   // ax^cd
                Delta::delete(1),   // ax^d
                Delta::insert("x"), // axx^d
                Delta::delete(1),   // axx^
            ],
        );
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
        let mut doc = Doc::with_options(Options {
            client_id: 1,
            skip_gc: true,
            ..Default::default()
        });
        let mut txn = doc.transact_mut();
        let txt = txn.get_or_insert_text("text");
        txt.apply_delta(&mut txn, [Delta::insert("abcd")]);
        let snapshot1 = txn.snapshot();
        txt.apply_delta(&mut txn, [Delta::retain(4), Delta::insert("e")]);
        let state1 = txt.diff_range(&mut txn, Some(&snapshot1), None, YChange::identity);
        assert_eq!(state1, vec![Diff::new("abcd".into(), None)]);
    }

    #[test]
    fn empty_delta_chunks() {
        let mut doc = Doc::with_client_id(1);
        let mut txn = doc.transact_mut();
        let txt = txn.get_or_insert_text("text");

        let delta = vec![
            Delta::insert("a"),
            Delta::Inserted(
                "".into(),
                Some(Box::new(Attrs::from([(
                    Arc::from("bold"),
                    Any::from(true),
                )]))),
            ),
            Delta::insert("b"),
        ];

        txt.apply_delta(&mut txn, delta);
        assert_eq!(txt.get_string(&txn), "ab");

        let bin = txn.encode_state_as_update_v1(&StateVector::default());

        let mut doc2 = Doc::with_client_id(2);
        let mut txn = doc2.transact_mut();
        let txt = txn.get_or_insert_text("text");

        let update = Update::decode_slice(bin.as_slice()).unwrap();
        txn.apply_update(update).unwrap();
        assert_eq!(txt.get_string(&txn), "ab");
    }
}
