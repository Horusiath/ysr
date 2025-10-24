use crate::transaction::Origin;
use crate::{ClientID, Transaction};
use lmdb_rs_m::{DbFlags, Environment};
use rand::random;

pub struct MultiDoc {
    client_id: ClientID,
    env: Environment,
}

impl MultiDoc {
    pub fn new(env: Environment, client_id: ClientID) -> Self {
        MultiDoc { env, client_id }
    }

    pub fn transact_mut(&self, doc_id: &str) -> crate::Result<Transaction<'_>> {
        let handle = self
            .env
            .create_db(doc_id, DbFlags::DbCreate | DbFlags::DbAllowDups)?;
        let tx = self.env.new_transaction()?;
        Ok(Transaction::read_write(tx, handle, None))
    }

    pub fn transact_mut_with<O: Into<Origin>>(
        &self,
        doc_id: &str,
        origin: O,
    ) -> crate::Result<Transaction<'_>> {
        let origin = origin.into();
        let handle = self
            .env
            .create_db(doc_id, DbFlags::DbCreate | DbFlags::DbAllowDups)?;
        let tx = self.env.new_transaction()?;
        Ok(Transaction::read_write(tx, handle, Some(origin)))
    }
}

impl From<Environment> for MultiDoc {
    #[inline]
    fn from(value: Environment) -> Self {
        Self::new(value, random::<u32>().into())
    }
}

#[cfg(test)]
mod test {

    use crate::read::DecoderV1;
    use crate::test_util::multi_doc;
    use crate::transaction::{CommitFlags, TransactionSummary};

    use crate::{lib0, Map, MultiDoc, StateVector, Text, TextRef, Unmounted};
    use bytes::Bytes;
    use uuid::Uuid;

    #[test]
    fn apply_update_basic_v1() {
        let (mdoc, _dir) = multi_doc(1);
        /* Result of calling following code:
        ```javascript
        const doc = new Y.Doc()
        const ytext = doc.getText('type')
        doc.transact(function () {
            for (let i = 0; i < 3; i++) {
                ytext.insert(0, (i % 10).toString())
            }
        })
        const update = Y.encodeStateAsUpdate(doc)
        ```
         */
        let update = &[
            1, 3, 227, 214, 245, 198, 5, 0, 4, 1, 4, 116, 121, 112, 101, 1, 48, 68, 227, 214, 245,
            198, 5, 0, 1, 49, 68, 227, 214, 245, 198, 5, 1, 1, 50, 0,
        ];

        let doc_id = Uuid::new_v4().to_string();
        let txt: Unmounted<Text> = Unmounted::root("type");
        let mut tx = mdoc.transact_mut(&doc_id).unwrap();
        let mut decoder = DecoderV1::from_slice(update);
        tx.apply_update(&mut decoder).unwrap();
        tx.commit(None).unwrap();

        let mut tx = mdoc.transact_mut(&doc_id).unwrap();
        let txt: TextRef<_> = txt.mount_mut(&mut tx).unwrap();
        let actual = txt.to_string();
        assert_eq!(actual, "210");
    }

    #[test]
    fn integrate() {
        let txt: Unmounted<Text> = Unmounted::root("test");
        // create new document at A and add some initial text to it
        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();

        txt1.insert(0, "hello").unwrap();
        txt1.insert(5, " ").unwrap();
        txt1.insert(6, "world").unwrap();

        assert_eq!(txt1.to_string(), "hello world");

        // create document at B
        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();
        let sv = t2.state_vector().unwrap();

        // create an update A->B based on B's state vector
        let binary = t1.diff_update(&sv).unwrap();

        // decode an update incoming from A and integrate it at B
        t2.apply_update(&mut DecoderV1::from_slice(&binary))
            .unwrap();

        // check if B sees the same thing that A does
        let txt2 = txt.mount_mut(&mut t2).unwrap();
        assert_eq!(txt2.to_string(), "hello world");

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();
    }

    #[test]
    fn encode_basic() {
        let txt: Unmounted<Text> = Unmounted::root("type");
        let (doc, _) = multi_doc(1490905955);
        let mut t = doc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut t).unwrap();
        txt.insert(0, "0").unwrap();
        txt.insert(0, "1").unwrap();
        txt.insert(0, "2").unwrap();

        let encoded = t.diff_update(&StateVector::default()).unwrap();
        let expected = &[
            1, 3, 227, 214, 245, 198, 5, 0, 4, 1, 4, 116, 121, 112, 101, 1, 48, 68, 227, 214, 245,
            198, 5, 0, 1, 49, 68, 227, 214, 245, 198, 5, 1, 1, 50, 0,
        ];
        assert_eq!(encoded.as_slice(), expected);
    }

    #[test]
    fn partially_duplicated_update() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(0, "hello").unwrap();
        let u = t1.diff_update(&StateVector::default()).unwrap();

        let (d2, _) = multi_doc(1);
        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u)).unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(5, "world").unwrap();
        let u = t1.diff_update(&StateVector::default()).unwrap();
        t2.apply_update(&mut DecoderV1::from_slice(&u)).unwrap();

        t1.commit(None).unwrap();
        t2.commit(None).unwrap();

        let t1 = d1.transact_mut("test").unwrap();
        let t2 = d2.transact_mut("test").unwrap();
        let txt1 = txt.mount(&t1).unwrap();
        let txt2 = txt.mount(&t2).unwrap();

        assert_eq!(txt1.to_string(), txt2.to_string());
    }

    #[test]
    fn out_of_order_updates() {
        let map: Unmounted<Map> = Unmounted::root("type");
        let mut updates = Vec::new();
        let mut summary = TransactionSummary::new(CommitFlags::UPDATE_V1);

        let put_value = {
            |mdoc: &MultiDoc,
             summary: &mut TransactionSummary,
             updates: &mut Vec<Bytes>,
             key: &str,
             value: f64| {
                let mut tx = mdoc.transact_mut("test").unwrap();
                let mut map = map.mount_mut(&mut tx).unwrap();
                map.insert(key, value).unwrap();
                tx.commit(Some(summary)).unwrap();

                let update = summary.update().clone();
                updates.push(update);
                summary.clear();
            }
        };

        let (d1, _) = multi_doc(1);

        put_value(&d1, &mut summary, &mut updates, "a", 1.0);
        put_value(&d1, &mut summary, &mut updates, "a", 1.1);
        put_value(&d1, &mut summary, &mut updates, "b", 2.0);

        let t1 = d1.transact_mut("test").unwrap();
        let m1 = map.mount(&t1).unwrap();
        assert_eq!(m1.to_value().unwrap(), lib0!({"a": 1.1, "b": 2.0}));

        let (d2, _) = multi_doc(2);
        {
            let u3 = updates.pop().unwrap();
            let u2 = updates.pop().unwrap();
            let u1 = updates.pop().unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&u1)).unwrap();
            let m2 = map.mount(&t1).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.0})); // applied
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&u3)).unwrap();
            let m2 = map.mount(&t1).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.0})); // pending update waiting for u2
            txn.commit(None).unwrap();

            let mut txn = d2.transact_mut("test").unwrap();
            txn.apply_update(&mut DecoderV1::from_slice(&u2)).unwrap();
            let m2 = map.mount(&t1).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.1, "b": 2.0})); // applied all updates
            txn.commit(None).unwrap();
        }
    }
}
