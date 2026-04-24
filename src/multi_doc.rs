use crate::lmdb::Env;
use crate::transaction::Origin;
use crate::{ClientID, Transaction};
use lmdb_master_sys::MDB_CREATE;

/// [MultiDoc] is an entry point to the library. It allows to store multiple documents within
/// the same database file. Individual documents can be accessed by opening transaction with their
/// identifiers.
pub struct MultiDoc {
    env: Env,
    client_id: Option<ClientID>,
}

impl MultiDoc {
    /// Creates a new [MultiDoc] instance.
    ///
    /// If `client_id` was provided it will be used by all the documents within the scope of
    /// this multi-doc. Otherwise, it will be generated randomly once when the document is created,
    /// then persisted and reused in subsequent requests.
    pub fn new(env: Env, client_id: Option<ClientID>) -> Self {
        MultiDoc { env, client_id }
    }

    /// Returns the LMDB [Env] reference.
    pub fn env(&self) -> &Env {
        &self.env
    }

    /// Opens a new read-only transaction into the document with a given `doc_id`. If the document
    /// doesn't exist locally an error will be returned. This transaction can only be used
    /// for reading the state of the document. Any operations changing its state will cause an error.
    ///
    /// Multiple read-only transactions to the same document can coexist at the same time without
    /// blocking the read-write transactions (they won't however sho the changes made by concurrent
    /// read-write transactions).
    ///
    /// Keep in mind that read-only transactions are blocking the LMDB pages from being released and
    /// reused by future writes. This means that keeping the transaction for prolonged amount of
    /// time can cause database file to grow in face of writes. The database file can be compacted
    /// into a new file via [Env::copy_to] method with `compact` flag on.
    pub fn transact(&self, doc_id: &str) -> crate::Result<Transaction<'_>> {
        let handle = self.env.create_db(doc_id, 0)?;
        let tx = self.env.begin_ro_txn()?;
        Ok(Transaction::read_only(tx, handle))
    }

    /// Opens a new read-write transaction into the document with a given `doc_id`. If the document
    /// doesn't exist locally, it will be created. Each newly created document requires LMDB to
    /// reserve at least 4 pages of extra space (1 db page, 2x transaction root pages and
    /// 1 leaf page for initial data), which means that **overhead of a document is at least 16KiB**.
    ///
    /// Only one read-write transaction for the same document can exist at the same time. It will
    /// not block read-only transactions from being created, however read-only transactions will
    /// hold on database pages from being released and reused by read-write transaction to apply
    /// changes. This means that keeping the read-only transaction for prolonged amount of
    /// time can cause database file to grow in face of writes. The database file can be compacted
    /// into a new file via [Env::copy_to] method with `compact` flag on.
    pub fn transact_mut(&self, doc_id: &str) -> crate::Result<Transaction<'_>> {
        let handle = self.env.create_db(doc_id, MDB_CREATE)?;
        let tx = self.env.begin_rw_txn()?;
        Transaction::read_write(tx, handle, self.client_id, None)
    }

    /// Opens a new read-write transaction into the document with a given `doc_id` with a specific
    /// origin that can be used to differentiate transaction purpose. If the document doesn't exist
    /// locally, it will be created. Each newly created document requires LMDB to reserve
    /// at least 4 pages of extra space (1 db page, 2x transaction root pages and 1 leaf page for
    /// initial data), which means that **overhead of a document is at least 16KiB**.
    ///
    /// Only one read-write transaction for the same document can exist at the same time. It will
    /// not block read-only transactions from being created, however read-only transactions will
    /// hold on database pages from being released and reused by read-write transaction to apply
    /// changes. This means that keeping the read-only transaction for prolonged amount of
    /// time can cause database file to grow in face of writes. The database file can be compacted
    /// into a new file via [Env::copy_to] method with `compact` flag on.
    pub fn transact_mut_with<O: Into<Origin>>(
        &self,
        doc_id: &str,
        origin: O,
    ) -> crate::Result<Transaction<'_>> {
        let origin = origin.into();
        let handle = self.env.create_db(doc_id, MDB_CREATE)?;
        let tx = self.env.begin_rw_txn()?;
        Transaction::read_write(tx, handle, self.client_id, Some(origin))
    }

    /// Permanently removes a document from current database file, together with all of its contents.
    /// The space occupied by the document doesn't cause the database file to shrink, however it can
    /// be reused by other documents to accommodate their changes.
    ///
    /// The database file can be compacted into a new file via [Env::copy_to] method with `compact`
    /// flag on.
    pub fn destroy_doc(&self, doc_id: &str) -> crate::Result<()> {
        let handle = self.env.create_db(doc_id, 0)?;
        let tx = self.env.begin_rw_txn()?;
        tx.bind(&handle).remove()?;
        Ok(())
    }
}

impl From<Env> for MultiDoc {
    #[inline]
    fn from(value: Env) -> Self {
        Self::new(value, None)
    }
}

#[cfg(test)]
mod test {
    use crate::test_util::multi_doc;
    use crate::transaction::{CommitFlags, TransactionSummary};

    use crate::{Map, MultiDoc, StateVector, Text, TextRef, Unmounted, lib0};

    use crate::lib0::Encoding;
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
        tx.apply_update(update, Encoding::V1).unwrap();
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
        {
            let mut txt1 = txt.mount_mut(&mut t1).unwrap();

            txt1.insert(0, "hello").unwrap();
            txt1.insert(5, " ").unwrap();
            txt1.insert(6, "world").unwrap();

            assert_eq!(txt1.to_string(), "hello world");
        }

        // create document at B
        let (d2, _) = multi_doc(2);
        let mut t2 = d2.transact_mut("test").unwrap();
        let sv = t2.state_vector().unwrap();

        // create an update A->B based on B's state vector
        let binary = t1.diff_update(&sv, Encoding::V1).unwrap();

        // decode an update incoming from A and integrate it at B
        t2.apply_update(&binary, Encoding::V1).unwrap();

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

        let encoded = t
            .diff_update(&StateVector::default(), Encoding::V1)
            .unwrap();
        let expected = &[
            1, 3, 227, 214, 245, 198, 5, 0, 4, 1, 4, 116, 121, 112, 101, 1, 48, 68, 227, 214, 245,
            198, 5, 0, 1, 49, 68, 227, 214, 245, 198, 5, 1, 1, 50, 0,
        ];
        assert_eq!(&*encoded, expected);
    }

    #[test]
    fn partially_duplicated_update() {
        let txt: Unmounted<Text> = Unmounted::root("type");

        let (d1, _) = multi_doc(1);
        let mut t1 = d1.transact_mut("test").unwrap();
        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(0, "hello").unwrap();
        let u = t1
            .diff_update(&StateVector::default(), Encoding::V1)
            .unwrap();

        let (d2, _) = multi_doc(1);
        let mut t2 = d2.transact_mut("test").unwrap();
        t2.apply_update(&u, Encoding::V1).unwrap();

        let mut txt1 = txt.mount_mut(&mut t1).unwrap();
        txt1.insert(5, "world").unwrap();
        let u = t1
            .diff_update(&StateVector::default(), Encoding::V1)
            .unwrap();
        t2.apply_update(&u, Encoding::V1).unwrap();

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
             updates: &mut Vec<Vec<u8>>,
             key: &str,
             value: f64| {
                let mut tx = mdoc.transact_mut("test").unwrap();
                let mut map = map.mount_mut(&mut tx).unwrap();
                map.insert(key, value).unwrap();
                tx.commit(Some(summary)).unwrap();

                let update = summary.update.clone();
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

            let mut t2 = d2.transact_mut("test").unwrap();
            t2.apply_update(&u1, Encoding::V1).unwrap();
            let m2 = map.mount(&t2).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.0})); // applied
            t2.commit(None).unwrap();

            let mut t2 = d2.transact_mut("test").unwrap();
            t2.apply_update(&u3, Encoding::V1).unwrap();
            let m2 = map.mount(&t2).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.0})); // pending update waiting for u2
            t2.commit(None).unwrap();

            let mut t2 = d2.transact_mut("test").unwrap();
            t2.apply_update(&u2, Encoding::V1).unwrap();
            let m2 = map.mount(&t2).unwrap();
            assert_eq!(m2.to_value().unwrap(), lib0!({"a": 1.1, "b": 2.0})); // applied all updates
            t2.commit(None).unwrap();
        }
    }
}
