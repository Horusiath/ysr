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
        let handle = self.env.create_db(doc_id, DbFlags::DbCreate)?;
        let tx = self.env.new_transaction()?;
        Ok(Transaction::read_write(tx, handle, None))
    }

    pub fn transact_mut_with<O: Into<Origin>>(
        &self,
        doc_id: &str,
        origin: O,
    ) -> crate::Result<Transaction<'_>> {
        let origin = origin.into();
        let handle = self.env.create_db(doc_id, DbFlags::DbCreate)?;
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
    use crate::{Text, TextRef, Unmounted};
    use uuid::Uuid;

    #[test]
    fn apply_update_basic_v1() {
        let (mdoc, _dir) = crate::test_util::multi_doc(1);
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

        let txt: TextRef<_> = txt.mount_mut(&mut tx).unwrap();
        let actual = txt.to_string();
        assert_eq!(actual, "210");
    }
}
