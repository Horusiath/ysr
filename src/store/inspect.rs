use crate::store::Db;
use lmdb_rs_m::Database;
use std::fmt::Debug;

pub struct DbInspector<'tx> {
    db: &'tx mut Database<'tx>,
}

impl<'tx> DbInspector<'tx> {
    pub fn new(db: &'tx mut Database<'tx>) -> Self {
        DbInspector { db }
    }
}

impl<'tx> Debug for DbInspector<'tx> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let sv = self.db.state_vector()?;
        let meta = self.db.meta()?;
        let intern_strings = self.db.intern_strings()?;
        let blocks = self.db.blocks()?;
        let contents = self.db.contents()?;
        let map_entries = self.db.map_entries()?;

        f.debug_struct("Db")
            .field("meta", todo!())
            .field("state_vector", todo!())
            .field("intern_string", todo!())
            .finish()
    }
}
