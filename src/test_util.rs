use crate::{ClientID, MultiDoc};
use tempfile::TempDir;

pub fn multi_doc<C>(client_id: C) -> (crate::MultiDoc, TempDir)
where
    C: Into<ClientID>,
{
    let client_id = client_id.into();
    let dir = TempDir::new().unwrap();
    let env = lmdb_rs_m::Environment::builder()
        .max_dbs(10)
        .open(dir.path(), 0o600)
        .unwrap();
    let multi_doc = MultiDoc::new(env, client_id);
    (multi_doc, dir)
}
