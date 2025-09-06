use crate::read::DecoderV1;
use crate::{ClientID, MultiDoc, Transaction};
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

pub fn sync<const N: usize>(txns: [&mut Transaction<'_>; N]) {
    let states: Vec<_> = txns.iter().map(|txn| txn.state_vector().unwrap()).collect();
    for i in 0..N {
        let sv = states[i].clone();
        for j in 0..N {
            if i != j {
                let update = txns[j].diff_update(&sv).unwrap();
                txns[i]
                    .apply_update(&mut DecoderV1::from_slice(&update))
                    .unwrap();
            }
        }
    }
}
