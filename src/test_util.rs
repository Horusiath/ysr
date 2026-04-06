use crate::read::DecoderV1;
use crate::{ClientID, MultiDoc, Transaction};
use tempfile::TempDir;

pub fn multi_doc<C>(client_id: C) -> (crate::MultiDoc, TempDir)
where
    C: Into<ClientID>,
{
    let dir = TempDir::new().unwrap();
    let env = crate::lmdb::Env::builder()
        .max_dbs(10)
        .map_size(10 * 1024 * 1024) // 10 MB
        .open(dir.path(), 0o600)
        .unwrap();
    let multi_doc = MultiDoc::new(env, Some(client_id.into()));
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
