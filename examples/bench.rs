use lmdb_rs_m::EnvBuilder;
use std::time::Instant;
use ysr::{DecoderV1, MultiDoc, TextRef, Unmounted};

fn main() {
    let data = std::fs::read("./examples/data/b4-update.bin").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let env = EnvBuilder::new()
        .max_dbs(1)
        .map_size(10 * 1024 * 1024)
        .open(dir.path(), 0o777)
        .unwrap();
    let mdoc = MultiDoc::new(env, 1.into());
    let mut tx = mdoc.transact_mut("test").unwrap();
    let start = Instant::now();
    tx.apply_update(&mut DecoderV1::from_slice(&data)).unwrap();
    tx.commit(None).unwrap();
    let end = start.elapsed();
    println!("applied {}B update in {:?}", data.len(), end);
    let mut tx = mdoc.transact_mut("test").unwrap();
    let txt: TextRef<_> = Unmounted::root("text").mount_mut(&mut tx).unwrap();
    let str = txt.to_string();

    let expected = std::fs::read_to_string("./examples/data/b4-string.txt").unwrap();
    assert_eq!(str, expected);
}
