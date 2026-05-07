use flate2::read::GzDecoder;
use lmdb_master_sys::MDB_CREATE;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};
use yrs::ReadTxn;
use ysr::lmdb::{Dbi, Env, EnvFlags};
use zerocopy::IntoBytes;

fn main() {
    let data = load_testing_data(
        "./tests/test-data/editing-traces/sequential_traces/sveltecomponent.json.gz",
    );
    editing_trace_disk(&data, 0.5);
    editing_trace_memory(&data, 0.5);
}

fn editing_trace_disk(test: &TestData, p: f64) {
    use ysr::*;

    let dir = tempfile::tempdir().unwrap();
    let env = ysr::lmdb::Env::builder()
        .flags(EnvFlags::NOSYNC)
        .max_dbs(1)
        .map_size(10 * 1024 * 1024)
        .open(dir.path(), 0o777)
        .unwrap();
    let txt: Unmounted<Text> = Unmounted::root("text");
    let mdoc = MultiDoc::new(env, Some(1.into()));

    // 1. preload `p` % of the test data as an initial state of the document
    let init_size = (test.txns.len() as f64 * p).ceil() as usize;
    {
        println!("use {}% of the test data as initial state", p * 100.0);

        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();
        for i in 0..init_size {
            let t = &test.txns[i];
            for patch in t.patches.iter() {
                let at = patch.0;
                let delete_count = patch.1;
                let content = &*patch.2;

                if delete_count != 0 {
                    txt.remove_range(at..(at + delete_count)).unwrap();
                }
                if !content.is_empty() {
                    txt.insert(at, content).unwrap();
                }
            }
        }
        tx.commit(None).unwrap();
    }

    // 2. actual test
    {
        let start = Instant::now();
        for i in init_size..test.txns.len() {
            let mut tx = mdoc.transact_mut("test").unwrap();
            let mut txt = txt.mount_mut(&mut tx).unwrap();
            let t = &test.txns[i];
            for patch in t.patches.iter() {
                let at = patch.0;
                let delete_count = patch.1;
                let content = &*patch.2;

                if delete_count != 0 {
                    txt.remove_range(at..(at + delete_count)).unwrap();
                }
                if !content.is_empty() {
                    txt.insert(at, content).unwrap();
                }
            }
            tx.commit(None).unwrap();
        }
        let end = start.elapsed();
        println!(
            "applied {} updates in {:?}",
            test.txns.len() - init_size,
            end
        );
    }
}

fn editing_trace_memory(test: &TestData, p: f64) {
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, Text, Transact, Update};

    let dir = tempfile::tempdir().unwrap();
    /// Persist individual incremental update.
    fn persist_update(db: &ysr::lmdb::Database<'_>, update: &[u8]) {
        let mut key = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        db.put(key.as_bytes(), update).unwrap();
    }
    /// Load document.
    fn load_doc(db: &ysr::lmdb::Database<'_>, tx: &mut yrs::TransactionMut<'_>) -> usize {
        let mut cursor = db.cursor().unwrap();
        let mut i = 0;
        if let Ok((_, update)) = cursor.set_range(&[0]) {
            let update = Update::decode_v1(update).unwrap();
            tx.apply_update(update).unwrap();
            i += 1;
            while let Ok((_, update)) = cursor.next() {
                let update = Update::decode_v1(update).unwrap();
                tx.apply_update(update).unwrap();
                i += 1;
            }
        }
        i
    }
    /// Remove all incremental updates and store a single compact document update.
    fn flush(db: &ysr::lmdb::Database<'_>, tx: &yrs::TransactionMut<'_>) {
        let mut cursor = db.cursor().unwrap();
        if let Ok(_) = cursor.set_range(&[0]) {
            while { cursor.del().is_ok() } {}
        }

        let mut key = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let update = tx.encode_state_as_update_v1(&yrs::StateVector::default());
        db.put(key.as_bytes(), &update).unwrap();
    }

    fn setup_persistence(doc: &yrs::Doc, env: Arc<Env>, db: Dbi) {
        let mut c = AtomicU64::new(0);
        doc.observe_update_v1_with("test", move |tx, u| {
            let lmdb_tx = env.begin_rw_txn().unwrap();
            let db = lmdb_tx.bind(&db);
            if c.fetch_add(1, Ordering::SeqCst) % 100 == 0 {
                flush(&db, &tx);
            } else {
                persist_update(&db, &u.update);
            }
            lmdb_tx.commit().unwrap();
        })
        .unwrap();
    }

    let env = Arc::new(
        ysr::lmdb::Env::builder()
            .flags(EnvFlags::NOSYNC)
            .max_dbs(1)
            .map_size(10 * 1024 * 1024)
            .open(dir.path(), 0o777)
            .unwrap(),
    );

    let db = env.create_db("test", MDB_CREATE).unwrap();

    let doc = Doc::new();
    setup_persistence(&doc, env.clone(), db);
    let txt = doc.get_or_insert_text("text");

    // 1. preload `p` % of the test data as an initial state of the document
    let init_size = (test.txns.len() as f64 * p).ceil() as usize;
    {
        println!("use {}% of the test data as initial state", p * 100.0);

        for i in 0..init_size {
            let mut tx = doc.transact_mut();
            let t = &test.txns[i];
            for patch in t.patches.iter() {
                let at = patch.0 as u32;
                let delete_count = patch.1 as u32;
                let content = &*patch.2;

                if delete_count != 0 {
                    txt.remove_range(&mut tx, at, delete_count);
                }
                if !content.is_empty() {
                    txt.insert(&mut tx, at, content);
                }
            }
        }
    }

    // 2. actual test
    {
        let start = Instant::now();
        let doc = Doc::new();
        let txt = doc.get_or_insert_text("text");

        // load initial state of the doc
        {
            let lmdb_tx = env.begin_ro_txn().unwrap();
            let db = lmdb_tx.bind(&db);
            let mut tx = doc.transact_mut();
            let c = load_doc(&db, &mut tx);
            drop(lmdb_tx);
        }
        setup_persistence(&doc, env.clone(), db);

        for i in init_size..test.txns.len() {
            let mut tx = doc.transact_mut();
            let t = &test.txns[i];
            for patch in t.patches.iter() {
                let at = patch.0 as u32;
                let delete_count = patch.1 as u32;
                let content = &*patch.2;

                if delete_count != 0 {
                    txt.remove_range(&mut tx, at, delete_count);
                }
                if !content.is_empty() {
                    txt.insert(&mut tx, at, content);
                }
            }
        }
        let end = start.elapsed();
        println!(
            "applied {} updates in {:?}",
            test.txns.len() - init_size,
            end
        );
    }
}

/// This file contains some simple helpers for loading test data. Its used by benchmarking and
/// testing code.

/// (position, delete length, insert content).
#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestPatch(pub usize, pub usize, pub String);

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestTxn {
    // time: String, // ISO String. Unused.
    pub patches: Vec<TestPatch>,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq)]
pub struct TestData {
    #[serde(default)]
    pub using_byte_positions: bool,

    #[serde(rename = "startContent")]
    pub start_content: String,
    #[serde(rename = "endContent")]
    pub end_content: String,

    pub txns: Vec<TestTxn>,
}

impl TestData {
    pub fn len(&self) -> usize {
        self.txns.iter().map(|txn| txn.patches.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        !self.txns.iter().any(|txn| !txn.patches.is_empty())
    }

    pub fn patches(&self) -> impl Iterator<Item = &TestPatch> {
        self.txns.iter().flat_map(|txn| txn.patches.iter())
    }
}

/// Load the testing data at the specified file. If the filename ends in .gz, it will be
/// transparently uncompressed.
///
/// This method panics if the file does not exist, or is corrupt. It'd be better to have a try_
/// variant of this method, but given this is mostly for benchmarking and testing, I haven't felt
/// the need to write that code.
pub fn load_testing_data(filename: &str) -> TestData {
    let file = File::open(filename).unwrap();

    let mut reader = BufReader::new(file);
    // We could pass the GzDecoder straight to serde, but it makes it way slower to parse for
    // some reason.
    let mut raw_json = vec![];

    if filename.ends_with(".gz") {
        let mut reader = GzDecoder::new(reader);
        reader.read_to_end(&mut raw_json).unwrap();
    } else {
        reader.read_to_end(&mut raw_json).unwrap();
    }
    let data: TestData = serde_json::from_reader(raw_json.as_slice()).unwrap();
    data
}
