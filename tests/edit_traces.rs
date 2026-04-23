use flate2::read::GzDecoder;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Instant;
use tempfile::TempDir;
use ysr::{ClientID, MultiDoc, Text, Unmounted};

#[ignore]
#[test]
fn edit_trace_automerge() {
    test_editing_trace(
        "./tests/test-data/editing-traces/sequential_traces/automerge-paper.json.gz",
    );
}

#[test]
fn edit_trace_friendsforever() {
    test_editing_trace(
        "./tests/test-data/editing-traces/sequential_traces/friendsforever_flat.json.gz",
    );
}

#[ignore]
#[test]
fn edit_trace_sephblog1() {
    test_editing_trace("./tests/test-data/editing-traces/sequential_traces/seph-blog1.json.gz");
}

#[ignore]
#[test]
fn edit_trace_sveltecomponent() {
    test_editing_trace(
        "./tests/test-data/editing-traces/sequential_traces/sveltecomponent.json.gz",
    );
}

#[ignore]
#[test]
fn edit_trace_rustcode() {
    test_editing_trace("./tests/test-data/editing-traces/sequential_traces/rustcode.json.gz");
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
    // let start = SystemTime::now();
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

fn multi_doc<C>(client_id: C) -> (crate::MultiDoc, TempDir)
where
    C: Into<ClientID>,
{
    let dir = TempDir::new().unwrap();
    let env = ysr::lmdb::Env::builder()
        .max_dbs(10)
        .map_size(10 * 1024 * 1024) // 10 MB
        .open(dir.path(), 0o600)
        .unwrap();
    let multi_doc = MultiDoc::new(env, Some(client_id.into()));
    (multi_doc, dir)
}

fn test_editing_trace(path: &str) {
    let (mdoc, _tempdir) = multi_doc(1);
    let data = load_testing_data(path);
    let txt: Unmounted<Text> = Unmounted::root("text");
    let start = Instant::now();
    {
        let mut tx = mdoc.transact_mut("test").unwrap();
        let mut txt = txt.mount_mut(&mut tx).unwrap();
        for t in data.txns {
            for patch in t.patches {
                let at = patch.0;
                let delete_count = patch.1;
                let content = patch.2;

                if delete_count != 0 {
                    txt.remove_range(at..(at + delete_count)).unwrap();
                }
                if !content.is_empty() {
                    txt.insert(at, &content).unwrap();
                }
            }
        }
        tx.commit(None).unwrap();
    }
    let finish = Instant::now();
    println!("elapsed: {}ms", (finish - start).as_millis());
    let mut tx = mdoc.transact_mut("test").unwrap();
    let txt = txt.mount_mut(&mut tx).unwrap();
    assert_eq!(txt.to_string(), data.end_content);
}
