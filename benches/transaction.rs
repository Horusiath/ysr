use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Duration;
use tempfile::TempDir;
use ysr::lib0::v1::DecoderV1;
use ysr::lib0::v2::DecoderV2;
use ysr::{MultiDoc, StateVector, Text, Unmounted};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// LMDB environment backed by a temporary directory.
/// The `TempDir` is kept alive to prevent cleanup until the struct is dropped.
struct TestEnv {
    mdoc: MultiDoc,
    _dir: TempDir,
}

impl TestEnv {
    fn new() -> Self {
        Self::with_flags(0)
    }

    fn nosync() -> Self {
        Self::with_flags(ysr::lmdb::ENV_NOSYNC)
    }

    fn with_flags(flags: u32) -> Self {
        let dir = TempDir::new().unwrap();
        let env = ysr::lmdb::Env::builder()
            .max_dbs(10)
            .map_size(100 * 1024 * 1024) // 100 MB – enough for all bench datasets
            .flags(flags)
            .open(dir.path(), 0o600)
            .unwrap();
        let mdoc = MultiDoc::new(env, Some(1.into()));
        TestEnv { mdoc, _dir: dir }
    }
}

#[derive(Clone, Copy)]
enum Encoding {
    V1,
    V2,
}

struct BinDataset {
    name: &'static str,
    encoding: Encoding,
    data: Vec<u8>,
}

impl BinDataset {
    fn apply(&self, tx: &mut ysr::Transaction<'_>) {
        match self.encoding {
            Encoding::V1 => {
                tx.apply_update(&mut DecoderV1::from_slice(&self.data))
                    .unwrap();
            }
            Encoding::V2 => {
                tx.apply_update(&mut DecoderV2::from_slice(&self.data).unwrap())
                    .unwrap();
            }
        }
    }
}

fn load_bin_datasets() -> Vec<BinDataset> {
    [
        (
            "small-v2",
            Encoding::V2,
            "./tests/test-data/bench-input/small-test-dataset.bin",
        ),
        // medium-test-dataset.bin is excluded for now: 30MB dataset
        // needs format investigation (decodes with neither V1 nor V2).
        (
            "b4-v1",
            Encoding::V1,
            "./tests/test-data/bench-input/b4-update.bin",
        ),
    ]
    .into_iter()
    .filter_map(|(name, encoding, path)| {
        std::fs::read(path).ok().map(|data| BinDataset {
            name,
            encoding,
            data,
        })
    })
    .collect()
}

// -- Editing trace loading (mirrors tests/edit_traces.rs) -------------------

#[derive(Debug, Clone, Deserialize)]
struct TestPatch(usize, usize, String);

#[derive(Debug, Clone, Deserialize)]
struct TestTxn {
    patches: Vec<TestPatch>,
}

#[derive(Debug, Clone, Deserialize)]
struct TestData {
    #[serde(rename = "startContent")]
    #[allow(dead_code)]
    start_content: String,
    #[serde(rename = "endContent")]
    #[allow(dead_code)]
    end_content: String,
    txns: Vec<TestTxn>,
}

fn load_testing_data(filename: &str) -> TestData {
    let file = File::open(filename).unwrap();
    let mut reader = BufReader::new(file);
    let mut raw_json = Vec::new();
    if filename.ends_with(".gz") {
        GzDecoder::new(reader).read_to_end(&mut raw_json).unwrap();
    } else {
        reader.read_to_end(&mut raw_json).unwrap();
    }
    serde_json::from_reader(raw_json.as_slice()).unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark applying a pre-encoded binary update (decode + integrate).
fn bench_apply_update(c: &mut Criterion) {
    let datasets = load_bin_datasets();
    let mut group = c.benchmark_group("apply_update");
    group.sample_size(10);

    for ds in &datasets {
        group.bench_with_input(BenchmarkId::from_parameter(ds.name), ds, |b, ds| {
            b.iter_batched(
                TestEnv::new,
                |env| {
                    let mut tx = env.mdoc.transact_mut("test").unwrap();
                    ds.apply(&mut tx);
                    // transaction is dropped (aborted) without commit
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Benchmark applying a binary update followed by committing to LMDB.
fn bench_apply_and_commit(c: &mut Criterion) {
    let datasets = load_bin_datasets();
    let mut group = c.benchmark_group("apply_and_commit");
    group.sample_size(10);

    for ds in &datasets {
        group.bench_with_input(BenchmarkId::from_parameter(ds.name), ds, |b, ds| {
            b.iter_batched(
                TestEnv::new,
                |env| {
                    let mut tx = env.mdoc.transact_mut("test").unwrap();
                    ds.apply(&mut tx);
                    tx.commit(None).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Benchmark encoding an incremental update (uncommitted transaction state).
///
/// Setup applies the update once; the benchmark repeatedly encodes the
/// incremental diff — a read-only operation on the transaction.
fn bench_incremental_update(c: &mut Criterion) {
    let datasets = load_bin_datasets();
    let mut group = c.benchmark_group("incremental_update");

    for ds in &datasets {
        let env = TestEnv::new();
        let mut tx = env.mdoc.transact_mut("test").unwrap();
        ds.apply(&mut tx);

        group.bench_function(ds.name, |b| {
            b.iter(|| tx.incremental_update().unwrap());
        });

        tx.commit(None).unwrap();
    }

    group.finish();
}

/// Benchmark encoding a full diff update from an empty state vector.
///
/// The document is first committed so the data lives in LMDB; then a new
/// read-write transaction repeatedly encodes the full state.
fn bench_diff_update(c: &mut Criterion) {
    let datasets = load_bin_datasets();
    let mut group = c.benchmark_group("diff_update");

    for ds in &datasets {
        let env = TestEnv::new();
        {
            let mut tx = env.mdoc.transact_mut("test").unwrap();
            ds.apply(&mut tx);
            tx.commit(None).unwrap();
        }

        let tx = env.mdoc.transact_mut("test").unwrap();
        let empty_sv = StateVector::default();

        group.bench_function(ds.name, |b| {
            b.iter(|| tx.diff_update(&empty_sv).unwrap());
        });

        tx.commit(None).unwrap();
    }

    group.finish();
}

/// Benchmark replaying a sequential editing trace (insert/delete operations
/// on a Text type) and committing the result.
fn bench_editing_trace(c: &mut Criterion) {
    let traces: Vec<(&str, TestData)> = [
        (
            "friendsforever",
            "./tests/test-data/editing-traces/sequential_traces/friendsforever_flat.json.gz",
        ),
        (
            "sveltecomponent",
            "./tests/test-data/editing-traces/sequential_traces/sveltecomponent.json.gz",
        ),
    ]
    .into_iter()
    .filter_map(|(name, path)| {
        std::fs::metadata(path)
            .ok()
            .map(|_| (name, load_testing_data(path)))
    })
    .collect();

    let mut group = c.benchmark_group("editing_trace");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for (name, data) in &traces {
        group.bench_function(*name, |b| {
            b.iter_batched(
                TestEnv::new,
                |env| {
                    let mut tx = env.mdoc.transact_mut("test").unwrap();
                    let txt: Unmounted<Text> = Unmounted::root("text");
                    {
                        let mut txt = txt.mount_mut(&mut tx).unwrap();
                        for t in &data.txns {
                            for patch in &t.patches {
                                if patch.1 != 0 {
                                    txt.remove_range(patch.0..(patch.0 + patch.1)).unwrap();
                                }
                                if !patch.2.is_empty() {
                                    txt.insert(patch.0, &patch.2).unwrap();
                                }
                            }
                        }
                    }
                    tx.commit(None).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Same as `bench_apply_and_commit` but with `ENV_NOSYNC` — no fsync on commit.
fn bench_apply_and_commit_nosync(c: &mut Criterion) {
    let datasets = load_bin_datasets();
    let mut group = c.benchmark_group("apply_and_commit_nosync");
    group.sample_size(10);

    for ds in &datasets {
        group.bench_with_input(BenchmarkId::from_parameter(ds.name), ds, |b, ds| {
            b.iter_batched(
                TestEnv::nosync,
                |env| {
                    let mut tx = env.mdoc.transact_mut("test").unwrap();
                    ds.apply(&mut tx);
                    tx.commit(None).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Same as `bench_editing_trace` but with `ENV_NOSYNC`.
fn bench_editing_trace_nosync(c: &mut Criterion) {
    let traces: Vec<(&str, TestData)> = [
        (
            "friendsforever",
            "./tests/test-data/editing-traces/sequential_traces/friendsforever_flat.json.gz",
        ),
        (
            "sveltecomponent",
            "./tests/test-data/editing-traces/sequential_traces/sveltecomponent.json.gz",
        ),
    ]
    .into_iter()
    .filter_map(|(name, path)| {
        std::fs::metadata(path)
            .ok()
            .map(|_| (name, load_testing_data(path)))
    })
    .collect();

    let mut group = c.benchmark_group("editing_trace_nosync");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for (name, data) in &traces {
        group.bench_function(*name, |b| {
            b.iter_batched(
                TestEnv::nosync,
                |env| {
                    let mut tx = env.mdoc.transact_mut("test").unwrap();
                    let txt: Unmounted<Text> = Unmounted::root("text");
                    {
                        let mut txt = txt.mount_mut(&mut tx).unwrap();
                        for t in &data.txns {
                            for patch in &t.patches {
                                if patch.1 != 0 {
                                    txt.remove_range(patch.0..(patch.0 + patch.1)).unwrap();
                                }
                                if !patch.2.is_empty() {
                                    txt.insert(patch.0, &patch.2).unwrap();
                                }
                            }
                        }
                    }
                    tx.commit(None).unwrap();
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_apply_update,
    bench_apply_and_commit,
    bench_apply_and_commit_nosync,
    bench_incremental_update,
    bench_diff_update,
    bench_editing_trace,
    bench_editing_trace_nosync,
);
criterion_main!(benches);
