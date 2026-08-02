// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Reporting binary, same as the other benchmark targets.
#![allow(clippy::print_stdout)]

//! Matched-workload local benchmark for flat and Bε manifest layouts.
//!
//! THE Bε CONTENDER COMMITS THROUGH THE PRODUCTION DATASET PATH:
//! `lance::dataset::write::CommitBuilder::execute` with
//! `lance.manifest.layout=betree`, real `Operation::Append` /
//! `Operation::DataReplacement` transactions, and real `_transactions/` files.
//! Reintroducing `BeTreeDataset` or the `lance_table` research commit builder
//! as the contender is a bug (see devtools/betree/DATASET_WIRE.md).
//!
//! The flat contender stays the real full-manifest writer. Byte components
//! are split per commit from tracked PUT records: `_transactions/` and
//! `_bt/txn/` count as transaction bytes, `_bt/root/` objects decode to
//! either delta or compacted roots, and `_bt/node|leaf/` count as tree bytes.

use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance::dataset::betree::{
    MAX_CHILDREN_PER_NODE_KEY, MAX_LEAF_BYTES_KEY, MAX_NODE_BYTES_KEY, MAX_ROOT_DELTA_TAIL_KEY,
    Reader,
};
use lance::dataset::transaction::{DataReplacementGroup, Operation, Transaction};
use lance::dataset::write::CommitBuilder;
use lance_core::datatypes::Schema;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_table::betree::MANIFEST_LAYOUT_KEY;
use lance_table::betree::support::{
    make_backfill_data_file, make_fragment, make_replacement_data_file,
};
use lance_table::format::pb;
use lance_table::format::{DataFile, Fragment};
use object_store::path::Path;
use prost::Message;
use tokio::runtime::Runtime;
use uuid::Uuid;

use lance_table::betree::flat_baseline::{FlatBaseline, manifest_path};

const CSV_HEADER: &str = "scenario,layout,run,n,f,commits,budget_kib,locality,\
tree_bytes_avg,transaction_bytes_avg,total_bytes_avg,commit_ms_avg,commit_ms_p50,\
commit_ms_p95,flushes,open_read_ops,open_ms,resolve_read_ops_avg,resolve_ms_avg,\
resolve_ms_p50,resolve_ms_p95,materialize_read_ops,materialize_ms,cache_policy,\
delta_bytes_avg,folds,max_delta_tail\n";

/// Byte and IOP accounting uses the store's built-in tracker, which also
/// captures the local-filesystem writer fast path that bypasses the wrapped
/// `object_store` trait.
struct BenchStore {
    object_store: Arc<ObjectStore>,
    base: Path,
    handler: Arc<dyn lance_table::io::commit::CommitHandler>,
}

#[derive(Clone, Copy)]
enum Locality {
    Contiguous,
    Scattered,
}

impl Locality {
    fn from_env() -> Self {
        match env::var("AB_LOCALITY")
            .unwrap_or_else(|_| "contiguous".to_string())
            .as_str()
        {
            "contiguous" => Self::Contiguous,
            "scattered" => Self::Scattered,
            value => {
                panic!("invalid AB_LOCALITY={value:?}; expected \"contiguous\" or \"scattered\"")
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Contiguous => "contiguous",
            Self::Scattered => "scattered",
        }
    }
}

/// One commit's mutation, shared by both contenders. Data replacements carry
/// production `DataReplacement` semantics: a file matching on fields and file
/// version swaps in place, a disjoint-field file is an add-column append.
enum MutationPlan {
    Append(Box<Fragment>),
    DataReplacements(Vec<(u64, DataFile)>),
}

#[derive(Clone, Copy)]
struct MutationCase {
    scenario: &'static str,
    run: u64,
    n: u64,
    f: u64,
    commits: u64,
    budget_kib: u64,
    locality: Locality,
}

#[derive(Default)]
struct Record {
    scenario: &'static str,
    layout: &'static str,
    run: u64,
    n: u64,
    f: u64,
    commits: u64,
    budget_kib: u64,
    locality: &'static str,
    tree_bytes_avg: f64,
    transaction_bytes_avg: f64,
    total_bytes_avg: f64,
    commit_ms_avg: f64,
    commit_ms_p50: f64,
    commit_ms_p95: f64,
    flushes: u64,
    open_read_ops: u64,
    open_ms: f64,
    resolve_read_ops_avg: f64,
    resolve_ms_avg: f64,
    resolve_ms_p50: f64,
    resolve_ms_p95: f64,
    materialize_read_ops: u64,
    materialize_ms: f64,
    cache_policy: &'static str,
    delta_bytes_avg: f64,
    folds: u64,
    max_delta_tail: u32,
}

impl Record {
    fn csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},\
{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{},{:.3},{},{}",
            self.scenario,
            self.layout,
            self.run,
            self.n,
            self.f,
            self.commits,
            self.budget_kib,
            self.locality,
            self.tree_bytes_avg,
            self.transaction_bytes_avg,
            self.total_bytes_avg,
            self.commit_ms_avg,
            self.commit_ms_p50,
            self.commit_ms_p95,
            self.flushes,
            self.open_read_ops,
            self.open_ms,
            self.resolve_read_ops_avg,
            self.resolve_ms_avg,
            self.resolve_ms_p50,
            self.resolve_ms_p95,
            self.materialize_read_ops,
            self.materialize_ms,
            self.cache_policy,
            self.delta_bytes_avg,
            self.folds,
            self.max_delta_tail,
        )
    }
}

fn schema() -> Schema {
    Schema::try_from(&ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
    ]))
    .unwrap()
}

async fn store(uri: &str) -> BenchStore {
    let (object_store, base) = ObjectStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        uri,
        &ObjectStoreParams::default(),
    )
    .await
    .unwrap();
    let handler = lance_table::io::commit::commit_handler_from_url(uri, &None)
        .await
        .unwrap();
    BenchStore {
        object_store,
        base,
        handler,
    }
}

fn mutation_ids(commit: u64, f: u64, n: u64, locality: Locality) -> Vec<u64> {
    match locality {
        Locality::Contiguous => {
            let start = (commit * f) % n;
            (start..(start + f).min(n)).collect()
        }
        Locality::Scattered => (0..f)
            .map(|offset| (commit * f + offset).wrapping_mul(104_729).wrapping_add(17) % n)
            .collect(),
    }
}

/// Builds each commit's mutation before the timer starts. Stateful so
/// repeated replacements of the same fragment get unique paths and mixed
/// streams allocate the same fresh append ids for every layout.
struct MutationPlanner {
    case: MutationCase,
    replace_rounds: HashMap<u64, u32>,
    next_append_id: u64,
}

impl MutationPlanner {
    fn new(case: MutationCase) -> Self {
        Self {
            case,
            replace_rounds: HashMap::new(),
            next_append_id: case.n,
        }
    }

    fn plan(&mut self, commit: u64) -> MutationPlan {
        let case = self.case;
        match case.scenario {
            scenario if scenario.starts_with("AB-APPEND") => self.append(),
            "AB-REPLACE" => self.replace_files(commit),
            // 70% append, 25% F-fragment add-column, 5% selective replace.
            "AB-MIXED" => match commit % 20 {
                0..=13 => self.append(),
                14..=18 => self.add_files(commit),
                _ => self.replace_files(commit),
            },
            _ => self.add_files(commit),
        }
    }

    fn append(&mut self) -> MutationPlan {
        let fragment = make_fragment(self.next_append_id);
        self.next_append_id += 1;
        MutationPlan::Append(Box::new(fragment))
    }

    fn add_files(&mut self, commit: u64) -> MutationPlan {
        let case = self.case;
        MutationPlan::DataReplacements(
            mutation_ids(commit, case.f, case.n, case.locality)
                .into_iter()
                .map(|fragment_id| {
                    (
                        fragment_id,
                        make_backfill_data_file(fragment_id, commit as u32),
                    )
                })
                .collect(),
        )
    }

    fn replace_files(&mut self, commit: u64) -> MutationPlan {
        let case = self.case;
        MutationPlan::DataReplacements(
            mutation_ids(commit, case.f, case.n, case.locality)
                .into_iter()
                .map(|fragment_id| {
                    let round = self.replace_rounds.entry(fragment_id).or_insert(0);
                    let replacement = make_replacement_data_file(fragment_id, *round);
                    *round += 1;
                    (fragment_id, replacement)
                })
                .collect(),
        )
    }
}

fn latency_summary(samples: &[f64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let average = samples.iter().sum::<f64>() / samples.len() as f64;
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let percentile = |fraction: f64| {
        let rank = (sorted.len() as f64 * fraction).ceil() as usize;
        sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
    };
    (average, percentile(0.50), percentile(0.95))
}

async fn flat_mutation(uri: &str, case: MutationCase) -> Record {
    let store = store(uri).await;
    let mut flat = FlatBaseline::new(
        store.object_store.clone(),
        store.base.clone(),
        schema(),
        (0..case.n).map(make_fragment).collect(),
    );
    flat.write().await.unwrap();
    let mut planner = MutationPlanner::new(case);
    let mut total_bytes = 0u64;
    let mut latencies = Vec::with_capacity(case.commits as usize);
    for commit in 0..case.commits {
        let plan = planner.plan(commit);
        let previous_version = flat.version();
        let started = Instant::now();
        let bytes = match plan {
            MutationPlan::Append(fragment) => flat.commit_append(*fragment).await.unwrap(),
            MutationPlan::DataReplacements(replacements) => {
                flat.commit_data_replacements(&replacements).await.unwrap()
            }
        };
        latencies.push(started.elapsed().as_secs_f64() * 1e3);
        total_bytes += bytes;
        store
            .object_store
            .delete(&manifest_path(&store.base, previous_version))
            .await
            .unwrap();
    }
    let (commit_ms_avg, commit_ms_p50, commit_ms_p95) = latency_summary(&latencies);
    let tree_bytes_avg = total_bytes as f64 / case.commits.max(1) as f64;

    // Post-stream reads: flat's open is eager (whole manifest), so a resolve
    // after open is a pure in-memory search with zero additional read ops.
    store.object_store.io_stats_incremental();
    let started = Instant::now();
    let manifest = FlatBaseline::cold_open(&store.object_store, &store.base, flat.version())
        .await
        .unwrap();
    let open_ms = started.elapsed().as_secs_f64() * 1e3;
    let open_read_ops = store.object_store.io_stats_incremental().read_iops;
    let mut resolve_latencies = Vec::with_capacity(100);
    for index in 0..100 {
        let fragment_id = (index * 104_729 + 17) % case.n;
        let started = Instant::now();
        assert!(
            manifest
                .fragments
                .binary_search_by_key(&fragment_id, |fragment| fragment.id)
                .is_ok()
        );
        resolve_latencies.push(started.elapsed().as_secs_f64() * 1e3);
    }
    let resolve_read_ops = store.object_store.io_stats_incremental().read_iops;
    let (resolve_ms_avg, resolve_ms_p50, resolve_ms_p95) = latency_summary(&resolve_latencies);

    Record {
        scenario: case.scenario,
        layout: "flat",
        run: case.run,
        n: case.n,
        f: case.f,
        commits: case.commits,
        budget_kib: case.budget_kib,
        locality: case.locality.label(),
        tree_bytes_avg,
        total_bytes_avg: tree_bytes_avg,
        commit_ms_avg,
        commit_ms_p50,
        commit_ms_p95,
        open_read_ops,
        open_ms,
        materialize_read_ops: open_read_ops,
        materialize_ms: open_ms,
        resolve_read_ops_avg: resolve_read_ops as f64 / 100.0,
        resolve_ms_avg,
        resolve_ms_p50,
        resolve_ms_p95,
        cache_policy: "disabled",
        ..Default::default()
    }
}

/// Per-commit write-byte components, split from tracked PUT records.
#[derive(Default)]
struct WriteSplit {
    tree_bytes: u64,
    transaction_bytes: u64,
    delta_bytes: u64,
    folds: u64,
}

fn split_writes(store: &BenchStore) -> WriteSplit {
    let mut split = WriteSplit::default();
    for request in store.object_store.io_stats_incremental().requests {
        if !request.method.starts_with("put") {
            continue;
        }
        let path = request.path.to_string();
        let bytes = object_size(&path);
        if path.contains("/_transactions/") || path.contains("/_bt/txn/") {
            split.transaction_bytes += bytes;
        } else if path.contains("/_bt/root/") {
            let root = pb::BeTreeRoot::decode(read_object(&path).as_slice()).unwrap();
            if root.base_root_version != 0 {
                split.delta_bytes += bytes;
            } else {
                split.tree_bytes += bytes;
                split.folds += 1;
            }
        } else if path.contains("/_bt/") {
            split.tree_bytes += bytes;
        } else {
            panic!("unexpected write outside the Bε layout: {path}");
        }
    }
    split
}

fn read_object(object_path: &str) -> Vec<u8> {
    std::fs::read(format!("/{object_path}"))
        .or_else(|_| std::fs::read(object_path))
        .unwrap()
}

fn object_size(object_path: &str) -> u64 {
    std::fs::metadata(format!("/{object_path}"))
        .or_else(|_| std::fs::metadata(object_path))
        .map(|meta| meta.len())
        .unwrap()
}

async fn betree_mutation(uri: &str, case: MutationCase, config: HashMap<String, String>) -> Record {
    let store = store(uri).await;
    let mut dataset = Arc::new(
        CommitBuilder::new(uri)
            .with_object_store(store.object_store.clone())
            .with_commit_handler(store.handler.clone())
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments: (0..case.n).map(make_fragment).collect(),
                    schema: schema(),
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap(),
    );
    let mut planner = MutationPlanner::new(case);
    let mut split_total = WriteSplit::default();
    let mut latencies = Vec::with_capacity(case.commits as usize);
    let mut delta_tail = 0u32;
    let mut max_delta_tail = 0u32;
    store.object_store.io_stats_incremental();
    for commit in 0..case.commits {
        let plan = planner.plan(commit);
        let operation = match plan {
            MutationPlan::Append(fragment) => Operation::Append {
                fragments: vec![*fragment],
            },
            MutationPlan::DataReplacements(replacements) => Operation::DataReplacement {
                replacements: replacements
                    .into_iter()
                    .map(|(fragment_id, file)| DataReplacementGroup(fragment_id, file))
                    .collect(),
            },
        };
        let transaction = Transaction::new_from_version(dataset.manifest.version, operation);
        let started = Instant::now();
        let committed = CommitBuilder::new(dataset.clone())
            .execute(transaction)
            .await
            .unwrap();
        latencies.push(started.elapsed().as_secs_f64() * 1e3);
        dataset = Arc::new(committed);
        let split = split_writes(&store);
        if split.folds > 0 {
            delta_tail = 0;
        } else if split.delta_bytes > 0 {
            delta_tail += 1;
            max_delta_tail = max_delta_tail.max(delta_tail);
        }
        split_total.tree_bytes += split.tree_bytes;
        split_total.transaction_bytes += split.transaction_bytes;
        split_total.delta_bytes += split.delta_bytes;
        split_total.folds += split.folds;
    }
    let (commit_ms_avg, commit_ms_p50, commit_ms_p95) = latency_summary(&latencies);
    let commit_count = case.commits.max(1) as f64;
    let tree_bytes_avg = split_total.tree_bytes as f64 / commit_count;
    let transaction_bytes_avg = split_total.transaction_bytes as f64 / commit_count;
    let delta_bytes_avg = split_total.delta_bytes as f64 / commit_count;
    drop(dataset);

    // Post-stream reads: lazy reopen plus 100 uncached point resolves.
    store.object_store.io_stats_incremental();
    let started = Instant::now();
    let reader = Reader::open(store.object_store.clone(), store.base.clone())
        .await
        .unwrap();
    let open_ms = started.elapsed().as_secs_f64() * 1e3;
    let open_read_ops = store.object_store.io_stats_incremental().read_iops;
    let mut resolve_read_ops = 0u64;
    let mut resolve_latencies = Vec::with_capacity(100);
    for index in 0..100 {
        let fragment_id = (index * 104_729 + 17) % case.n;
        let started = Instant::now();
        assert!(
            reader
                .resolve_fragment(fragment_id)
                .await
                .unwrap()
                .is_some()
        );
        resolve_latencies.push(started.elapsed().as_secs_f64() * 1e3);
        resolve_read_ops += store.object_store.io_stats_incremental().read_iops;
    }
    let (resolve_ms_avg, resolve_ms_p50, resolve_ms_p95) = latency_summary(&resolve_latencies);

    Record {
        scenario: case.scenario,
        layout: "betree",
        run: case.run,
        n: case.n,
        f: case.f,
        commits: case.commits,
        budget_kib: case.budget_kib,
        locality: case.locality.label(),
        tree_bytes_avg,
        transaction_bytes_avg,
        delta_bytes_avg,
        folds: split_total.folds,
        max_delta_tail,
        total_bytes_avg: tree_bytes_avg + transaction_bytes_avg + delta_bytes_avg,
        commit_ms_avg,
        commit_ms_p50,
        commit_ms_p95,
        open_read_ops,
        open_ms,
        resolve_read_ops_avg: resolve_read_ops as f64 / 100.0,
        resolve_ms_avg,
        resolve_ms_p50,
        resolve_ms_p95,
        cache_policy: "disabled",
        ..Default::default()
    }
}

async fn betree_read(
    uri: &str,
    run: u64,
    n: u64,
    budget_kib: u64,
    config: HashMap<String, String>,
) -> [Record; 2] {
    let case = MutationCase {
        scenario: "AB-OPEN",
        run,
        n,
        f: 1,
        commits: 100,
        budget_kib,
        locality: Locality::Contiguous,
    };
    let store = store(uri).await;
    let mut dataset = Arc::new(
        CommitBuilder::new(uri)
            .with_object_store(store.object_store.clone())
            .with_commit_handler(store.handler.clone())
            .execute(Transaction::new_from_version(
                0,
                Operation::Overwrite {
                    fragments: (0..n).map(make_fragment).collect(),
                    schema: schema(),
                    config_upsert_values: Some(config),
                    initial_bases: None,
                },
            ))
            .await
            .unwrap(),
    );
    for commit in 0..case.commits {
        let committed = CommitBuilder::new(dataset.clone())
            .execute(Transaction::new_from_version(
                dataset.manifest.version,
                Operation::Append {
                    fragments: vec![make_fragment(n + commit)],
                },
            ))
            .await
            .unwrap();
        dataset = Arc::new(committed);
    }
    drop(dataset);

    store.object_store.io_stats_incremental();
    let started = Instant::now();
    let reader = Reader::open(store.object_store.clone(), store.base.clone())
        .await
        .unwrap();
    let open_ms = started.elapsed().as_secs_f64() * 1e3;
    let open_read_ops = store.object_store.io_stats_incremental().read_iops;

    let started = Instant::now();
    let materialized = reader.materialize().await.unwrap();
    let materialize_ms = started.elapsed().as_secs_f64() * 1e3;
    let materialize_read_ops = store.object_store.io_stats_incremental().read_iops;
    assert_eq!(materialized.len() as u64, n + 100);

    let resolver = Reader::open(store.object_store.clone(), store.base.clone())
        .await
        .unwrap();
    store.object_store.io_stats_incremental();
    let mut resolve_read_ops = 0u64;
    let mut resolve_latencies = Vec::with_capacity(100);
    for index in 0..100 {
        let fragment_id = (index * 104_729 + 17) % n;
        let started = Instant::now();
        assert!(
            resolver
                .resolve_fragment(fragment_id)
                .await
                .unwrap()
                .is_some()
        );
        resolve_latencies.push(started.elapsed().as_secs_f64() * 1e3);
        resolve_read_ops += store.object_store.io_stats_incremental().read_iops;
    }
    let (resolve_ms_avg, resolve_ms_p50, resolve_ms_p95) = latency_summary(&resolve_latencies);

    [
        Record {
            scenario: "AB-OPEN",
            layout: "betree",
            run,
            n,
            f: 1,
            commits: 100,
            budget_kib,
            locality: "append",
            open_read_ops,
            open_ms,
            materialize_read_ops,
            materialize_ms,
            cache_policy: "disabled",
            ..Default::default()
        },
        Record {
            scenario: "AB-RESOLVE",
            layout: "betree",
            run,
            n,
            f: 1,
            commits: 100,
            budget_kib,
            locality: "deterministic",
            resolve_read_ops_avg: resolve_read_ops as f64 / 100.0,
            resolve_ms_avg,
            resolve_ms_p50,
            resolve_ms_p95,
            cache_policy: "disabled",
            ..Default::default()
        },
    ]
}

async fn flat_read(uri: &str, run: u64, n: u64, budget_kib: u64) -> [Record; 2] {
    let store = store(uri).await;
    let mut flat = FlatBaseline::new(
        store.object_store.clone(),
        store.base.clone(),
        schema(),
        (0..n).map(make_fragment).collect(),
    );
    flat.write().await.unwrap();
    for commit in 0..100 {
        let previous_version = flat.version();
        flat.commit_append(make_fragment(n + commit)).await.unwrap();
        store
            .object_store
            .delete(&manifest_path(&store.base, previous_version))
            .await
            .unwrap();
    }

    store.object_store.io_stats_incremental();
    let started = Instant::now();
    let manifest = FlatBaseline::cold_open(&store.object_store, &store.base, flat.version())
        .await
        .unwrap();
    let open_ms = started.elapsed().as_secs_f64() * 1e3;
    let open_read_ops = store.object_store.io_stats_incremental().read_iops;

    let mut resolve_latencies = Vec::with_capacity(100);
    for index in 0..100 {
        let fragment_id = (index * 104_729 + 17) % n;
        let started = Instant::now();
        assert!(
            manifest
                .fragments
                .binary_search_by_key(&fragment_id, |fragment| fragment.id)
                .is_ok()
        );
        resolve_latencies.push(started.elapsed().as_secs_f64() * 1e3);
    }
    let resolve_read_ops = store.object_store.io_stats_incremental().read_iops;
    let (resolve_ms_avg, resolve_ms_p50, resolve_ms_p95) = latency_summary(&resolve_latencies);

    [
        Record {
            scenario: "AB-OPEN",
            layout: "flat",
            run,
            n,
            f: 1,
            commits: 100,
            budget_kib,
            locality: "append",
            open_read_ops,
            open_ms,
            materialize_read_ops: open_read_ops,
            materialize_ms: open_ms,
            cache_policy: "disabled",
            ..Default::default()
        },
        Record {
            scenario: "AB-RESOLVE",
            layout: "flat",
            run,
            n,
            f: 1,
            commits: 100,
            budget_kib,
            locality: "deterministic",
            resolve_read_ops_avg: resolve_read_ops as f64 / 100.0,
            resolve_ms_avg,
            resolve_ms_p50,
            resolve_ms_p95,
            cache_policy: "disabled",
            ..Default::default()
        },
    ]
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn emit(csv_path: &str, records: &[Record]) {
    let exists = std::path::Path::new(csv_path).exists();
    let mut csv = OpenOptions::new()
        .create(true)
        .append(true)
        .open(csv_path)
        .unwrap();
    if !exists {
        csv.write_all(CSV_HEADER.as_bytes()).unwrap();
    }
    for record in records {
        writeln!(csv, "{}", record.csv_line()).unwrap();
        println!("{}", record.csv_line());
    }
}

fn main() {
    let runtime = Runtime::new().unwrap();
    let n = env_u64("NUM_FRAGMENTS", 50_000);
    let repeats = env_u64("AB_REPEATS", 1);
    let append_commits = env_u64("AB_APPEND_COMMITS", 100);
    let steady_append_commits = env_u64("AB_STEADY_APPEND_COMMITS", 5_000);
    let trickle_commits = env_u64("AB_TRICKLE_COMMITS", 500);
    let mixed_commits = env_u64("AB_MIXED_COMMITS", 1_000);
    let budget_kib = env_u64("AB_NODE_SIZE_KIB", 128);
    let leaf_budget_kib = env_u64("AB_LEAF_SIZE_KIB", budget_kib);
    let fanout = env_u64("FANOUT", 16);
    let root_delta_tail = env_u64("AB_ROOT_DELTA_TAIL", 0);
    let locality = Locality::from_env();
    let config: HashMap<String, String> = HashMap::from([
        (
            MANIFEST_LAYOUT_KEY.to_string(),
            lance_table::betree::MANIFEST_LAYOUT_BETREE.to_string(),
        ),
        (
            MAX_NODE_BYTES_KEY.to_string(),
            (budget_kib * 1024).to_string(),
        ),
        (
            MAX_LEAF_BYTES_KEY.to_string(),
            (leaf_budget_kib * 1024).to_string(),
        ),
        (MAX_CHILDREN_PER_NODE_KEY.to_string(), fanout.to_string()),
        (
            MAX_ROOT_DELTA_TAIL_KEY.to_string(),
            root_delta_tail.to_string(),
        ),
    ]);
    let scenarios =
        env::var("AB_SCENARIOS").unwrap_or_else(|_| "append,trickle,oneshot,read".to_string());
    let selected = |scenario: &str| scenarios.split(',').any(|value| value == scenario);
    let csv_path = env::var("AB_CSV").unwrap_or_else(|_| "/tmp/betree_ab_dataset.csv".to_string());
    let root = env::temp_dir().join(format!("betree_ab_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let root = root.to_string_lossy();
    let uri = |name: &str, run: u64| format!("{root}/{name}_{run}");

    let records = runtime.block_on(async {
        let mut records = Vec::new();
        for run in 1..=repeats {
            if selected("append") {
                let case = MutationCase {
                    scenario: "AB-APPEND",
                    run,
                    n,
                    f: 1,
                    commits: append_commits,
                    budget_kib,
                    locality,
                };
                records.push(flat_mutation(&uri("append_flat", run), case).await);
                records
                    .push(betree_mutation(&uri("append_betree", run), case, config.clone()).await);
            }
            if selected("append_steady") {
                let case = MutationCase {
                    scenario: "AB-APPEND-STEADY",
                    run,
                    n,
                    f: 1,
                    commits: steady_append_commits,
                    budget_kib,
                    locality,
                };
                records.push(flat_mutation(&uri("append_steady_flat", run), case).await);
                records.push(
                    betree_mutation(&uri("append_steady_betree", run), case, config.clone()).await,
                );
            }
            if selected("trickle") {
                let case = MutationCase {
                    scenario: "AB-TRICKLE",
                    run,
                    n,
                    f: 10,
                    commits: trickle_commits,
                    budget_kib,
                    locality,
                };
                records.push(flat_mutation(&uri("trickle_flat", run), case).await);
                records
                    .push(betree_mutation(&uri("trickle_betree", run), case, config.clone()).await);
            }
            if selected("replace") {
                let case = MutationCase {
                    scenario: "AB-REPLACE",
                    run,
                    n,
                    f: 10,
                    commits: trickle_commits,
                    budget_kib,
                    locality,
                };
                records.push(flat_mutation(&uri("replace_flat", run), case).await);
                records
                    .push(betree_mutation(&uri("replace_betree", run), case, config.clone()).await);
            }
            if selected("mixed") {
                let case = MutationCase {
                    scenario: "AB-MIXED",
                    run,
                    n,
                    f: 10,
                    commits: mixed_commits,
                    budget_kib,
                    locality,
                };
                records.push(flat_mutation(&uri("mixed_flat", run), case).await);
                records
                    .push(betree_mutation(&uri("mixed_betree", run), case, config.clone()).await);
            }
            if selected("oneshot") {
                let case = MutationCase {
                    scenario: "AB-ONESHOT",
                    run,
                    n,
                    f: n,
                    commits: 1,
                    budget_kib,
                    locality: Locality::Contiguous,
                };
                records.push(flat_mutation(&uri("oneshot_flat", run), case).await);
                records
                    .push(betree_mutation(&uri("oneshot_betree", run), case, config.clone()).await);
            }
            if selected("read") {
                records.extend(flat_read(&uri("read_flat", run), run, n, budget_kib).await);
                records.extend(
                    betree_read(&uri("read_betree", run), run, n, budget_kib, config.clone()).await,
                );
            }
        }
        records
    });
    emit(&csv_path, &records);
    std::fs::remove_dir_all(root.as_ref()).unwrap();
}
