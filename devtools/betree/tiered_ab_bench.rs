// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Reporting binary, same as the branch's tiered_manifest_bench example.
#![allow(clippy::print_stdout)]

//! Companion matched-workload harness for the `tiered-manifest` branch.
//!
//! Copy this file to `rust/lance/examples/tiered_ab_bench.rs` in a dedicated
//! worktree at commit 61066f586, apply `tiered_ab_public_resolve.patch`, and
//! run the commands recorded in `AB_BENCH.md`.

use std::env;
use std::fs::{OpenOptions, read_dir};
use std::io::Write;
use std::num::NonZero;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance::Dataset;
use lance::dataset::CommitBuilder;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance_core::datatypes::Schema;
use lance_table::format::{
    DataFile, Fragment, MANIFEST_BUFFER_CAP_KEY, MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED,
    MANIFEST_MAX_FANOUT_KEY, MANIFEST_MESSAGE_BYTE_CAP_KEY,
};

const CSV_HEADER: &str = "scenario,layout,run,n,f,commits,budget_kib,locality,\
tree_bytes_avg,transaction_bytes_avg,total_bytes_avg,commit_ms_avg,commit_ms_p50,\
commit_ms_p95,flushes,open_read_ops,open_ms,resolve_read_ops_avg,resolve_ms_avg,\
resolve_ms_p50,resolve_ms_p95,materialize_read_ops,materialize_ms,cache_policy\n";

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

enum MutationPlan {
    Append(Box<Fragment>),
    AddFiles(Vec<(u64, DataFile)>),
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
}

impl Record {
    fn csv_line(&self) -> String {
        format!(
            "{},tiered,{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},\
{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.3},{}",
            self.scenario,
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
        )
    }
}

#[derive(Clone, Copy, Default)]
struct MetaSnapshot {
    tree_bytes: u64,
    transaction_bytes: u64,
}

impl MetaSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            tree_bytes: self.tree_bytes.saturating_sub(before.tree_bytes),
            transaction_bytes: self
                .transaction_bytes
                .saturating_sub(before.transaction_bytes),
        }
    }
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    read_dir(path)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.metadata().ok())
                .map(|metadata| metadata.len())
                .sum()
        })
        .unwrap_or_default()
}

fn metadata_snapshot(root: &std::path::Path) -> MetaSnapshot {
    MetaSnapshot {
        tree_bytes: directory_bytes(&root.join("_versions"))
            + directory_bytes(&root.join("_manifest_children")),
        transaction_bytes: directory_bytes(&root.join("_transactions")),
    }
}

fn remove_all_but_newest(path: &std::path::Path) {
    let mut entries = read_dir(path)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| {
        entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
    });
    entries.reverse();
    for old in entries.into_iter().skip(1) {
        std::fs::remove_file(old.path()).unwrap();
    }
}

fn prune_history(root: &std::path::Path) {
    remove_all_but_newest(&root.join("_versions"));
    remove_all_but_newest(&root.join("_transactions"));
}

fn data_file_path(id: u64, salt: u64) -> String {
    let mut bytes = [0u8; 16];
    let mut state = id
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(salt.wrapping_mul(0xD1B5_4A32_D192_ED03));
    for chunk in bytes.chunks_mut(8) {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^= value >> 31;
        let len = chunk.len();
        chunk.copy_from_slice(&value.to_le_bytes()[..len]);
    }

    let mut stem = String::with_capacity(50);
    for &byte in &bytes[..3] {
        for bit in (0..8).rev() {
            stem.push(if (byte >> bit) & 1 == 1 { '1' } else { '0' });
        }
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in &bytes[3..] {
        stem.push(HEX[(byte >> 4) as usize] as char);
        stem.push(HEX[(byte & 0x0f) as usize] as char);
    }
    format!("data/{stem}.lance")
}

fn fragment(id: u64) -> Fragment {
    let mut fragment = Fragment::new(id).with_physical_rows(1);
    fragment.files.push(DataFile::new(
        data_file_path(id, 0),
        vec![0, 1],
        vec![0, 1],
        2,
        0,
        NonZero::new(1024),
        None,
    ));
    fragment
}

fn backfill_file(fragment_id: u64, column: u32) -> DataFile {
    DataFile::new(
        data_file_path(fragment_id, u64::from(column) + 1),
        vec![2 + column as i32],
        vec![0],
        2,
        0,
        NonZero::new(4096),
        None,
    )
}

fn schema() -> Schema {
    Schema::try_from(&ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("name", DataType::Utf8, false),
    ]))
    .unwrap()
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

fn mutation_plan(scenario: &str, commit: u64, f: u64, n: u64, locality: Locality) -> MutationPlan {
    if scenario.starts_with("AB-APPEND") {
        MutationPlan::Append(Box::new(fragment(n + commit)))
    } else {
        MutationPlan::AddFiles(
            mutation_ids(commit, f, n, locality)
                .into_iter()
                .map(|fragment_id| (fragment_id, backfill_file(fragment_id, commit as u32)))
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

async fn install(root: &std::path::Path, n: u64, budget_kib: u64) -> Dataset {
    let uri = root.to_str().unwrap();
    let transaction = Transaction::new_from_version(
        0,
        Operation::Overwrite {
            fragments: (0..n).map(fragment).collect(),
            schema: schema(),
            config_upsert_values: None,
            initial_bases: None,
        },
    );
    let mut dataset = CommitBuilder::new(uri).execute(transaction).await.unwrap();
    let buffer_cap = env_u64("AB_TIERED_BUFFER_CAP", (n / 10).max(1)).to_string();
    let message_byte_cap = (budget_kib * 1024).to_string();
    let fanout = env_u64("FANOUT", 16).to_string();
    dataset
        .update_config([
            (MANIFEST_LAYOUT_KEY, MANIFEST_LAYOUT_TIERED),
            (MANIFEST_BUFFER_CAP_KEY, buffer_cap.as_str()),
            (MANIFEST_MESSAGE_BYTE_CAP_KEY, message_byte_cap.as_str()),
            (MANIFEST_MAX_FANOUT_KEY, fanout.as_str()),
        ])
        .await
        .unwrap();
    prune_history(root);
    dataset
}

async fn execute_plan(dataset: Dataset, plan: MutationPlan) -> Dataset {
    let version = dataset.version().version;
    let operation = match plan {
        MutationPlan::Append(fragment) => Operation::Append {
            fragments: vec![*fragment],
        },
        MutationPlan::AddFiles(files) => {
            let mut fragments = dataset.fragments().as_ref().clone();
            for (fragment_id, file) in files {
                fragments[fragment_id as usize].files.push(file);
            }
            Operation::Merge {
                fragments,
                schema: dataset.schema().clone(),
            }
        }
    };
    let transaction = Transaction::new_from_version(version, operation);
    CommitBuilder::new(Arc::new(dataset))
        .execute(transaction)
        .await
        .unwrap()
}

async fn mutation(root: &std::path::Path, case: MutationCase) -> Record {
    let mut dataset = install(root, case.n, case.budget_kib).await;
    let mut tree_bytes = 0u64;
    let mut transaction_bytes = 0u64;
    let mut latencies = Vec::with_capacity(case.commits as usize);
    for commit in 0..case.commits {
        let plan = mutation_plan(case.scenario, commit, case.f, case.n, case.locality);
        let before = metadata_snapshot(root);
        let started = Instant::now();
        dataset = execute_plan(dataset, plan).await;
        latencies.push(started.elapsed().as_secs_f64() * 1e3);
        let delta = metadata_snapshot(root).delta(before);
        tree_bytes += delta.tree_bytes;
        transaction_bytes += delta.transaction_bytes;
        prune_history(root);
    }
    let commits = case.commits.max(1);
    let tree_bytes_avg = tree_bytes as f64 / commits as f64;
    let transaction_bytes_avg = transaction_bytes as f64 / commits as f64;
    let (commit_ms_avg, commit_ms_p50, commit_ms_p95) = latency_summary(&latencies);
    Record {
        scenario: case.scenario,
        run: case.run,
        n: case.n,
        f: case.f,
        commits,
        budget_kib: case.budget_kib,
        locality: case.locality.label(),
        tree_bytes_avg,
        transaction_bytes_avg,
        total_bytes_avg: tree_bytes_avg + transaction_bytes_avg,
        commit_ms_avg,
        commit_ms_p50,
        commit_ms_p95,
        cache_policy: "disabled",
        ..Default::default()
    }
}

async fn open_without_cache(root: &std::path::Path) -> Dataset {
    DatasetBuilder::from_uri(root.to_str().unwrap())
        .with_index_cache_size_bytes(0)
        .with_metadata_cache_size_bytes(0)
        .load()
        .await
        .unwrap()
}

async fn read_metrics(root: &std::path::Path, run: u64, n: u64, budget_kib: u64) -> [Record; 2] {
    let mut dataset = install(root, n, budget_kib).await;
    for commit in 0..100 {
        dataset = execute_plan(
            dataset,
            MutationPlan::Append(Box::new(fragment(n + commit))),
        )
        .await;
        prune_history(root);
    }
    drop(dataset);

    let started = Instant::now();
    let reader = open_without_cache(root).await;
    let open_ms = started.elapsed().as_secs_f64() * 1e3;
    let object_store = reader.object_store(None).await.unwrap();
    let open_read_ops = object_store.io_stats_incremental().read_iops;

    let started = Instant::now();
    assert_eq!(reader.get_fragments().len() as u64, n + 100);
    let materialize_ms = started.elapsed().as_secs_f64() * 1e3;
    let materialize_read_ops = object_store.io_stats_incremental().read_iops;
    drop(reader);
    drop(object_store);

    let resolver = open_without_cache(root).await;
    let resolve_store = resolver.object_store(None).await.unwrap();
    resolve_store.io_stats_incremental();
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
        resolve_read_ops += resolve_store.io_stats_incremental().read_iops;
    }
    let (resolve_ms_avg, resolve_ms_p50, resolve_ms_p95) = latency_summary(&resolve_latencies);

    [
        Record {
            scenario: "AB-OPEN",
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

fn emit(path: &str, records: &[Record]) {
    let exists = std::path::Path::new(path).exists();
    let mut csv = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    if !exists {
        csv.write_all(CSV_HEADER.as_bytes()).unwrap();
    }
    for record in records {
        writeln!(csv, "{}", record.csv_line()).unwrap();
        println!("{}", record.csv_line());
    }
}

#[tokio::main]
pub async fn main() {
    let n = env_u64("NUM_FRAGMENTS", 50_000);
    let repeats = env_u64("AB_REPEATS", 1);
    let append_commits = env_u64("AB_APPEND_COMMITS", 100);
    let steady_append_commits = env_u64("AB_STEADY_APPEND_COMMITS", 5_000);
    let trickle_commits = env_u64("AB_TRICKLE_COMMITS", 500);
    let budget_kib = env_u64("AB_NODE_SIZE_KIB", 128);
    let locality = Locality::from_env();
    let scenarios =
        env::var("AB_SCENARIOS").unwrap_or_else(|_| "append,trickle,oneshot,read".to_string());
    let selected = |scenario: &str| scenarios.split(',').any(|value| value == scenario);
    let csv_path = env::var("AB_CSV").unwrap_or_else(|_| "/tmp/tiered_ab_v2.csv".to_string());
    let tempdir = tempfile::tempdir().unwrap();
    let root = |name: &str, run: u64| tempdir.path().join(format!("{name}_{run}"));
    let mut records = Vec::new();

    for run in 1..=repeats {
        if selected("append") {
            records.push(
                mutation(
                    &root("append", run),
                    MutationCase {
                        scenario: "AB-APPEND",
                        run,
                        n,
                        f: 1,
                        commits: append_commits,
                        budget_kib,
                        locality,
                    },
                )
                .await,
            );
        }
        if selected("append_steady") {
            records.push(
                mutation(
                    &root("append_steady", run),
                    MutationCase {
                        scenario: "AB-APPEND-STEADY",
                        run,
                        n,
                        f: 1,
                        commits: steady_append_commits,
                        budget_kib,
                        locality,
                    },
                )
                .await,
            );
        }
        if selected("trickle") {
            records.push(
                mutation(
                    &root("trickle", run),
                    MutationCase {
                        scenario: "AB-TRICKLE",
                        run,
                        n,
                        f: 10,
                        commits: trickle_commits,
                        budget_kib,
                        locality,
                    },
                )
                .await,
            );
        }
        if selected("oneshot") {
            records.push(
                mutation(
                    &root("oneshot", run),
                    MutationCase {
                        scenario: "AB-ONESHOT",
                        run,
                        n,
                        f: n,
                        commits: 1,
                        budget_kib,
                        locality: Locality::Contiguous,
                    },
                )
                .await,
            );
        }
        if selected("read") {
            records.extend(read_metrics(&root("read", run), run, n, budget_kib).await);
        }
    }
    emit(&csv_path, &records);
}
