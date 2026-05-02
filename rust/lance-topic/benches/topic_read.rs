// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Matrix benchmark for Lance topic consumer read throughput.
//!
//! ## Configuration
//!
//! - `DATASET_PREFIX`: Directory namespace root URI. If not set, uses a temporary local directory.
//! - `PAYLOAD_BYTES`: Approximate bytes in each JSON payload body string (default: `256`).
//! - `REPEATS`: Repeated measurements per scenario (default: `3`).
//! - `SCHEMA_MODE`: `default` (id + payload) or `custom` (event_id + user_id + score + 1024-dim embedding). Default: `default`.
//! - `READ_CASES`: Optional explicit cases as `name:producers:rows:write_batch_size:poll_entries` separated by `;`.
//! - `RESULT_CSV`: Optional output CSV file path.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use futures::future::try_join_all;
use lance_core::{Error, Result};
use lance_topic::{Producer, Topic, TopicBatch, WalTailer};
use serde_json::{Value, json};
use uuid::Uuid;

const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_REPEATS: usize = 3;
const EMBEDDING_DIM: i32 = 1024;
const DEFAULT_CASES: &[(&str, u32, usize, usize, usize)] = &[
    ("read_prod1_50k_poll32", 1, 50_000, 5_000, 32),
    ("read_prod1_200k_poll1", 1, 200_000, 5_000, 1),
    ("read_prod1_200k_poll8", 1, 200_000, 5_000, 8),
    ("read_prod1_200k_poll32", 1, 200_000, 5_000, 32),
    ("read_prod4_200k_poll32", 4, 200_000, 5_000, 32),
    ("read_prod8_500k_poll32", 8, 500_000, 5_000, 32),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaMode {
    Default,
    Custom,
}

impl SchemaMode {
    fn from_env() -> Self {
        match std::env::var("SCHEMA_MODE").unwrap_or_default().as_str() {
            "custom" => Self::Custom,
            _ => Self::Default,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Custom => "custom",
        }
    }
}

fn custom_user_schema() -> ArrowSchema {
    let pk_meta = std::collections::HashMap::from([(
        "lance-schema:unenforced-primary-key".to_string(),
        "true".to_string(),
    )]);
    ArrowSchema::new(vec![
        Field::new("event_id", DataType::Utf8, false).with_metadata(pk_meta),
        Field::new("user_id", DataType::Int64, false),
        Field::new("score", DataType::Float64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                EMBEDDING_DIM,
            ),
            true,
        ),
    ])
}

#[derive(Debug, Clone)]
struct ReadCase {
    name: String,
    producer_count: u32,
    rows: usize,
    write_batch_size: usize,
    poll_entries: usize,
}

#[derive(Debug, Clone)]
struct ReadMeasurement {
    case_name: String,
    schema_mode: String,
    producer_count: u32,
    rows: usize,
    write_batch_size: usize,
    poll_entries: usize,
    payload_bytes: usize,
    repeat: usize,
    elapsed: Duration,
    wal_entries_read: usize,
    arrow_batches_read: usize,
    polls: usize,
}

impl ReadMeasurement {
    fn rows_per_second(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64()
    }

    fn csv_header() -> &'static str {
        "benchmark,case,schema_mode,producer_count,rows,write_batch_size,poll_entries,payload_bytes,repeat,elapsed_seconds,rows_per_second,wal_entries_read,arrow_batches_read,polls"
    }

    fn csv_row(&self) -> String {
        format!(
            "read,{},{},{},{},{},{},{},{},{:.6},{:.3},{},{},{}",
            self.case_name,
            self.schema_mode,
            self.producer_count,
            self.rows,
            self.write_batch_size,
            self.poll_entries,
            self.payload_bytes,
            self.repeat,
            self.elapsed.as_secs_f64(),
            self.rows_per_second(),
            self.wal_entries_read,
            self.arrow_batches_read,
            self.polls
        )
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn get_dataset_prefix() -> String {
    std::env::var("DATASET_PREFIX").unwrap_or_else(|_| {
        let d = std::env::temp_dir().join(format!("lance_topic_read_bench_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&d).expect("failed to create benchmark temp directory");
        d.to_string_lossy().to_string()
    })
}

fn parse_cases() -> Vec<ReadCase> {
    if let Ok(raw) = std::env::var("READ_CASES") {
        let parsed: Vec<_> = raw
            .split(';')
            .filter_map(|c| {
                let p: Vec<_> = c.split(':').collect();
                if p.len() != 5 {
                    return None;
                }
                Some(ReadCase {
                    name: p[0].to_string(),
                    producer_count: p[1].parse().ok()?,
                    rows: p[2].parse().ok()?,
                    write_batch_size: p[3].parse().ok()?,
                    poll_entries: p[4].parse().ok()?,
                })
            })
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    DEFAULT_CASES
        .iter()
        .map(|(n, pc, r, wbs, pe)| ReadCase {
            name: (*n).to_string(),
            producer_count: *pc,
            rows: *r,
            write_batch_size: *wbs,
            poll_entries: *pe,
        })
        .collect()
}

fn topic_table_id(case_name: &str, repeat: usize) -> Vec<String> {
    vec![format!(
        "topic_read_{}_{}",
        case_name.replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
        repeat
    )]
}

fn rows_for_producer(total: usize, count: u32, pid: u32) -> usize {
    let c = count as usize;
    let p = pid as usize;
    total / c + usize::from(p < total % c)
}

fn make_record_batches(
    schema: &Arc<ArrowSchema>,
    mode: SchemaMode,
    pid: u32,
    rows: usize,
    batch_size: usize,
    payload_bytes: usize,
) -> Vec<RecordBatch> {
    let body = "x".repeat(payload_bytes);
    let mut batches = Vec::with_capacity(rows.div_ceil(batch_size));
    let mut next = 0usize;
    while next < rows {
        let n = batch_size.min(rows - next);
        let ids: Vec<String> = (0..n).map(|i| format!("p{pid}-{}", next + i)).collect();
        let batch = match mode {
            SchemaMode::Default => {
                let payloads: Vec<Value> = (0..n)
                    .map(|i| json!({"row": next + i, "body": body}))
                    .collect();
                lance_topic::default_message_batch(ids, payloads).unwrap()
            }
            SchemaMode::Custom => {
                let emb: Vec<f32> = (0..n * EMBEDDING_DIM as usize)
                    .map(|i| i as f32 * 0.001)
                    .collect();
                let vals = Arc::new(Float32Array::from(emb));
                let lf = match schema.field_with_name("embedding").unwrap().data_type() {
                    DataType::FixedSizeList(inner, _) => inner.clone(),
                    _ => panic!("expected FixedSizeList"),
                };
                let ea = FixedSizeListArray::try_new(lf, EMBEDDING_DIM, vals, None).unwrap();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(ids)) as ArrayRef,
                        Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64))),
                        Arc::new(Float64Array::from_iter_values(
                            (0..n).map(|i| i as f64 * 1.5),
                        )),
                        Arc::new(ea) as ArrayRef,
                    ],
                )
                .unwrap()
            }
        };
        batches.push(batch);
        next += n;
    }
    batches
}

fn _input_bytes(batches: &[Vec<RecordBatch>]) -> u64 {
    batches
        .iter()
        .flatten()
        .map(|b| {
            b.columns()
                .iter()
                .map(|c| c.get_array_memory_size() as u64)
                .sum::<u64>()
        })
        .sum()
}

async fn seed_topic(producers: &[Producer], batches: Vec<Vec<RecordBatch>>) -> Result<()> {
    let mut iters: Vec<_> = batches.into_iter().map(Vec::into_iter).collect();
    loop {
        let mut futs = Vec::with_capacity(producers.len());
        for (p, it) in producers.iter().zip(iters.iter_mut()) {
            if let Some(b) = it.next() {
                futs.push(p.send(b));
            }
        }
        if futs.is_empty() {
            break;
        }
        try_join_all(futs).await?;
    }
    Ok(())
}

fn result_writer() -> Result<Option<std::fs::File>> {
    let Some(path) = std::env::var("RESULT_CSV").ok() else {
        return Ok(None);
    };
    let exists = std::path::Path::new(&path).exists();
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| Error::io(format!("failed to open RESULT_CSV: {e}")))?;
    if !exists {
        writeln!(f, "{}", ReadMeasurement::csv_header()).map_err(|e| Error::io(format!("{e}")))?;
    }
    Ok(Some(f))
}

fn write_measurement(w: &mut Option<std::fs::File>, m: &ReadMeasurement) -> Result<()> {
    let row = m.csv_row();
    println!("{row}");
    if let Some(f) = w {
        writeln!(f, "{row}").map_err(|e| Error::io(format!("{e}")))?;
        f.flush().map_err(|e| Error::io(format!("{e}")))?;
    }
    Ok(())
}

async fn run_case(
    prefix: &str,
    mode: SchemaMode,
    payload_bytes: usize,
    repeat: usize,
    case: &ReadCase,
) -> Result<ReadMeasurement> {
    let mut builder = Topic::builder()
        .directory(prefix, topic_table_id(&case.name, repeat))
        .partition_count(1);
    if mode == SchemaMode::Custom {
        builder = builder.schema(custom_user_schema());
    }
    let topic = builder.create().await?;
    let user_schema = topic.user_schema().clone();

    let input = (0..case.producer_count)
        .map(|pid| {
            make_record_batches(
                &user_schema,
                mode,
                pid,
                rows_for_producer(case.rows, case.producer_count, pid),
                case.write_batch_size,
                payload_bytes,
            )
        })
        .collect::<Vec<_>>();
    let producers = try_join_all((0..case.producer_count).map(|pid| {
        let t = topic.clone();
        async move { t.producer(format!("producer-{}", pid)).await }
    }))
    .await?;
    seed_topic(&producers, input).await?;

    topic.refresh_partitions().await?;
    let partitions = topic.partitions()?;
    let os: Arc<lance_io::object_store::ObjectStore> = topic.dataset().object_store(None).await?;
    let bp = topic.dataset().branch_location().path;
    let tailers: Vec<WalTailer> = partitions
        .iter()
        .filter(|p| p.partition_id == 0)
        .map(|p| WalTailer::new(os.clone(), bp.clone(), p.shard_id))
        .collect();
    let mut positions: Vec<u64> = Vec::new();
    for t in &tailers {
        positions.push(t.first_position().await?);
    }

    let start = Instant::now();
    let mut rows_read = 0usize;
    let mut wal_entries_read = 0usize;
    let mut arrow_batches_read = 0usize;
    let mut polls = 0usize;
    while rows_read < case.rows {
        let mut any = false;
        for (idx, tailer) in tailers.iter().enumerate() {
            for _ in 0..case.poll_entries {
                match tailer.read_entry(positions[idx]).await? {
                    Some(entry) => {
                        any = true;
                        positions[idx] += 1;
                        let batch = TopicBatch::from_entry(entry, 0, String::new())?;
                        rows_read += batch.num_rows();
                        wal_entries_read += 1;
                        arrow_batches_read += batch.batches.len();
                    }
                    None => break,
                }
            }
        }
        polls += 1;
        if !any {
            return Err(Error::io(format!(
                "case '{}' reached end of WAL after {} rows, expected {}",
                case.name, rows_read, case.rows
            )));
        }
    }
    let elapsed = start.elapsed();

    Ok(ReadMeasurement {
        case_name: case.name.clone(),
        schema_mode: mode.label().to_string(),
        producer_count: case.producer_count,
        rows: case.rows,
        write_batch_size: case.write_batch_size,
        poll_entries: case.poll_entries,
        payload_bytes,
        repeat,
        elapsed,
        wal_entries_read,
        arrow_batches_read,
        polls,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let prefix = get_dataset_prefix();
    let cases = parse_cases();
    let mode = SchemaMode::from_env();
    let payload_bytes = env_usize("PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES);
    let repeats = env_usize("REPEATS", DEFAULT_REPEATS).max(1);
    let mut writer = result_writer()?;

    println!("=== Lance Topic Read Benchmark ===");
    println!("dataset_prefix={prefix}");
    println!("schema_mode={}", mode.label());
    println!("payload_bytes={payload_bytes}");
    println!("repeats={repeats}");
    println!("cases={}", cases.len());
    println!("{}", ReadMeasurement::csv_header());

    for case in &cases {
        for repeat in 0..repeats {
            let m = run_case(&prefix, mode, payload_bytes, repeat, case).await?;
            write_measurement(&mut writer, &m)?;
        }
    }
    Ok(())
}
