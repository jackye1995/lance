// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Matrix benchmark for Lance topic producer write throughput.
//!
//! ## Configuration
//!
//! - `DATASET_PREFIX`: Directory namespace root URI. If not set, uses a temporary local directory.
//! - `PAYLOAD_BYTES`: Approximate bytes in each JSON payload body string (default: `256`).
//! - `REPEATS`: Repeated measurements per scenario (default: `3`).
//! - `SCHEMA_MODE`: `default` (id + payload) or `custom` (event_id + user_id + score + 1024-dim embedding). Default: `default`.
//! - `WRITE_CASES`: Optional explicit cases as `name:partitions:producers:rows:batch_size` separated by `;`.
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
use lance_core::Result;
use lance_topic::{Producer, Topic};
use serde_json::{Value, json};
use uuid::Uuid;

const DEFAULT_PAYLOAD_BYTES: usize = 256;
const DEFAULT_REPEATS: usize = 3;
const EMBEDDING_DIM: i32 = 1024;
const DEFAULT_CASES: &[(&str, u32, u32, usize, usize)] = &[
    ("horizontal_p1_prod1", 1, 1, 500_000, 5_000),
    ("horizontal_p2_prod2", 2, 2, 500_000, 5_000),
    ("horizontal_p4_prod4", 4, 4, 500_000, 5_000),
    ("horizontal_p4_prod10", 4, 10, 500_000, 5_000),
    ("horizontal_p8_prod16", 8, 16, 500_000, 5_000),
    ("trend_50k_p4_prod10", 4, 10, 50_000, 5_000),
    ("trend_200k_p4_prod10", 4, 10, 200_000, 5_000),
    ("trend_500k_p4_prod10", 4, 10, 500_000, 5_000),
    ("batch_1_p1_prod1", 1, 1, 2_000, 1),
    ("batch_10_p1_prod1", 1, 1, 20_000, 10),
    ("batch_100_p1_prod1", 1, 1, 100_000, 100),
    ("batch_1000_p1_prod1", 1, 1, 200_000, 1_000),
    ("batch_5000_p1_prod1", 1, 1, 500_000, 5_000),
    ("batch_10000_p1_prod1", 1, 1, 500_000, 10_000),
];

#[derive(Debug, Clone)]
struct WriteCase {
    name: String,
    partition_count: u32,
    producer_count: u32,
    rows: usize,
    batch_size: usize,
}

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

fn make_record_batches(
    schema: &Arc<ArrowSchema>,
    mode: SchemaMode,
    producer_id: u32,
    rows: usize,
    batch_size: usize,
    payload_bytes: usize,
    id_prefix: &str,
) -> Vec<RecordBatch> {
    let body = "x".repeat(payload_bytes);
    let mut batches = Vec::with_capacity(rows.div_ceil(batch_size));
    let mut next_row = 0usize;
    while next_row < rows {
        let n = batch_size.min(rows - next_row);
        let ids: Vec<String> = (0..n)
            .map(|idx| format!("{id_prefix}-p{producer_id}-{}", next_row + idx))
            .collect();
        let batch = match mode {
            SchemaMode::Default => {
                let payloads: Vec<Value> = (0..n)
                    .map(|idx| json!({"row": next_row + idx, "body": body}))
                    .collect();
                lance_topic::default_message_batch(ids, payloads).unwrap()
            }
            SchemaMode::Custom => {
                let embeddings: Vec<f32> = (0..n * EMBEDDING_DIM as usize)
                    .map(|i| i as f32 * 0.001)
                    .collect();
                let values = Arc::new(Float32Array::from(embeddings));
                let list_field = match schema.field_with_name("embedding").unwrap().data_type() {
                    DataType::FixedSizeList(inner, _) => inner.clone(),
                    _ => panic!("expected FixedSizeList"),
                };
                let embedding =
                    FixedSizeListArray::try_new(list_field, EMBEDDING_DIM, values, None).unwrap();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(ids)) as ArrayRef,
                        Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64))),
                        Arc::new(Float64Array::from_iter_values(
                            (0..n).map(|i| i as f64 * 1.5),
                        )),
                        Arc::new(embedding) as ArrayRef,
                    ],
                )
                .unwrap()
            }
        };
        batches.push(batch);
        next_row += n;
    }
    batches
}

fn rows_for_producer(total_rows: usize, producer_count: u32, producer_id: u32) -> usize {
    let pc = producer_count as usize;
    let pid = producer_id as usize;
    total_rows / pc + usize::from(pid < total_rows % pc)
}

fn make_batches_by_producer(
    schema: &Arc<ArrowSchema>,
    mode: SchemaMode,
    producer_count: u32,
    rows: usize,
    batch_size: usize,
    payload_bytes: usize,
    id_prefix: &str,
) -> Vec<Vec<RecordBatch>> {
    (0..producer_count)
        .map(|pid| {
            make_record_batches(
                schema,
                mode,
                pid,
                rows_for_producer(rows, producer_count, pid),
                batch_size,
                payload_bytes,
                id_prefix,
            )
        })
        .collect()
}

fn _input_bytes(batches_by_producer: &[Vec<RecordBatch>]) -> u64 {
    batches_by_producer
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

#[derive(Debug, Clone)]
struct WriteMeasurement {
    case_name: String,
    schema_mode: String,
    partition_count: u32,
    producer_count: u32,
    rows: usize,
    batch_size: usize,
    payload_bytes: usize,
    repeat: usize,
    elapsed: Duration,
    wal_bytes: u64,
    wal_entries: usize,
}

impl WriteMeasurement {
    fn rows_per_second(&self) -> f64 {
        self.rows as f64 / self.elapsed.as_secs_f64()
    }

    fn wal_mib_per_second(&self) -> f64 {
        self.wal_bytes as f64 / self.elapsed.as_secs_f64() / (1024.0 * 1024.0)
    }

    fn csv_header() -> &'static str {
        "benchmark,case,schema_mode,partition_count,producer_count,rows,batch_size,payload_bytes,repeat,elapsed_seconds,rows_per_second,wal_mib_per_second,wal_bytes,wal_entries"
    }

    fn csv_row(&self) -> String {
        format!(
            "write,{},{},{},{},{},{},{},{},{:.6},{:.3},{:.3},{},{}",
            self.case_name,
            self.schema_mode,
            self.partition_count,
            self.producer_count,
            self.rows,
            self.batch_size,
            self.payload_bytes,
            self.repeat,
            self.elapsed.as_secs_f64(),
            self.rows_per_second(),
            self.wal_mib_per_second(),
            self.wal_bytes,
            self.wal_entries
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
        let d = std::env::temp_dir().join(format!("lance_topic_bench_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&d).expect("failed to create benchmark temp directory");
        d.to_string_lossy().to_string()
    })
}

fn parse_cases() -> Vec<WriteCase> {
    if let Ok(raw) = std::env::var("WRITE_CASES") {
        let parsed: Vec<_> = raw
            .split(';')
            .filter_map(|c| {
                let p: Vec<_> = c.split(':').collect();
                if p.len() != 5 {
                    return None;
                }
                Some(WriteCase {
                    name: p[0].to_string(),
                    partition_count: p[1].parse().ok()?,
                    producer_count: p[2].parse().ok()?,
                    rows: p[3].parse().ok()?,
                    batch_size: p[4].parse().ok()?,
                })
            })
            .collect();
        if !parsed.is_empty() {
            return parsed;
        }
    }
    DEFAULT_CASES
        .iter()
        .map(|(n, pc, prc, r, bs)| WriteCase {
            name: (*n).to_string(),
            partition_count: *pc,
            producer_count: *prc,
            rows: *r,
            batch_size: *bs,
        })
        .collect()
}

fn topic_table_id(case_name: &str, repeat: usize) -> Vec<String> {
    vec![format!(
        "topic_write_{}_{}",
        case_name.replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
        repeat
    )]
}

async fn produce_batches(
    producers: &[Producer],
    batches_by_producer: Vec<Vec<RecordBatch>>,
) -> Result<(u64, usize)> {
    let mut iters: Vec<_> = batches_by_producer
        .into_iter()
        .map(Vec::into_iter)
        .collect();
    let mut wal_bytes = 0u64;
    let mut wal_entries = 0usize;
    loop {
        let mut futs = Vec::with_capacity(producers.len());
        for (producer, it) in producers.iter().zip(iters.iter_mut()) {
            if let Some(batch) = it.next() {
                futs.push(producer.send(batch));
            }
        }
        if futs.is_empty() {
            break;
        }
        for r in try_join_all(futs).await? {
            wal_bytes += r.entries.iter().map(|e| e.wal_bytes as u64).sum::<u64>();
            wal_entries += r.entries.len();
        }
    }
    Ok((wal_bytes, wal_entries))
}

fn result_writer() -> Result<Option<std::fs::File>> {
    let Some(path) = std::env::var("RESULT_CSV").ok() else {
        return Ok(None);
    };
    let exists = std::path::Path::new(&path).exists();
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(f, "{}", WriteMeasurement::csv_header())?;
    }
    Ok(Some(f))
}

fn write_measurement(w: &mut Option<std::fs::File>, m: &WriteMeasurement) -> Result<()> {
    let row = m.csv_row();
    println!("{row}");
    if let Some(f) = w {
        writeln!(f, "{row}")?;
        f.flush()?;
    }
    Ok(())
}

async fn run_case(
    dataset_prefix: &str,
    mode: SchemaMode,
    payload_bytes: usize,
    repeat: usize,
    case: &WriteCase,
) -> Result<WriteMeasurement> {
    let mut builder = Topic::builder()
        .directory(dataset_prefix, topic_table_id(&case.name, repeat))
        .partition_count(case.partition_count);
    if mode == SchemaMode::Custom {
        builder = builder.schema(custom_user_schema());
    }
    let topic = builder.create().await?;
    let user_schema = topic.user_schema().clone();

    let input = make_batches_by_producer(
        &user_schema,
        mode,
        case.producer_count,
        case.rows,
        case.batch_size,
        payload_bytes,
        "msg",
    );
    let producers = try_join_all((0..case.producer_count).map(|pid| {
        let t = topic.clone();
        async move { t.producer(format!("producer-{}", pid)).await }
    }))
    .await?;

    let warmup = make_batches_by_producer(
        &user_schema,
        mode,
        case.producer_count,
        (case.batch_size * case.producer_count as usize).min(1_000),
        case.batch_size,
        payload_bytes,
        "warmup",
    );
    produce_batches(&producers, warmup).await?;

    let start = Instant::now();
    let (wal_bytes, wal_entries) = produce_batches(&producers, input).await?;
    let elapsed = start.elapsed();

    Ok(WriteMeasurement {
        case_name: case.name.clone(),
        schema_mode: mode.label().to_string(),
        partition_count: case.partition_count,
        producer_count: case.producer_count,
        rows: case.rows,
        batch_size: case.batch_size,
        payload_bytes,
        repeat,
        elapsed,
        wal_bytes,
        wal_entries,
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

    println!("=== Lance Topic Write Benchmark ===");
    println!("dataset_prefix={prefix}");
    println!("schema_mode={}", mode.label());
    println!("payload_bytes={payload_bytes}");
    println!("repeats={repeats}");
    println!("cases={}", cases.len());
    println!("{}", WriteMeasurement::csv_header());

    for case in &cases {
        for repeat in 0..repeats {
            let m = run_case(&prefix, mode, payload_bytes, repeat, case).await?;
            write_measurement(&mut writer, &m)?;
        }
    }
    Ok(())
}
