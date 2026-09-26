// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use rocksdb::{DB, Options, WriteBatch as RocksWriteBatch, WriteOptions};
use serde_json::json;
use slatedb::config::{CloseOptions, FlushOptions, FlushType, Settings};
use slatedb::object_store::ObjectStore;
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::{Db, DbBuilder, WriteBatch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Engine {
    RocksDb,
    SlateDb,
}

impl Engine {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "rocksdb" => Ok(Self::RocksDb),
            "slatedb" => Ok(Self::SlateDb),
            _ => bail!("unknown engine '{value}', expected rocksdb|slatedb"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Storage {
    Active,
    Flushed,
}

impl Storage {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "flushed" => Ok(Self::Flushed),
            _ => bail!("unknown storage '{value}', expected active|flushed"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Flushed => "flushed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyType {
    Int,
    Uuid,
}

impl KeyType {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "int" | "i64" => Ok(Self::Int),
            "uuid" => Ok(Self::Uuid),
            _ => bail!("unknown key type '{value}', expected int|uuid"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Uuid => "uuid",
        }
    }
}

#[derive(Clone, Debug)]
struct Args {
    engine: Engine,
    storage: Storage,
    key_type: KeyType,
    rows: usize,
    value_size: usize,
    queries: usize,
    miss_ratio: f64,
    threads: usize,
    batch_rows: usize,
    warmup_rounds: usize,
    seed: u64,
    uri: String,
    output: Option<PathBuf>,
    rocksdb_sync: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            engine: Engine::SlateDb,
            storage: Storage::Active,
            key_type: KeyType::Int,
            rows: 1_000_000,
            value_size: 100,
            queries: 5_000,
            miss_ratio: 0.5,
            threads: 8,
            batch_rows: 1_000,
            warmup_rounds: 0,
            seed: 0x5eed,
            uri: String::new(),
            output: None,
            rocksdb_sync: false,
        }
    }
}

fn parse_value<T>(flag: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| anyhow::anyhow!("invalid {flag}: {value} ({error})"))
}

fn parse_args() -> Result<Args> {
    let mut args = Args::default();
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        if flag == "--rocksdb-sync" {
            args.rocksdb_sync = true;
            continue;
        }
        let value = values
            .next()
            .with_context(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--engine" => args.engine = Engine::parse(&value)?,
            "--storage" => args.storage = Storage::parse(&value)?,
            "--key-type" => args.key_type = KeyType::parse(&value)?,
            "--rows" => args.rows = parse_value(&flag, &value)?,
            "--value-size" => args.value_size = parse_value(&flag, &value)?,
            "--queries" => args.queries = parse_value(&flag, &value)?,
            "--miss-ratio" => args.miss_ratio = parse_value(&flag, &value)?,
            "--threads" => args.threads = parse_value(&flag, &value)?,
            "--batch-rows" => args.batch_rows = parse_value(&flag, &value)?,
            "--warmup-rounds" => args.warmup_rounds = parse_value(&flag, &value)?,
            "--seed" => args.seed = parse_value(&flag, &value)?,
            "--uri" => args.uri = value,
            "--output" => args.output = Some(PathBuf::from(value)),
            _ => bail!("unknown argument: {flag}"),
        }
    }
    if args.uri.is_empty() {
        bail!("--uri is required");
    }
    if args.rows == 0
        || args.value_size == 0
        || args.queries == 0
        || args.threads == 0
        || args.batch_rows == 0
    {
        bail!("rows, value-size, queries, threads, and batch-rows must be positive");
    }
    if !(0.0..=1.0).contains(&args.miss_ratio) {
        bail!("miss-ratio must be in [0, 1]");
    }
    if args.engine == Engine::RocksDb && args.uri.starts_with("s3://") {
        bail!("RocksDB does not support an S3 URI");
    }
    Ok(args)
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn next_below(&mut self, limit: u64) -> u64 {
        if limit == 0 {
            0
        } else {
            self.next_u64() % limit
        }
    }
}

fn shuffled_keys(count: usize, seed: u64) -> Vec<i64> {
    let mut random = SplitMix64::new(seed);
    let mut keys: Vec<i64> = (0..count as i64).collect();
    for index in (1..count).rev() {
        let swap = random.next_below(index as u64 + 1) as usize;
        keys.swap(index, swap);
    }
    keys
}

fn build_queries(rows: usize, count: usize, miss_ratio: f64, seed: u64) -> Vec<(i64, bool)> {
    let mut random = SplitMix64::new(seed ^ 0xd1b54a32d192ed03);
    let misses = (count as f64 * miss_ratio).round() as usize;
    let mut queries = Vec::with_capacity(count);
    for index in 0..count {
        if index < misses {
            let key = rows as i64 + random.next_below(rows as u64) as i64;
            queries.push((key, false));
        } else {
            let key = random.next_below(rows as u64) as i64;
            queries.push((key, true));
        }
    }
    for index in (1..queries.len()).rev() {
        let swap = random.next_below(index as u64 + 1) as usize;
        queries.swap(index, swap);
    }
    queries
}

fn key_bytes(key: i64, key_type: KeyType) -> Vec<u8> {
    match key_type {
        KeyType::Int => key.to_be_bytes().to_vec(),
        KeyType::Uuid => {
            let mut random = SplitMix64::new((key as u64) ^ 0xa5a55a5adeadbeef);
            let mut bytes = Vec::with_capacity(16);
            bytes.extend_from_slice(&random.next_u64().to_be_bytes());
            bytes.extend_from_slice(&(key as u64).to_be_bytes());
            bytes
        }
    }
}

fn make_value(key: i64, size: usize) -> Vec<u8> {
    (0..size)
        .map(|index| b'a' + ((key as u64).wrapping_add(index as u64) % 26) as u8)
        .collect()
}

#[derive(Default)]
struct LatencyStats {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    mean_us: f64,
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

fn compute_stats(mut values: Vec<f64>) -> LatencyStats {
    values.sort_by(f64::total_cmp);
    LatencyStats {
        p50_us: percentile(&values, 0.50),
        p95_us: percentile(&values, 0.95),
        p99_us: percentile(&values, 0.99),
        mean_us: values.iter().sum::<f64>() / values.len() as f64,
    }
}

fn process_cpu_secs() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return 0.0;
    }
    let usage = unsafe { usage.assume_init() };
    usage.ru_utime.tv_sec as f64
        + usage.ru_utime.tv_usec as f64 / 1_000_000.0
        + usage.ru_stime.tv_sec as f64
        + usage.ru_stime.tv_usec as f64 / 1_000_000.0
}

fn current_rss_mb() -> f64 {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else {
        return 0.0;
    };
    let pages = statm
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    pages as f64 * 4096.0 / (1024.0 * 1024.0)
}

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_bits: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_bits = Arc::new(AtomicU64::new(current_rss_mb().to_bits()));
        let thread_stop = stop.clone();
        let thread_peak = peak_bits.clone();
        let handle = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let current = current_rss_mb();
                let mut old = thread_peak.load(Ordering::Relaxed);
                while current > f64::from_bits(old) {
                    match thread_peak.compare_exchange_weak(
                        old,
                        current.to_bits(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(value) => old = value,
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        Self {
            stop,
            peak_bits,
            handle: Some(handle),
        }
    }

    fn peak_mb(&self) -> f64 {
        f64::from_bits(self.peak_bits.load(Ordering::Relaxed))
    }

    fn stop(mut self) -> f64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.peak_mb()
    }
}

struct EngineResult {
    engine: &'static str,
    write_rows_per_s: f64,
    write_accepted_rows_per_s: Option<f64>,
    wal_durable_rows_per_s: Option<f64>,
    durability_drain_s: Option<f64>,
    sstable_flush_s: Option<f64>,
    write_cpu_s: f64,
    read: LatencyStats,
    read_qps_1t: f64,
    read_qps_nt: f64,
    read_cpu_s: f64,
    hits: usize,
    misses_resolved: usize,
    peak_rss_mb: f64,
    rss_after_load_mb: f64,
}

impl EngineResult {
    fn to_json(&self, args: &Args) -> serde_json::Value {
        let engine_version = match args.engine {
            Engine::RocksDb => "rocksdb crate 0.23.0, librocksdb 10.4.2",
            Engine::SlateDb => "0.16.0",
        };
        json!({
            "engine": self.engine,
            "engine_version": engine_version,
            "source_revision": std::env::var("BENCH_SOURCE_REVISION").ok(),
            "host_type": std::env::var("BENCH_HOST_TYPE").ok(),
            "campaign_id": std::env::var("BENCH_CAMPAIGN_ID").ok(),
            "phase": std::env::var("BENCH_PHASE").ok(),
            "storage": args.storage.as_str(),
            "key_type": args.key_type.as_str(),
            "rows": args.rows,
            "value_size": args.value_size,
            "queries": args.queries,
            "miss_ratio": args.miss_ratio,
            "threads": args.threads,
            "batch_rows": args.batch_rows,
            "warmup_rounds": args.warmup_rounds,
            "seed": args.seed,
            "write_rows_per_s": self.write_rows_per_s as u64,
            "write_accepted_rows_per_s": self.write_accepted_rows_per_s.map(|value| value as u64),
            "wal_durable_rows_per_s": self.wal_durable_rows_per_s.map(|value| value as u64),
            "durability_drain_s": self.durability_drain_s.map(|value| format!("{value:.3}")),
            "sstable_flush_s": self.sstable_flush_s.map(|value| format!("{value:.3}")),
            "write_cpu_s": format!("{:.3}", self.write_cpu_s),
            "read_p50_us": (self.read.p50_us * 1000.0).round() / 1000.0,
            "read_p95_us": (self.read.p95_us * 1000.0).round() / 1000.0,
            "read_p99_us": (self.read.p99_us * 1000.0).round() / 1000.0,
            "read_mean_us": (self.read.mean_us * 1000.0).round() / 1000.0,
            "read_qps_1t": self.read_qps_1t as u64,
            "read_qps_nt": self.read_qps_nt as u64,
            "read_cpu_s": format!("{:.3}", self.read_cpu_s),
            "hits": self.hits,
            "misses_resolved": self.misses_resolved,
            "peak_rss_mb": self.peak_rss_mb as u64,
            "rss_after_load_mb": self.rss_after_load_mb as u64,
        })
    }
}

fn open_slate_store(uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, prefix) = rest
            .split_once('/')
            .map_or((rest, ""), |(bucket, prefix)| (bucket, prefix));
        if bucket.is_empty() {
            bail!("S3 URI requires a bucket");
        }
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .context("build SlateDB S3 object store")?;
        Ok((Arc::new(store), ObjectPath::from(prefix)))
    } else {
        std::fs::create_dir_all(uri).with_context(|| format!("create {uri}"))?;
        let store = LocalFileSystem::new_with_prefix(uri)
            .with_context(|| format!("open local object store {uri}"))?;
        Ok((Arc::new(store), ObjectPath::from("db")))
    }
}

fn slate_settings(args: &Args) -> Settings {
    let mut settings = Settings::default();
    let working_bytes = args.rows.saturating_mul(args.value_size + 256).max(1 << 30);
    settings.l0_sst_size_bytes = working_bytes;
    settings.max_unflushed_bytes = working_bytes.saturating_mul(2);
    settings.compactor_options = None;
    settings.garbage_collector_options = None;
    settings
}

async fn open_slate_db(
    path: ObjectPath,
    store: Arc<dyn ObjectStore>,
    settings: Settings,
) -> Result<Db> {
    DbBuilder::new(path, store)
        .with_settings(settings)
        .build()
        .await
        .context("open SlateDB")
}

async fn run_slate(
    args: &Args,
    insert_order: &[i64],
    queries: &[(i64, bool)],
) -> Result<EngineResult> {
    let sampler = RssSampler::start();
    let (store, path) = open_slate_store(&args.uri)?;
    let settings = slate_settings(args);
    let mut db = open_slate_db(path.clone(), store.clone(), settings.clone()).await?;

    let cpu_start = process_cpu_secs();
    let write_start = Instant::now();
    let mut handles = Vec::with_capacity(insert_order.len().div_ceil(args.batch_rows));
    for keys in insert_order.chunks(args.batch_rows) {
        let mut batch = WriteBatch::new();
        for &key in keys {
            batch.put(
                key_bytes(key, args.key_type),
                make_value(key, args.value_size),
            );
        }
        handles.push(db.write(batch).await.context("SlateDB write")?);
    }
    let accepted_s = write_start.elapsed().as_secs_f64();
    let drain_start = Instant::now();
    for handle in &handles {
        handle.await_durable().await.context("SlateDB durability")?;
    }
    let durability_drain_s = drain_start.elapsed().as_secs_f64();
    let durable_s = write_start.elapsed().as_secs_f64();
    let write_cpu_s = process_cpu_secs() - cpu_start;
    let rss_after_load_mb = sampler.peak_mb();

    let sstable_flush_s = if args.storage == Storage::Flushed {
        let flush_start = Instant::now();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .context("SlateDB MemTable flush")?;
        let elapsed = flush_start.elapsed().as_secs_f64();
        db.close_with_options(CloseOptions::default().with_flush_type(None))
            .await
            .context("close SlateDB before reopen")?;
        db = open_slate_db(path, store, settings).await?;
        Some(elapsed)
    } else {
        None
    };

    for round in 0..args.warmup_rounds {
        for &(key, expect_hit) in queries {
            let found = db.get(key_bytes(key, args.key_type)).await?.is_some();
            if found != expect_hit {
                bail!("SlateDB prewarm returned the wrong result for key {key}");
            }
        }
        eprintln!("[slatedb] completed read prewarm round {}", round + 1);
    }

    let read_cpu_start = process_cpu_secs();
    let read_start = Instant::now();
    let mut latencies = Vec::with_capacity(queries.len());
    let mut hits = 0;
    let mut misses_resolved = 0;
    for &(key, expect_hit) in queries {
        let start = Instant::now();
        let found = db.get(key_bytes(key, args.key_type)).await?.is_some();
        latencies.push(start.elapsed().as_nanos() as f64 / 1000.0);
        if found != expect_hit {
            bail!("SlateDB returned the wrong result for key {key}");
        }
        if found {
            hits += 1;
        } else {
            misses_resolved += 1;
        }
    }
    let read_qps_1t = queries.len() as f64 / read_start.elapsed().as_secs_f64().max(1e-9);
    let read_cpu_s = process_cpu_secs() - read_cpu_start;
    let read = compute_stats(latencies);

    let read_qps_nt = if args.threads == 1 {
        read_qps_1t
    } else {
        let keys: Arc<Vec<Vec<u8>>> = Arc::new(
            queries
                .iter()
                .map(|(key, _)| key_bytes(*key, args.key_type))
                .collect(),
        );
        let db = Arc::new(db.clone());
        let start = Instant::now();
        let mut tasks = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let keys = keys.clone();
            let db = db.clone();
            let threads = args.threads;
            tasks.push(tokio::spawn(async move {
                let mut index = shard;
                while index < keys.len() {
                    std::hint::black_box(db.get(&keys[index]).await.unwrap());
                    index += threads;
                }
            }));
        }
        for task in tasks {
            task.await?;
        }
        keys.len() as f64 / start.elapsed().as_secs_f64().max(1e-9)
    };

    db.close_with_options(CloseOptions::default().with_flush_type(None))
        .await
        .context("close SlateDB")?;
    let peak_rss_mb = sampler.stop();
    Ok(EngineResult {
        engine: if args.storage == Storage::Flushed {
            "slatedb-flushed"
        } else {
            "slatedb"
        },
        write_rows_per_s: args.rows as f64 / durable_s.max(1e-9),
        write_accepted_rows_per_s: Some(args.rows as f64 / accepted_s.max(1e-9)),
        wal_durable_rows_per_s: Some(args.rows as f64 / durable_s.max(1e-9)),
        durability_drain_s: Some(durability_drain_s),
        sstable_flush_s,
        write_cpu_s,
        read,
        read_qps_1t,
        read_qps_nt,
        read_cpu_s,
        hits,
        misses_resolved,
        peak_rss_mb,
        rss_after_load_mb,
    })
}

fn run_rocks(args: &Args, insert_order: &[i64], queries: &[(i64, bool)]) -> Result<EngineResult> {
    let sampler = RssSampler::start();
    let path = format!("{}/rocksdb", args.uri.trim_end_matches('/'));
    std::fs::create_dir_all(&path)?;
    let mut options = Options::default();
    options.create_if_missing(true);
    let write_buffer = args.rows * (args.value_size + 200) + (64 << 20);
    options.set_write_buffer_size(write_buffer);
    options.set_db_write_buffer_size(write_buffer);
    options.set_disable_auto_compactions(true);
    let db = Arc::new(DB::open(&options, &path)?);
    let mut write_options = WriteOptions::default();
    write_options.set_sync(args.rocksdb_sync);

    let cpu_start = process_cpu_secs();
    let write_start = Instant::now();
    for keys in insert_order.chunks(args.batch_rows) {
        let mut batch = RocksWriteBatch::default();
        for &key in keys {
            batch.put(
                key_bytes(key, args.key_type),
                make_value(key, args.value_size),
            );
        }
        db.write_opt(batch, &write_options)?;
    }
    let write_s = write_start.elapsed().as_secs_f64();
    let write_cpu_s = process_cpu_secs() - cpu_start;
    let rss_after_load_mb = sampler.peak_mb();
    let sstable_flush_s = if args.storage == Storage::Flushed {
        let start = Instant::now();
        db.flush()?;
        Some(start.elapsed().as_secs_f64())
    } else {
        None
    };

    for round in 0..args.warmup_rounds {
        for &(key, expect_hit) in queries {
            let found = db.get(key_bytes(key, args.key_type))?.is_some();
            if found != expect_hit {
                bail!("RocksDB prewarm returned the wrong result for key {key}");
            }
        }
        eprintln!("[rocksdb] completed read prewarm round {}", round + 1);
    }

    let read_cpu_start = process_cpu_secs();
    let read_start = Instant::now();
    let mut latencies = Vec::with_capacity(queries.len());
    let mut hits = 0;
    let mut misses_resolved = 0;
    for &(key, expect_hit) in queries {
        let start = Instant::now();
        let found = db.get(key_bytes(key, args.key_type))?.is_some();
        latencies.push(start.elapsed().as_nanos() as f64 / 1000.0);
        if found != expect_hit {
            bail!("RocksDB returned the wrong result for key {key}");
        }
        if found {
            hits += 1;
        } else {
            misses_resolved += 1;
        }
    }
    let read_qps_1t = queries.len() as f64 / read_start.elapsed().as_secs_f64().max(1e-9);
    let read_cpu_s = process_cpu_secs() - read_cpu_start;
    let read = compute_stats(latencies);
    let read_qps_nt = if args.threads == 1 {
        read_qps_1t
    } else {
        let keys: Arc<Vec<Vec<u8>>> = Arc::new(
            queries
                .iter()
                .map(|(key, _)| key_bytes(*key, args.key_type))
                .collect(),
        );
        let start = Instant::now();
        let mut workers = Vec::with_capacity(args.threads);
        for shard in 0..args.threads {
            let keys = keys.clone();
            let db = db.clone();
            let threads = args.threads;
            workers.push(std::thread::spawn(move || {
                let mut index = shard;
                while index < keys.len() {
                    std::hint::black_box(db.get(&keys[index]).unwrap());
                    index += threads;
                }
            }));
        }
        for worker in workers {
            worker.join().expect("RocksDB read worker");
        }
        keys.len() as f64 / start.elapsed().as_secs_f64().max(1e-9)
    };
    let peak_rss_mb = sampler.stop();
    Ok(EngineResult {
        engine: if args.storage == Storage::Flushed {
            "rocksdb-flushed"
        } else {
            "rocksdb"
        },
        write_rows_per_s: args.rows as f64 / write_s.max(1e-9),
        write_accepted_rows_per_s: Some(args.rows as f64 / write_s.max(1e-9)),
        wal_durable_rows_per_s: args
            .rocksdb_sync
            .then_some(args.rows as f64 / write_s.max(1e-9)),
        durability_drain_s: None,
        sstable_flush_s,
        write_cpu_s,
        read,
        read_qps_1t,
        read_qps_nt,
        read_cpu_s,
        hits,
        misses_resolved,
        peak_rss_mb,
        rss_after_load_mb,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    eprintln!(
        "engine={} storage={} rows={} value_size={} queries={} threads={} warmup_rounds={} uri={}",
        args.engine.as_str(),
        args.storage.as_str(),
        args.rows,
        args.value_size,
        args.queries,
        args.threads,
        args.warmup_rounds,
        args.uri
    );
    let insert_order = shuffled_keys(args.rows, args.seed);
    let queries = build_queries(args.rows, args.queries, args.miss_ratio, args.seed);
    let result = match args.engine {
        Engine::RocksDb => run_rocks(&args, &insert_order, &queries)?,
        Engine::SlateDb => run_slate(&args, &insert_order, &queries).await?,
    };
    let output = json!({
        "bench": "mem_wal_kv_point_lookup",
        "reference_version": env!("CARGO_PKG_VERSION"),
        "results": [result.to_json(&args)],
    });
    let text = serde_json::to_string_pretty(&output)?;
    if let Some(path) = &args.output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, text.as_bytes())?;
    }
    println!("{text}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{KeyType, build_queries, key_bytes, shuffled_keys};

    #[test]
    fn deterministic_inputs_are_stable() {
        assert_eq!(shuffled_keys(8, 7), shuffled_keys(8, 7));
        assert_eq!(
            build_queries(100, 20, 0.5, 7),
            build_queries(100, 20, 0.5, 7)
        );
        assert_eq!(key_bytes(7, KeyType::Int).len(), 8);
        assert_eq!(key_bytes(7, KeyType::Uuid).len(), 16);
    }
}
