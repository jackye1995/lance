// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Memory leak reproduction benchmark.
//!
//! Equivalent to the Python script repro_pylance.py.
//! This runs concurrent vector searches against a Lance dataset and monitors RSS memory growth.
//!
//! Run with:
//! ```bash
//! export AZURE_STORAGE_ACCOUNT_NAME=lancedb50ohp7
//! export AZURE_STORAGE_ACCOUNT_KEY=<your-key>
//! cargo run --release --example memleak_repro
//! ```

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::Float32Array;
use clap::Parser;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_io::object_store::ObjectStoreRegistry;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Semaphore;
use tokio::time::interval;

const URI: &str = "az://examples/fineweb_edu_full_384_dim.lance";
const VECTOR_COLUMN: &str = "text_embedding";
const VECTOR_DIM: usize = 384;
const K: usize = 10;
const CONCURRENCY: usize = 32;
const REPORT_INTERVAL_SECS: f64 = 5.0;
const INDEX_CACHE_SIZE_BYTES: usize = 100_000_000; // 100MB
const METADATA_CACHE_SIZE_BYTES: usize = 100_000_000; // 100MB
const LOG_PATH: &str = "/tmp/lance_repro_rust.jsonl";

#[derive(Parser, Debug)]
#[command(version, about = "Memory leak reproduction benchmark")]
struct Args {
    /// Dataset URI (default: az://examples/fineweb_edu_full_384_dim.lance)
    #[arg(long, default_value = URI)]
    uri: String,

    /// Concurrency level
    #[arg(long, default_value_t = CONCURRENCY)]
    concurrency: usize,

    /// Index cache size in bytes
    #[arg(long, default_value_t = INDEX_CACHE_SIZE_BYTES)]
    index_cache_size: usize,

    /// Metadata cache size in bytes
    #[arg(long, default_value_t = METADATA_CACHE_SIZE_BYTES)]
    metadata_cache_size: usize,
}

/// Get RSS memory in bytes from /proc/self/statm (Linux only)
fn get_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let parts: Vec<&str> = statm.split_whitespace().collect();
        if parts.len() >= 2 {
            let rss_pages: u64 = parts[1].parse().ok()?;
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 };
            return Some(rss_pages * page_size);
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        // For non-Linux, try using sysinfo-like approach or return None
        None
    }
}

/// Call malloc_trim on Linux to release memory back to OS
#[cfg(target_os = "linux")]
fn malloc_trim() -> bool {
    extern "C" {
        fn malloc_trim(pad: libc::size_t) -> libc::c_int;
    }
    unsafe { malloc_trim(0) != 0 }
}

#[cfg(not(target_os = "linux"))]
fn malloc_trim() -> bool {
    false
}

struct Stats {
    done: AtomicU64,
    latency_nanos: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            done: AtomicU64::new(0),
            latency_nanos: AtomicU64::new(0),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let args = Args::parse();

    println!("{}", "=".repeat(70));
    println!("Lance (Rust) Memory Growth Reproduction");
    println!("{}", "=".repeat(70));
    println!("\nConfiguration:");
    println!("  Dataset: {}", args.uri);
    println!(
        "  Index cache limit: {}MB",
        args.index_cache_size / 1024 / 1024
    );
    println!(
        "  Metadata cache limit: {}MB",
        args.metadata_cache_size / 1024 / 1024
    );
    println!(
        "  Total cache limit: {}MB",
        (args.index_cache_size + args.metadata_cache_size) / 1024 / 1024
    );
    println!("  Concurrency: {}", args.concurrency);
    println!();

    // Check if malloc_trim is available
    #[cfg(target_os = "linux")]
    println!("malloc_trim available - will call every 30 seconds");
    #[cfg(not(target_os = "linux"))]
    println!("malloc_trim NOT available (not on Linux)");
    println!();

    // Create session with cache limits
    let session = Arc::new(Session::new(
        args.index_cache_size,
        args.metadata_cache_size,
        Arc::new(ObjectStoreRegistry::default()),
    ));

    // Get storage options from environment
    let mut storage_options = HashMap::new();
    if let Ok(account_name) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
        storage_options.insert("account_name".to_string(), account_name);
    }
    if let Ok(account_key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
        storage_options.insert("account_key".to_string(), account_key);
    }

    println!("Opening dataset...");
    let dataset = Arc::new(
        DatasetBuilder::from_uri(&args.uri)
            .with_storage_options(storage_options)
            .with_session(session.clone())
            .load()
            .await?,
    );
    let row_count = dataset.count_rows(None).await?;
    println!("Dataset opened: {} rows", row_count);

    let log_file = File::create(LOG_PATH)?;
    let mut log_writer = BufWriter::new(log_file);

    let stats = Arc::new(Stats::new());
    let semaphore = Arc::new(Semaphore::new(args.concurrency));

    println!("\nStarting {} concurrent workers...", args.concurrency);
    println!("{}\n", "=".repeat(70));

    let start = Instant::now();

    // Spawn worker tasks
    let workers: Vec<_> = (0..args.concurrency)
        .map(|_| {
            let dataset = dataset.clone();
            let stats = stats.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(async move {
                let mut rng = StdRng::from_os_rng();
                loop {
                    let _permit = semaphore.acquire().await.unwrap();

                    // Generate random query vector
                    let query: Vec<f32> = (0..VECTOR_DIM)
                        .map(|_| rng.random_range(-1.0..1.0))
                        .collect();
                    let query_array = Float32Array::from(query);

                    let search_start = Instant::now();

                    // Perform vector search
                    let columns: &[&str] = &["id"];
                    let result = dataset
                        .scan()
                        .project(columns)
                        .unwrap()
                        .nearest(VECTOR_COLUMN, &query_array, K)
                        .unwrap()
                        .minimum_nprobes(20)
                        .maximum_nprobes(20)
                        .try_into_stream()
                        .await
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await;

                    if let Err(e) = result {
                        eprintln!("Search error: {}", e);
                        continue;
                    }

                    let latency = search_start.elapsed();
                    stats.done.fetch_add(1, Ordering::Relaxed);
                    stats
                        .latency_nanos
                        .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Reporter task
    let stats_for_reporter = stats.clone();
    let session_for_reporter = session.clone();
    let reporter = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs_f64(REPORT_INTERVAL_SECS));
        let mut last_done: u64 = 0;
        let mut last_latency: u64 = 0;
        let mut last_trim_time = Instant::now();
        let limit_mb =
            (INDEX_CACHE_SIZE_BYTES + METADATA_CACHE_SIZE_BYTES) as f64 / 1024.0 / 1024.0;

        loop {
            interval.tick().await;

            let now = Instant::now();
            let elapsed = start.elapsed().as_secs_f64();
            let done = stats_for_reporter.done.load(Ordering::Relaxed);
            let latency = stats_for_reporter.latency_nanos.load(Ordering::Relaxed);

            let window_done = done - last_done;
            let window_latency = latency - last_latency;
            let qps = window_done as f64 / REPORT_INTERVAL_SECS;
            let avg_latency_ms = if window_done > 0 {
                Some((window_latency as f64 / window_done as f64) / 1_000_000.0)
            } else {
                None
            };

            let rss_before = get_rss_bytes().unwrap_or(0);
            let rss_before_gb = rss_before as f64 / (1024.0 * 1024.0 * 1024.0);
            let cache_bytes = session_for_reporter.size_bytes();
            let cache_mb = cache_bytes as f64 / (1024.0 * 1024.0);
            let status = if cache_mb <= limit_mb * 1.1 {
                "OK"
            } else {
                "EXCEEDED"
            };

            // Check if we should call malloc_trim (every 30 seconds)
            let trimmed = if now.duration_since(last_trim_time).as_secs() >= 30 {
                let result = malloc_trim();
                last_trim_time = now;
                result
            } else {
                false
            };

            let rss_after = get_rss_bytes().unwrap_or(0);
            let rss_after_gb = rss_after as f64 / (1024.0 * 1024.0 * 1024.0);

            if trimmed {
                let freed_mb = (rss_before as f64 - rss_after as f64) / (1024.0 * 1024.0);
                println!(
                    "[{:7.1}s] done={:6} qps={:6.2} avg={:.3}ms rss={:.2}GB -> {:.2}GB (freed {:.1}MB) cache={:.1}MB [{}] [TRIMMED]",
                    elapsed,
                    done,
                    qps,
                    avg_latency_ms.unwrap_or(0.0),
                    rss_before_gb,
                    rss_after_gb,
                    freed_mb,
                    cache_mb,
                    status
                );
            } else {
                println!(
                    "[{:7.1}s] done={:6} qps={:6.2} avg={:.3}ms rss={:.2}GB cache={:.1}MB [{}]",
                    elapsed,
                    done,
                    qps,
                    avg_latency_ms.unwrap_or(0.0),
                    rss_before_gb,
                    cache_mb,
                    status
                );
            }

            // Write to log file
            let log_entry = serde_json::json!({
                "event": "report",
                "elapsed_seconds": elapsed,
                "done": done,
                "qps": qps,
                "avg_latency_ms": avg_latency_ms,
                "rss_before_gb": rss_before_gb,
                "rss_after_gb": rss_after_gb,
                "cache_mb": cache_mb,
                "status": status,
                "trimmed": trimmed,
            });
            writeln!(log_writer, "{}", log_entry).ok();
            log_writer.flush().ok();

            last_done = done;
            last_latency = latency;
        }
    });

    // Wait for Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\nInterrupted by user");
        }
        _ = async {
            for worker in workers {
                worker.await.ok();
            }
        } => {}
    }

    reporter.abort();

    Ok(())
}
