// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Memory profiling version of the memory leak reproduction benchmark.
//!
//! This uses dhat for heap profiling to show detailed allocation statistics.
//!
//! Run with:
//! ```bash
//! export AZURE_STORAGE_ACCOUNT_NAME=lancedb50ohp7
//! export AZURE_STORAGE_ACCOUNT_KEY=<your-key>
//!
//! # Run with dhat profiling (generates dhat-heap.json)
//! cargo run --release --example memleak_profile --features dhat-heap
//!
//! # View results at https://nnethercote.github.io/dh_view/dh_view.html
//! ```
//!
//! Alternative: Use heaptrack (Linux)
//! ```bash
//! # Install heaptrack
//! sudo yum install heaptrack  # or apt-get install heaptrack
//!
//! # Run with heaptrack
//! heaptrack ./target/release/examples/memleak_repro
//!
//! # Analyze results
//! heaptrack_gui heaptrack.memleak_repro.*.gz
//! # Or for text output:
//! heaptrack_print heaptrack.memleak_repro.*.gz
//! ```

#![allow(clippy::print_stdout)]

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::array::Float32Array;
use clap::Parser;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_io::object_store::ObjectStoreRegistry;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::Semaphore;

const URI: &str = "az://examples/fineweb_edu_full_384_dim.lance";
const VECTOR_COLUMN: &str = "text_embedding";
const VECTOR_DIM: usize = 384;
const K: usize = 10;
const CONCURRENCY: usize = 2;
const INDEX_CACHE_SIZE_BYTES: usize = 100_000_000; // 100MB
const METADATA_CACHE_SIZE_BYTES: usize = 100_000_000; // 100MB

#[derive(Parser, Debug)]
#[command(version, about = "Memory profiling benchmark")]
struct Args {
    /// Dataset URI
    #[arg(long, default_value = URI)]
    uri: String,

    /// Concurrency level
    #[arg(long, default_value_t = CONCURRENCY)]
    concurrency: usize,

    /// Number of searches to run before stopping (for profiling)
    #[arg(long, default_value_t = 100)]
    num_searches: u64,

    /// Index cache size in bytes
    #[arg(long, default_value_t = INDEX_CACHE_SIZE_BYTES)]
    index_cache_size: usize,

    /// Metadata cache size in bytes
    #[arg(long, default_value_t = METADATA_CACHE_SIZE_BYTES)]
    metadata_cache_size: usize,

    /// Print memory stats every N searches
    #[arg(long, default_value_t = 10)]
    stats_interval: u64,
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
        None
    }
}

/// Get detailed memory info from /proc/self/status (Linux only)
#[cfg(target_os = "linux")]
fn get_memory_details() -> HashMap<String, u64> {
    use std::fs;
    let mut result = HashMap::new();

    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let key = parts[0].trim_end_matches(':');
                if let Ok(value) = parts[1].parse::<u64>() {
                    // Values are in kB
                    result.insert(key.to_string(), value * 1024);
                }
            }
        }
    }

    result
}

#[cfg(not(target_os = "linux"))]
fn get_memory_details() -> HashMap<String, u64> {
    HashMap::new()
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

fn print_memory_snapshot(label: &str, session: &Session) {
    let rss = get_rss_bytes().unwrap_or(0);
    let cache_bytes = session.size_bytes();
    let mem_details = get_memory_details();

    println!("\n=== Memory Snapshot: {} ===", label);
    println!("  RSS:        {:>10.2} MB", rss as f64 / 1024.0 / 1024.0);
    println!(
        "  Cache:      {:>10.2} MB",
        cache_bytes as f64 / 1024.0 / 1024.0
    );

    if let Some(vm_rss) = mem_details.get("VmRSS") {
        println!(
            "  VmRSS:      {:>10.2} MB",
            *vm_rss as f64 / 1024.0 / 1024.0
        );
    }
    if let Some(vm_data) = mem_details.get("VmData") {
        println!(
            "  VmData:     {:>10.2} MB",
            *vm_data as f64 / 1024.0 / 1024.0
        );
    }
    if let Some(vm_stk) = mem_details.get("VmStk") {
        println!(
            "  VmStk:      {:>10.2} MB",
            *vm_stk as f64 / 1024.0 / 1024.0
        );
    }
    if let Some(rss_anon) = mem_details.get("RssAnon") {
        println!(
            "  RssAnon:    {:>10.2} MB",
            *rss_anon as f64 / 1024.0 / 1024.0
        );
    }
    if let Some(rss_file) = mem_details.get("RssFile") {
        println!(
            "  RssFile:    {:>10.2} MB",
            *rss_file as f64 / 1024.0 / 1024.0
        );
    }

    #[cfg(feature = "dhat-heap")]
    {
        let dhat_stats = dhat::HeapStats::get();
        println!("  --- dhat stats ---");
        println!("  Total bytes:     {:>10}", dhat_stats.total_bytes);
        println!("  Total blocks:    {:>10}", dhat_stats.total_blocks);
        println!("  Current bytes:   {:>10}", dhat_stats.curr_bytes);
        println!("  Current blocks:  {:>10}", dhat_stats.curr_blocks);
        println!("  Max bytes:       {:>10}", dhat_stats.max_bytes);
        println!("  Max blocks:      {:>10}", dhat_stats.max_blocks);
    }

    println!();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    env_logger::init();

    let args = Args::parse();

    println!("{}", "=".repeat(70));
    println!("Lance Memory Profiling Benchmark");
    println!("{}", "=".repeat(70));

    #[cfg(feature = "dhat-heap")]
    println!("dhat heap profiling ENABLED - will generate dhat-heap.json on exit");
    #[cfg(not(feature = "dhat-heap"))]
    println!("dhat heap profiling DISABLED - run with --features dhat-heap to enable");

    println!("\nConfiguration:");
    println!("  Dataset: {}", args.uri);
    println!(
        "  Cache limit: {}MB (index) + {}MB (metadata)",
        args.index_cache_size / 1024 / 1024,
        args.metadata_cache_size / 1024 / 1024
    );
    println!("  Concurrency: {}", args.concurrency);
    println!("  Searches to run: {}", args.num_searches);
    println!("  Stats interval: every {} searches", args.stats_interval);
    println!();

    // Create session with cache limits
    let session = Arc::new(Session::new(
        args.index_cache_size,
        args.metadata_cache_size,
        Arc::new(ObjectStoreRegistry::default()),
    ));

    print_memory_snapshot("After session creation", &session);

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

    print_memory_snapshot("After dataset open", &session);

    let stats = Arc::new(Stats::new());
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let target_searches = args.num_searches;
    let stats_interval = args.stats_interval;

    println!(
        "\nStarting {} searches with {} workers...",
        target_searches, args.concurrency
    );
    println!("{}\n", "=".repeat(70));

    #[cfg(feature = "dhat-heap")]
    println!("TIMELINE,elapsed_s,searches,rss_bytes,cache_bytes,total_alloc_bytes,total_blocks,curr_bytes,max_bytes");

    let start = Instant::now();

    // Spawn worker tasks
    let workers: Vec<_> = (0..args.concurrency)
        .map(|worker_id| {
            let dataset = dataset.clone();
            let stats = stats.clone();
            let semaphore = semaphore.clone();
            let session = session.clone();

            tokio::spawn(async move {
                let mut rng = StdRng::from_os_rng();
                loop {
                    let current = stats.done.load(Ordering::Relaxed);
                    if current >= target_searches {
                        break;
                    }

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
                    let done = stats.done.fetch_add(1, Ordering::Relaxed) + 1;
                    stats
                        .latency_nanos
                        .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);

                    // Print stats periodically
                    if done % stats_interval == 0 && worker_id == 0 {
                        let elapsed = start.elapsed().as_secs_f64();
                        let rss = get_rss_bytes().unwrap_or(0);
                        let cache_bytes = session.size_bytes();

                        #[cfg(feature = "dhat-heap")]
                        {
                            let dhat_stats = dhat::HeapStats::get();
                            // Output CSV format for timeline analysis
                            println!(
                                "TIMELINE,{:.1},{},{},{},{},{},{},{}",
                                elapsed,
                                done,
                                rss,
                                cache_bytes,
                                dhat_stats.total_bytes,
                                dhat_stats.total_blocks,
                                dhat_stats.curr_bytes,
                                dhat_stats.max_bytes
                            );
                        }
                        #[cfg(not(feature = "dhat-heap"))]
                        println!(
                            "[{:6.1}s] searches={:5} rss={:.2}MB cache={:.1}MB",
                            elapsed,
                            done,
                            rss as f64 / 1024.0 / 1024.0,
                            cache_bytes as f64 / 1024.0 / 1024.0
                        );
                    }
                }
            })
        })
        .collect();

    // Wait for all workers to complete
    for worker in workers {
        worker.await.ok();
    }

    let elapsed = start.elapsed();
    let total_done = stats.done.load(Ordering::Relaxed);
    let total_latency = stats.latency_nanos.load(Ordering::Relaxed);

    println!("\n{}", "=".repeat(70));
    println!("Benchmark Complete");
    println!("{}", "=".repeat(70));
    println!("  Total searches: {}", total_done);
    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    println!("  QPS: {:.2}", total_done as f64 / elapsed.as_secs_f64());
    println!(
        "  Avg latency: {:.2}ms",
        (total_latency as f64 / total_done as f64) / 1_000_000.0
    );

    print_memory_snapshot("Final", &session);

    #[cfg(feature = "dhat-heap")]
    println!("\ndhat profile saved to dhat-heap.json");
    #[cfg(feature = "dhat-heap")]
    println!("View at: https://nnethercote.github.io/dh_view/dh_view.html");

    Ok(())
}
