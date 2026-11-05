// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Opt-in request statistics and tracing for `la://`, for benchmarking.
//!
//! Disabled unless one of these environment variables is set:
//!
//! - `LA_STATS_FILE`: every `LA_STATS_INTERVAL_SECS` (default 10) seconds,
//!   append one JSON line of cumulative counters for this process.
//! - `LA_TRACE_FILE`: append one tab-separated line per HTTP request, keeping
//!   every `LA_TRACE_SAMPLE`-th request (default 1, i.e. all of them).

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::Stream;
use futures::stream::BoxStream;
use object_store::path::Path;
use serde::Serialize;

const STATS_FILE_ENV: &str = "LA_STATS_FILE";
const STATS_INTERVAL_ENV: &str = "LA_STATS_INTERVAL_SECS";
const TRACE_FILE_ENV: &str = "LA_TRACE_FILE";
const TRACE_SAMPLE_ENV: &str = "LA_TRACE_SAMPLE";
const DEFAULT_STATS_INTERVAL_SECS: u64 = 10;
/// Upper bounds of the request size histogram buckets; the last bucket is unbounded.
const SIZE_BUCKETS: [u64; 7] = [
    4 << 10,
    16 << 10,
    64 << 10,
    256 << 10,
    1 << 20,
    4 << 20,
    16 << 20,
];

#[derive(Default, Clone, Copy, Serialize)]
struct Counter {
    count: u64,
    bytes: u64,
    errors: u64,
    micros: u64,
}

#[derive(Default)]
struct Tables {
    counters: HashMap<String, Counter>,
    sizes: HashMap<String, [u64; SIZE_BUCKETS.len() + 1]>,
}

struct Stats {
    tables: Mutex<Tables>,
    trace: Option<Mutex<BufWriter<File>>>,
    trace_sample: u64,
    seq: AtomicU64,
}

#[derive(Serialize)]
struct Snapshot<'a> {
    ts: f64,
    pid: u32,
    counters: &'a HashMap<String, Counter>,
    sizes: &'a HashMap<String, [u64; SIZE_BUCKETS.len() + 1]>,
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

fn open_append(path: &str) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

fn stats() -> Option<&'static Stats> {
    static STATS: OnceLock<Option<Stats>> = OnceLock::new();
    STATS
        .get_or_init(|| {
            let stats_file = std::env::var(STATS_FILE_ENV).ok();
            let trace_file = std::env::var(TRACE_FILE_ENV).ok();
            if stats_file.is_none() && trace_file.is_none() {
                return None;
            }
            if let Some(path) = stats_file {
                let interval = std::env::var(STATS_INTERVAL_ENV)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(DEFAULT_STATS_INTERVAL_SECS);
                std::thread::spawn(move || dump_loop(path, Duration::from_secs(interval)));
            }
            Some(Stats {
                tables: Mutex::default(),
                trace: trace_file
                    .and_then(|path| open_append(&path))
                    .map(|file| Mutex::new(BufWriter::new(file))),
                trace_sample: std::env::var(TRACE_SAMPLE_ENV)
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .filter(|n| *n > 0)
                    .unwrap_or(1),
                seq: AtomicU64::new(0),
            })
        })
        .as_ref()
}

fn dump_loop(path: String, interval: Duration) {
    loop {
        std::thread::sleep(interval);
        let Some(stats) = stats() else { return };
        let line = {
            let tables = stats.tables.lock().unwrap_or_else(|e| e.into_inner());
            serde_json::to_string(&Snapshot {
                ts: now_secs(),
                pid: std::process::id(),
                counters: &tables.counters,
                sizes: &tables.sizes,
            })
        };
        if let (Ok(line), Some(mut file)) = (line, open_append(&path)) {
            let _ = writeln!(file, "{line}");
        }
        if let Some(trace) = &stats.trace {
            let _ = trace.lock().unwrap_or_else(|e| e.into_inner()).flush();
        }
    }
}

pub(super) fn enabled() -> bool {
    stats().is_some()
}

/// The kind of object `location` is, from its place in the table layout.
pub(super) fn category(location: &Path) -> &'static str {
    let path = location.as_ref();
    let (dir, file) = path.rsplit_once('/').unwrap_or(("", path));
    let parent = dir.rsplit('/').next().unwrap_or("");
    if path.contains("/_indices/") || path.starts_with("_indices/") {
        return "index";
    }
    match parent {
        "data" if file.ends_with(".lance") => "data",
        "_deletions" => "deletion",
        "_versions" => "manifest",
        "_transactions" => "transaction",
        _ => "other",
    }
}

fn bucket(len: u64) -> usize {
    SIZE_BUCKETS
        .iter()
        .position(|bound| len <= *bound)
        .unwrap_or(SIZE_BUCKETS.len())
}

fn record(key: String, bytes: u64, error: bool, elapsed: Duration, size: Option<u64>) {
    let Some(stats) = stats() else { return };
    let mut tables = stats.tables.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(len) = size {
        tables.sizes.entry(key.clone()).or_default()[bucket(len)] += 1;
    }
    let counter = tables.counters.entry(key).or_default();
    counter.count += 1;
    counter.bytes += bytes;
    counter.errors += u64::from(error);
    counter.micros += elapsed.as_micros() as u64;
}

/// One object-store read as Lance issued it, before routing.
pub(super) fn record_read(kind: &str, location: &Path, len: Option<u64>) {
    if !enabled() {
        return;
    }
    record(
        format!("read\t{kind}\t{}", category(location)),
        len.unwrap_or(0),
        false,
        Duration::ZERO,
        len,
    );
}

pub(super) fn record_list(endpoint: &str) {
    if !enabled() {
        return;
    }
    record(format!("list\t{endpoint}"), 0, false, Duration::ZERO, None);
}

fn trace(
    method: &str,
    endpoint: &str,
    location: &Path,
    range: Option<&Range<u64>>,
    status: u16,
    latency: Duration,
    bytes: u64,
) {
    let Some(stats) = stats() else { return };
    let Some(trace) = &stats.trace else { return };
    if stats.seq.fetch_add(1, Ordering::Relaxed) % stats.trace_sample != 0 {
        return;
    }
    let (start, len) = range.map_or((0, 0), |r| (r.start, r.end - r.start));
    let mut out = trace.lock().unwrap_or_else(|e| e.into_inner());
    let _ = writeln!(
        out,
        "{:.3}\t{}\t{method}\t{}\t{endpoint}\t{location}\t{start}\t{len}\t{status}\t{}\t{bytes}",
        now_secs(),
        std::process::id(),
        category(location),
        latency.as_micros(),
    );
}

/// An HTTP request that ended before a body was streamed: a HEAD or a failure.
pub(super) fn record_response(
    method: &str,
    endpoint: &str,
    location: &Path,
    range: Option<&Range<u64>>,
    status: u16,
    latency: Duration,
) {
    if !enabled() {
        return;
    }
    let error = !(200..300).contains(&status);
    record(
        format!("http\t{method}\t{}\t{endpoint}", category(location)),
        0,
        error,
        latency,
        None,
    );
    trace(method, endpoint, location, range, status, latency, 0);
}

/// Counts the bytes of a GET body as they arrive, and whether the body was
/// dropped before it was fully read.
pub(super) struct CountedBody {
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    endpoint: String,
    location: Path,
    range: Option<Range<u64>>,
    status: u16,
    expected: u64,
    received: u64,
    started: Instant,
    headers_latency: Duration,
    done: bool,
}

impl CountedBody {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        inner: BoxStream<'static, object_store::Result<Bytes>>,
        endpoint: &str,
        location: &Path,
        range: Option<Range<u64>>,
        status: u16,
        expected: u64,
        started: Instant,
    ) -> Self {
        Self {
            inner,
            endpoint: endpoint.to_string(),
            location: location.clone(),
            range,
            status,
            expected,
            received: 0,
            headers_latency: started.elapsed(),
            started,
            done: false,
        }
    }

    fn finish(&mut self, error: bool) {
        self.done = true;
        let category = category(&self.location);
        let total = self.started.elapsed();
        record(
            format!("http\tGET\t{category}\t{}", self.endpoint),
            self.received,
            error,
            self.headers_latency,
            Some(self.expected),
        );
        record(
            format!("http\tGET_body\t{category}\t{}", self.endpoint),
            0,
            error,
            total,
            None,
        );
        trace(
            "GET",
            &self.endpoint,
            &self.location,
            self.range.as_ref(),
            self.status,
            total,
            self.received,
        );
    }
}

impl Stream for CountedBody {
    type Item = object_store::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.inner.as_mut().poll_next(cx);
        match &item {
            Poll::Ready(Some(Ok(bytes))) => self.received += bytes.len() as u64,
            Poll::Ready(Some(Err(_))) => self.finish(true),
            Poll::Ready(None) => self.finish(false),
            Poll::Pending => {}
        }
        item
    }
}

impl Drop for CountedBody {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let category = category(&self.location);
        let missing = self.expected.saturating_sub(self.received);
        record(
            format!("http\tGET_cancelled\t{category}\t{}", self.endpoint),
            missing,
            false,
            self.started.elapsed(),
            None,
        );
        self.finish(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn categorizes_table_objects() {
        for (path, expected) in [
            ("db/t.lance/data/0a1b.lance", "data"),
            ("db/t.lance/_deletions/0-1-2.arrow", "deletion"),
            ("db/t.lance/_indices/9f/index.idx", "index"),
            ("db/t.lance/_versions/1.manifest", "manifest"),
            ("db/t.lance/_transactions/1-x.txn", "transaction"),
            ("db/t.lance/_latest.manifest", "other"),
        ] {
            assert_eq!(category(&Path::from(path)), expected, "{path}");
        }
    }

    #[test]
    fn size_buckets_are_upper_bounds() {
        assert_eq!(bucket(1), 0);
        assert_eq!(bucket(4 << 10), 0);
        assert_eq!(bucket((4 << 10) + 1), 1);
        assert_eq!(bucket(16 << 20), 6);
        assert_eq!(bucket((16 << 20) + 1), 7);
    }
}
