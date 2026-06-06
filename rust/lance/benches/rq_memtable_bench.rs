// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTable index comparison: real `RabitqMemIndex` (reusing the base table's
//! IVF-RQ centroids + quantizer) vs real `HnswMemIndex`. Measures write
//! throughput, read latency (p50/p95/p99), and recall@k vs exact ground truth.
//! `harness = false`.
//!
//! Flow: build a real IVF_RQ index on a base table, extract its centroids +
//! quantizer, feed them to a RabitqMemIndex, insert a held-out memtable
//! incrementally, and search in-memory. HNSW builds on the same memtable.

#![allow(clippy::needless_range_loop)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Write as _};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use lance::dataset::mem_wal::index::{
    HnswMemIndex, RabitqIndexConfig, RabitqMemIndex, mem_wal_hnsw_default,
};
use lance::dataset::{Dataset, WriteParams};
use lance::index::vector::VectorIndexParams;
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};
use lance_arrow::FixedSizeListArrayExt;
use lance_index::IndexType;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::bq::RQRotationType;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

const COL: &str = "vector";

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    base_path: String,
    #[arg(long)]
    query_path: String,
    #[arg(long, default_value_t = 800000)]
    base_size: usize,
    #[arg(long, default_value_t = 100000)]
    memtable_size: usize,
    #[arg(long, default_value_t = 3)]
    num_bits: u8,
    #[arg(long, default_value_t = 4096)]
    nlist: usize,
    #[arg(long, default_value = "8,32,128")]
    nprobe: String,
    #[arg(long, default_value = "64,128,256")]
    ef: String,
    #[arg(long, default_value = "10,100")]
    k: String,
    #[arg(long, default_value_t = 500)]
    queries: usize,
    #[arg(long, default_value_t = 1024)]
    batch: usize,
    #[arg(long, default_value = "rq_memtable_bench.csv")]
    out: String,
    #[arg(long, hide = true, default_value_t = false)]
    bench: bool,
}

fn pl(s: &str) -> Vec<usize> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse().unwrap())
        .collect()
}

fn read_fvecs(path: &str, max_rows: usize) -> (Vec<f32>, usize) {
    let mut f = BufReader::new(File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}")));
    let mut data = Vec::new();
    let mut dim = 0usize;
    let mut buf4 = [0u8; 4];
    let mut rows = 0;
    while rows < max_rows {
        if f.read_exact(&mut buf4).is_err() {
            break;
        }
        let d = i32::from_le_bytes(buf4) as usize;
        if dim == 0 {
            dim = d;
        }
        let mut vbuf = vec![0u8; d * 4];
        f.read_exact(&mut vbuf).expect("truncated fvecs");
        for c in vbuf.chunks_exact(4) {
            data.push(f32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        }
        rows += 1;
    }
    (data, dim)
}

fn exact_topk(base: &[f32], queries: &[f32], dim: usize, k: usize) -> Vec<HashSet<u32>> {
    queries
        .par_chunks(dim)
        .map(|q| {
            let mut best: Vec<(f32, u32)> = base
                .chunks_exact(dim)
                .enumerate()
                .map(|(id, v)| (lance_linalg::distance::l2_f32(q, v), id as u32))
                .collect();
            let kk = k.min(best.len());
            best.select_nth_unstable_by(kk - 1, |a, b| a.0.total_cmp(&b.0));
            best.truncate(kk);
            best.into_iter().map(|(_, id)| id).collect()
        })
        .collect()
}

fn make_fsl(values: &[f32], dim: usize) -> FixedSizeListArray {
    FixedSizeListArray::try_new_from_values(Float32Array::from(values.to_vec()), dim as i32)
        .unwrap()
}

fn schema_of(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        COL,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
        ),
        true,
    )]))
}

fn pctl(us: &[f64], p: f64) -> f64 {
    let mut v = us.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v[(((v.len() as f64 - 1.0) * p).round()) as usize]
}

fn recall(ids: &[u32], gt: &HashSet<u32>, k: usize) -> f64 {
    let kk = k.min(gt.len());
    ids.iter().take(k).filter(|i| gt.contains(i)).count() as f64 / kk as f64
}

/// Batched inserts; returns vec/s build throughput.
fn build_batches(data: &[f32], dim: usize, batch: usize) -> Vec<(RecordBatch, u64)> {
    let n = data.len() / dim;
    let schema = schema_of(dim);
    (0..n)
        .step_by(batch)
        .map(|s| {
            let e = (s + batch).min(n);
            (
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(make_fsl(&data[s * dim..e * dim], dim))],
                )
                .unwrap(),
                s as u64,
            )
        })
        .collect()
}

async fn run(args: Args) {
    let dt = DistanceType::L2;
    let (full, dim) = read_fvecs(&args.base_path, usize::MAX);
    let (fq, dq) = read_fvecs(&args.query_path, args.queries);
    assert_eq!(dim, dq);
    let nq = (fq.len() / dim).min(args.queries);
    let queries = &fq[..nq * dim];
    let total = full.len() / dim;
    let m = args.memtable_size;
    let b = args.base_size.min(total - m);
    let memtable = &full[..m * dim];
    let base = &full[m * dim..(m + b) * dim];
    println!(
        "dim={dim} memtable={m} base={b} queries={nq} num_bits={}",
        args.num_bits
    );

    // ---- Build base IVF_RQ index, extract centroids + quantizer ----
    let tmp = tempfile::TempDir::new().unwrap();
    let schema = schema_of(dim);
    let reader = RecordBatchIterator::new(
        build_batches(base, dim, 50_000)
            .into_iter()
            .map(|(b, _)| Ok(b)),
        schema.clone(),
    );
    let wp = WriteParams {
        max_rows_per_file: b.max(1),
        ..Default::default()
    };
    let mut ds = Dataset::write(reader, tmp.path().to_str().unwrap(), Some(wp))
        .await
        .unwrap();
    let t = Instant::now();
    let params = VectorIndexParams::ivf_rq_with_rotation(
        args.nlist,
        args.num_bits,
        dt,
        RQRotationType::Fast,
    );
    let meta = ds
        .create_index(&[COL], IndexType::Vector, None, &params, true)
        .await
        .unwrap();
    println!("base IVF_RQ built in {:.1}s", t.elapsed().as_secs_f64());
    let idx = ds
        .open_vector_index(COL, &meta.uuid.to_string(), &NoOpMetricsCollector)
        .await
        .unwrap();
    let centroids = idx
        .ivf_model()
        .centroids_array()
        .expect("centroids")
        .clone();
    let quantizer: RabitQuantizer = idx.quantizer().try_into().unwrap();
    println!("extracted {} centroids + quantizer", centroids.len());

    let ks = pl(&args.k);
    let gts: Vec<(usize, Vec<HashSet<u32>>)> = ks
        .iter()
        .map(|&k| (k, exact_topk(memtable, queries, dim, k)))
        .collect();
    let batches = build_batches(memtable, dim, args.batch);

    let mut out = File::create(&args.out).expect("csv");
    writeln!(
        out,
        "method,param,k,recall,build_vec_per_s,p50_us,p95_us,p99_us"
    )
    .unwrap();

    // ---- RabitqMemIndex (reuse base IVF) ----
    let rq = RabitqMemIndex::new(RabitqIndexConfig {
        name: "rq".into(),
        field_id: 0,
        column: COL.into(),
        distance_type: dt,
        centroids,
        quantizer,
    })
    .expect("build RabitqMemIndex");
    let t = Instant::now();
    for (rb, off) in &batches {
        rq.insert(rb, *off).unwrap();
    }
    let rq_build = m as f64 / t.elapsed().as_secs_f64();
    let rq_bytes = rq.code_bytes();
    println!(
        "RabitqMemIndex write: {rq_build:.0} vec/s | code memory: {} MB ({:.0} B/vec)",
        rq_bytes >> 20,
        rq_bytes as f64 / m as f64
    );
    let qfsls: Vec<FixedSizeListArray> = (0..nq)
        .map(|i| make_fsl(&queries[i * dim..(i + 1) * dim], dim))
        .collect();
    // Warm the lazy per-partition storage cache once at the largest nprobe so
    // timed passes measure steady-state read latency. HnswMemIndex builds its
    // whole graph on insert, so it is already warm — this keeps the comparison
    // apples-to-apples rather than charging RQ for first-touch construction.
    let nps = pl(&args.nprobe);
    let max_np = nps.iter().copied().max().unwrap_or(1);
    let warm_k = *ks.iter().max().unwrap_or(&10);
    for qf in &qfsls {
        let _ = rq.search(qf, warm_k, max_np, (m - 1) as u64).unwrap();
    }
    for &np in &nps {
        for (k, gt) in &gts {
            let mut lat = Vec::with_capacity(nq);
            let mut rs = 0.0;
            for (i, qf) in qfsls.iter().enumerate() {
                let t = Instant::now();
                let res = rq.search(qf, *k, np, (m - 1) as u64).unwrap();
                lat.push(t.elapsed().as_secs_f64() * 1e6);
                let ids: Vec<u32> = res.iter().map(|(_, r)| *r as u32).collect();
                rs += recall(&ids, &gt[i], *k);
            }
            writeln!(
                out,
                "rq_ivf,np{np},{k},{:.4},{rq_build:.0},{:.1},{:.1},{:.1}",
                rs / nq as f64,
                pctl(&lat, 0.5),
                pctl(&lat, 0.95),
                pctl(&lat, 0.99)
            )
            .unwrap();
            println!(
                "  rq np={np} k={k}: recall={:.4} p50={:.0}us p99={:.0}us",
                rs / nq as f64,
                pctl(&lat, 0.5),
                pctl(&lat, 0.99)
            );
        }
    }

    // ---- HnswMemIndex (same memtable) ----
    let hnsw = HnswMemIndex::with_capacity(
        0,
        COL.into(),
        dt,
        mem_wal_hnsw_default(),
        m,
        batches.len() + 1,
    );
    let t = Instant::now();
    for (rb, off) in &batches {
        hnsw.insert(rb, *off).unwrap();
    }
    let hnsw_build = m as f64 / t.elapsed().as_secs_f64();
    // HNSW stores raw f32 vectors (FLAT) plus the graph; report the f32 lower
    // bound, which is the term RaBitQ codes replace.
    let hnsw_f32 = m * dim * 4;
    println!(
        "HnswMemIndex write: {hnsw_build:.0} vec/s | f32 vector storage: {} MB ({} B/vec) + graph",
        hnsw_f32 >> 20,
        dim * 4
    );
    for &ef in &pl(&args.ef) {
        for (k, gt) in &gts {
            if ef < *k {
                continue;
            }
            let mut lat = Vec::with_capacity(nq);
            let mut rs = 0.0;
            for (i, qf) in qfsls.iter().enumerate() {
                let t = Instant::now();
                let res = hnsw.search(qf, *k, Some(ef), (m - 1) as u64).unwrap();
                lat.push(t.elapsed().as_secs_f64() * 1e6);
                let ids: Vec<u32> = res.iter().map(|(_, r)| *r as u32).collect();
                rs += recall(&ids, &gt[i], *k);
            }
            writeln!(
                out,
                "hnsw,ef{ef},{k},{:.4},{hnsw_build:.0},{:.1},{:.1},{:.1}",
                rs / nq as f64,
                pctl(&lat, 0.5),
                pctl(&lat, 0.95),
                pctl(&lat, 0.99)
            )
            .unwrap();
            println!(
                "  hnsw ef={ef} k={k}: recall={:.4} p50={:.0}us p99={:.0}us",
                rs / nq as f64,
                pctl(&lat, 0.5),
                pctl(&lat, 0.99)
            );
        }
    }
    println!("wrote {}", args.out);
}

fn main() {
    let args = Args::parse();
    tokio::runtime::Runtime::new().unwrap().block_on(run(args));
}
