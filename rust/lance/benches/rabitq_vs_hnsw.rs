// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTable vector-index benchmark: real in-memory HNSW (`HnswMemIndex`) vs
//! in-memory 1-bit RaBitQ + f32 rerank, built on the public
//! `lance_index::vector::bq` APIs (the same estimator the persisted `IVF_RQ`
//! uses). Two RaBitQ variants are measured:
//!
//! - `rq_flat`: single global centroid, full flat scan ("no IVF" idea).
//! - `rq_ivf`: residuals against reused base IVF centroids (k-means on the full
//!   base, simulating the base table's existing index), nprobe partition scan.
//!   Intended for the "base table already has IVF-RQ" case.
//!
//! Plus a `brute` (exact f32 scan) baseline. Reports build throughput, single-
//! query latency p50/p95/p99 + QPS, recall@k vs exact ground truth, and index
//! overhead bytes — across memtable-realistic N / dim / metric.
//!
//! Cosine is run as normalized-L2 (identical ranking). `harness = false`.
//!
//! Example:
//!   cargo bench --bench rabitq_vs_hnsw -p lance -- \
//!     --dataset sift --base-path sift_base.fvecs --query-path sift_query.fvecs \
//!     --sizes 100000,400000 --nlist 1024 --nprobe 4,16,64 --out results.csv

#![allow(clippy::needless_range_loop)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use lance_arrow::FixedSizeListArrayExt;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

use lance::dataset::mem_wal::index::{HnswMemIndex, mem_wal_hnsw_default};
use lance_core::ROW_ID;
use lance_index::vector::bq::RQRotationType;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::storage::RabitQuantizationStorage;
use lance_index::vector::bq::transform::RQTransformer;
use lance_index::vector::kmeans::{KMeansParams, compute_partition, train_kmeans};
use lance_index::vector::quantizer::{Quantization, QuantizerStorage};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_index::vector::transform::Transformer;
use lance_index::vector::{CENTROID_DIST_COLUMN, PART_ID_COLUMN};

const VECTOR_COLUMN: &str = "vector";

#[derive(Parser, Debug, Clone)]
#[command(about = "MemTable HNSW vs RaBitQ (flat / base-IVF) benchmark")]
struct Args {
    #[arg(long, default_value = "synthetic")]
    dataset: String,
    #[arg(long)]
    base_path: Option<String>,
    #[arg(long)]
    query_path: Option<String>,
    #[arg(long, default_value = "25000,50000,100000,200000,400000")]
    sizes: String,
    #[arg(long, default_value_t = 768)]
    dim: usize,
    #[arg(long, default_value_t = 256)]
    clusters: usize,
    #[arg(long, default_value = "l2")]
    metric: String,
    #[arg(long, default_value = "10,100")]
    k: String,
    #[arg(long, default_value_t = 1000)]
    queries: usize,
    #[arg(long, default_value_t = 1024)]
    batch: usize,
    /// Comma-separated variants to run: hnsw, brute, rq_flat, rq_ivf.
    #[arg(long, default_value = "hnsw,brute,rq_flat,rq_ivf")]
    variants: String,
    #[arg(long, default_value = "64,128,256,512")]
    ef: String,
    #[arg(long, default_value = "1,2,3,5,10")]
    overfetch: String,
    #[arg(long, default_value = "fast")]
    rotation: String,
    /// rq_flat centroid policy: zero, mean, or both.
    #[arg(long, default_value = "mean")]
    centroid: String,
    /// rq_ivf partition counts (base IVF nlist) to sweep.
    #[arg(long, default_value = "1024")]
    nlist: String,
    /// rq_ivf nprobe values to sweep.
    #[arg(long, default_value = "1,4,16,64")]
    nprobe: String,
    /// k-means sample rate (vectors per centroid) for centroid training.
    #[arg(long, default_value_t = 256)]
    sample_rate: usize,
    #[arg(long, default_value = "rabitq_vs_hnsw.csv")]
    out: String,
    #[arg(long, default_value_t = 1_000_000)]
    synthetic_n: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Ignored flag that `cargo bench` injects after `--`.
    #[arg(long, hide = true, default_value_t = false)]
    bench: bool,
}

fn parse_usize_list(s: &str) -> Vec<usize> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse().expect("bad int"))
        .collect()
}
fn parse_str_list(s: &str) -> Vec<String> {
    s.split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().to_string())
        .collect()
}

// ----- deterministic RNG (splitmix64) -----
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    fn next_normal(&mut self) -> f32 {
        let u1 = (self.next_f32() as f64).max(1e-9);
        let u2 = self.next_f32() as f64;
        (((-2.0 * u1.ln()).sqrt()) * (std::f64::consts::TAU * u2).cos()) as f32
    }
}

// ----- data -----
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
        assert_eq!(d, dim, "inconsistent fvecs dim");
        let mut vbuf = vec![0u8; d * 4];
        f.read_exact(&mut vbuf).expect("truncated fvecs");
        for c in vbuf.chunks_exact(4) {
            data.push(f32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        }
        rows += 1;
    }
    (data, dim)
}

fn gen_synthetic(
    n: usize,
    dim: usize,
    clusters: usize,
    center_seed: u64,
    sample_seed: u64,
) -> Vec<f32> {
    let mut crng = Rng(center_seed);
    let mut centers = vec![0f32; clusters * dim];
    for v in centers.iter_mut() {
        *v = crng.next_normal() * 4.0;
    }
    let mut rng = Rng(sample_seed);
    let mut data = vec![0f32; n * dim];
    for i in 0..n {
        let c = (rng.next_u64() as usize) % clusters;
        for j in 0..dim {
            data[i * dim + j] = centers[c * dim + j] + rng.next_normal();
        }
    }
    data
}

fn normalize_inplace(data: &mut [f32], dim: usize) {
    data.par_chunks_mut(dim).for_each(|v| {
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
    });
}

fn exact_topk(base: &[f32], queries: &[f32], dim: usize, k: usize) -> Vec<Vec<u32>> {
    queries
        .par_chunks(dim)
        .map(|q| {
            let mut best: Vec<(f32, u32)> = base
                .chunks_exact(dim)
                .enumerate()
                .map(|(id, v)| (lance_linalg::distance::l2_distance(q, v), id as u32))
                .collect();
            let kk = k.min(best.len());
            best.select_nth_unstable_by(kk - 1, |a, b| a.0.total_cmp(&b.0));
            best.truncate(kk);
            best.sort_by(|a, b| a.0.total_cmp(&b.0));
            best.into_iter().map(|(_, id)| id).collect()
        })
        .collect()
}

fn recall(approx: &[u32], truth: &[u32], k: usize) -> f64 {
    let kk = k.min(truth.len());
    let t: std::collections::HashSet<u32> = truth.iter().take(kk).copied().collect();
    approx.iter().take(kk).filter(|id| t.contains(id)).count() as f64 / kk as f64
}

fn make_fsl(values: &[f32], dim: usize) -> FixedSizeListArray {
    FixedSizeListArray::try_new_from_values(Float32Array::from(values.to_vec()), dim as i32)
        .unwrap()
}

fn percentiles(mut us: Vec<f64>) -> (f64, f64, f64, f64) {
    us.sort_by(|a, b| a.total_cmp(b));
    let pick = |p: f64| us[(((us.len() as f64 - 1.0) * p).round()) as usize];
    let mean = us.iter().sum::<f64>() / us.len() as f64;
    (pick(0.50), pick(0.95), pick(0.99), mean)
}

// ----- HNSW (real HnswMemIndex) -----
fn build_hnsw(base: &[f32], dim: usize, dt: DistanceType, batch: usize) -> (HnswMemIndex, f64) {
    let n = base.len() / dim;
    let num_batches = n.div_ceil(batch);
    let idx = HnswMemIndex::with_capacity(
        0,
        VECTOR_COLUMN.to_string(),
        dt,
        mem_wal_hnsw_default(),
        n,
        num_batches + 1,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        VECTOR_COLUMN,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
        ),
        true,
    )]));
    let batches: Vec<(RecordBatch, u64)> = (0..num_batches)
        .map(|b| {
            let (start, end) = (b * batch, ((b + 1) * batch).min(n));
            let fsl = make_fsl(&base[start * dim..end * dim], dim);
            (
                RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap(),
                start as u64,
            )
        })
        .collect();
    let t = Instant::now();
    for (rb, offset) in &batches {
        idx.insert(rb, *offset).unwrap();
    }
    (idx, n as f64 / t.elapsed().as_secs_f64())
}

fn search_hnsw(
    idx: &HnswMemIndex,
    query: &FixedSizeListArray,
    k: usize,
    ef: usize,
    max_pos: u64,
) -> Vec<u32> {
    idx.search(query, k, Some(ef), max_pos)
        .unwrap()
        .into_iter()
        .map(|(_, pos)| pos as u32)
        .collect()
}

// ----- RaBitQ shared: quantize a set of vectors against one centroid -----
fn quantize_partition(
    vectors: &[f32],
    global_ids: &[u64],
    centroid: &[f32],
    dim: usize,
    dt: DistanceType,
    rq: &RabitQuantizer,
) -> RabitQuantizationStorage {
    let m = global_ids.len();
    let transformer = RQTransformer::new(rq.clone(), dt, make_fsl(centroid, dim), VECTOR_COLUMN);
    let mut residual = vec![0f32; m * dim];
    let mut cdist = vec![0f32; m];
    for i in 0..m {
        let v = &vectors[i * dim..(i + 1) * dim];
        let mut s = 0f32;
        for j in 0..dim {
            let r = v[j] - centroid[j];
            residual[i * dim + j] = r;
            s += r * r;
        }
        cdist[i] = s;
    }
    let in_schema = Arc::new(Schema::new(vec![
        Field::new(
            VECTOR_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
        Field::new(CENTROID_DIST_COLUMN, DataType::Float32, false),
        Field::new(PART_ID_COLUMN, DataType::UInt32, false),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));
    let in_batch = RecordBatch::try_new(
        in_schema,
        vec![
            Arc::new(make_fsl(&residual, dim)),
            Arc::new(Float32Array::from(cdist)),
            Arc::new(UInt32Array::from(vec![0u32; m])),
            Arc::new(UInt64Array::from(global_ids.to_vec())),
        ],
    )
    .unwrap();
    let coded = transformer.transform(&in_batch).unwrap();
    RabitQuantizationStorage::try_from_batch(coded, &rq.metadata(None), dt, None).unwrap()
}

fn rerank(base: &[f32], query: &[f32], dim: usize, cand: Vec<(f32, u32)>, k: usize) -> Vec<u32> {
    let mut reranked: Vec<(f32, u32)> = cand
        .iter()
        .map(|&(_, id)| {
            (
                lance_linalg::distance::l2_distance(
                    query,
                    &base[id as usize * dim..(id as usize + 1) * dim],
                ),
                id,
            )
        })
        .collect();
    let kk = k.min(reranked.len());
    if kk == 0 {
        return vec![];
    }
    reranked.select_nth_unstable_by(kk - 1, |a, b| a.0.total_cmp(&b.0));
    reranked.truncate(kk);
    reranked.sort_by(|a, b| a.0.total_cmp(&b.0));
    reranked.into_iter().map(|(_, id)| id).collect()
}

// ----- rq_flat: single centroid, full scan -----
struct RqFlat {
    storage: RabitQuantizationStorage,
    centroid: Vec<f32>,
    dim: usize,
    n: usize,
}
fn build_rq_flat(
    base: &[f32],
    dim: usize,
    dt: DistanceType,
    rot: RQRotationType,
    centroid: Vec<f32>,
) -> (RqFlat, f64) {
    let n = base.len() / dim;
    let rq = RabitQuantizer::new_with_rotation::<Float32Type>(1, dim as i32, rot);
    let ids: Vec<u64> = (0..n as u64).collect();
    let t = Instant::now();
    let storage = quantize_partition(base, &ids, &centroid, dim, dt, &rq);
    (
        RqFlat {
            storage,
            centroid,
            dim,
            n,
        },
        n as f64 / t.elapsed().as_secs_f64(),
    )
}
fn search_rq_flat(
    idx: &RqFlat,
    base: &[f32],
    query: &[f32],
    k: usize,
    overfetch: usize,
) -> Vec<u32> {
    let dim = idx.dim;
    let mut qr = vec![0f32; dim];
    let mut dqc = 0f32;
    for j in 0..dim {
        let r = query[j] - idx.centroid[j];
        qr[j] = r;
        dqc += r * r;
    }
    let calc = idx
        .storage
        .dist_calculator(Arc::new(Float32Array::from(qr)) as ArrayRef, dqc);
    let cand_n = (overfetch * k).min(idx.n);
    let mut est: Vec<(f32, u32)> = (0..idx.n as u32)
        .map(|id| (calc.distance(id), id))
        .collect();
    est.select_nth_unstable_by(cand_n - 1, |a, b| a.0.total_cmp(&b.0));
    est.truncate(cand_n);
    rerank(base, query, dim, est, k)
}

// ----- rq_ivf: reuse base IVF centroids, nprobe scan -----
struct RqIvf {
    centroids: Vec<f32>, // nlist*dim
    nlist: usize,
    storages: Vec<Option<RabitQuantizationStorage>>,
    dim: usize,
}
fn build_rq_ivf(
    base: &[f32],
    dim: usize,
    dt: DistanceType,
    rot: RQRotationType,
    centroids: &[f32],
    nlist: usize,
) -> (RqIvf, f64) {
    let n = base.len() / dim;
    let rq = RabitQuantizer::new_with_rotation::<Float32Type>(1, dim as i32, rot);
    let t = Instant::now();
    // Assign each vector to nearest centroid (base centroids reused -> not k-means time).
    let mut assign = vec![0u32; n];
    assign.par_iter_mut().enumerate().for_each(|(i, a)| {
        *a = compute_partition::<f32>(centroids, &base[i * dim..(i + 1) * dim], dt).unwrap_or(0);
    });
    let mut groups: Vec<Vec<u32>> = vec![Vec::new(); nlist];
    for (i, &p) in assign.iter().enumerate() {
        groups[p as usize].push(i as u32);
    }
    let storages: Vec<Option<RabitQuantizationStorage>> = groups
        .iter()
        .enumerate()
        .map(|(p, ids)| {
            if ids.is_empty() {
                return None;
            }
            let mut vecs = vec![0f32; ids.len() * dim];
            let mut gids = vec![0u64; ids.len()];
            for (slot, &gid) in ids.iter().enumerate() {
                vecs[slot * dim..(slot + 1) * dim]
                    .copy_from_slice(&base[gid as usize * dim..(gid as usize + 1) * dim]);
                gids[slot] = gid as u64;
            }
            Some(quantize_partition(
                &vecs,
                &gids,
                &centroids[p * dim..(p + 1) * dim],
                dim,
                dt,
                &rq,
            ))
        })
        .collect();
    (
        RqIvf {
            centroids: centroids.to_vec(),
            nlist,
            storages,
            dim,
        },
        n as f64 / t.elapsed().as_secs_f64(),
    )
}
fn search_rq_ivf(
    idx: &RqIvf,
    base: &[f32],
    query: &[f32],
    k: usize,
    nprobe: usize,
    overfetch: usize,
) -> Vec<u32> {
    let dim = idx.dim;
    // Pick nprobe nearest centroids.
    let mut cdist: Vec<(f32, usize)> = (0..idx.nlist)
        .map(|p| {
            (
                lance_linalg::distance::l2_distance(query, &idx.centroids[p * dim..(p + 1) * dim]),
                p,
            )
        })
        .collect();
    let np = nprobe.min(idx.nlist);
    cdist.select_nth_unstable_by(np - 1, |a, b| a.0.total_cmp(&b.0));
    cdist.truncate(np);

    let mut cand: Vec<(f32, u32)> = Vec::new();
    for &(dqc, p) in &cdist {
        let Some(storage) = &idx.storages[p] else {
            continue;
        };
        let c = &idx.centroids[p * dim..(p + 1) * dim];
        let qr: Vec<f32> = (0..dim).map(|j| query[j] - c[j]).collect();
        let calc = storage.dist_calculator(Arc::new(Float32Array::from(qr)) as ArrayRef, dqc);
        for local in 0..storage.len() as u32 {
            cand.push((calc.distance(local), storage.row_id(local) as u32));
        }
    }
    let cand_n = (overfetch * k).min(cand.len());
    if cand_n == 0 {
        return vec![];
    }
    cand.select_nth_unstable_by(cand_n - 1, |a, b| a.0.total_cmp(&b.0));
    cand.truncate(cand_n);
    rerank(base, query, dim, cand, k)
}

fn search_brute(base: &[f32], query: &[f32], dim: usize, k: usize) -> Vec<u32> {
    let n = base.len() / dim;
    let mut d: Vec<(f32, u32)> = (0..n)
        .map(|i| {
            (
                lance_linalg::distance::l2_distance(query, &base[i * dim..(i + 1) * dim]),
                i as u32,
            )
        })
        .collect();
    let kk = k.min(n);
    d.select_nth_unstable_by(kk - 1, |a, b| a.0.total_cmp(&b.0));
    d.truncate(kk);
    d.sort_by(|a, b| a.0.total_cmp(&b.0));
    d.into_iter().map(|(_, id)| id).collect()
}

fn main() {
    let args = Args::parse();
    let dt = DistanceType::L2;
    let normalize = args.metric == "cosine";

    let (mut full_base, dim, mut full_queries) = match args.dataset.as_str() {
        "synthetic" => {
            let b = gen_synthetic(
                args.synthetic_n,
                args.dim,
                args.clusters,
                args.seed,
                args.seed + 1,
            );
            let q = gen_synthetic(
                args.queries,
                args.dim,
                args.clusters,
                args.seed,
                args.seed + 2,
            );
            (b, args.dim, q)
        }
        "sift" | "gist" => {
            let bp = args.base_path.clone().expect("--base-path required");
            let qp = args.query_path.clone().expect("--query-path required");
            let (b, d) = read_fvecs(&bp, usize::MAX);
            let (q, dq) = read_fvecs(&qp, args.queries);
            assert_eq!(d, dq, "base/query dim mismatch");
            (b, d, q)
        }
        other => panic!("unknown dataset {other}"),
    };
    assert!(dim % 8 == 0, "RaBitQ requires dim % 8 == 0, got {dim}");
    if normalize {
        normalize_inplace(&mut full_base, dim);
        normalize_inplace(&mut full_queries, dim);
    }
    let n_queries = (full_queries.len() / dim).min(args.queries);
    let queries = &full_queries[..n_queries * dim];

    let sizes = parse_usize_list(&args.sizes);
    let ks = parse_usize_list(&args.k);
    let efs = parse_usize_list(&args.ef);
    let overfetches = parse_usize_list(&args.overfetch);
    let nlists = parse_usize_list(&args.nlist);
    let nprobes = parse_usize_list(&args.nprobe);
    let variants = parse_str_list(&args.variants);
    let has = |v: &str| variants.iter().any(|x| x == v);
    let rotations: Vec<(&str, RQRotationType)> = match args.rotation.as_str() {
        "fast" => vec![("fast", RQRotationType::Fast)],
        "matrix" => vec![("matrix", RQRotationType::Matrix)],
        "both" => vec![
            ("fast", RQRotationType::Fast),
            ("matrix", RQRotationType::Matrix),
        ],
        o => panic!("unknown rotation {o}"),
    };
    let centroid_policies = parse_str_list(&args.centroid);

    println!(
        "dataset={} dim={} metric={} base_total={} queries={} variants={:?}",
        args.dataset,
        dim,
        args.metric,
        full_base.len() / dim,
        n_queries,
        variants
    );

    // Train base IVF centroids once per nlist on the FULL base (reused, not in build time).
    let mut centroid_sets: Vec<(usize, Vec<f32>)> = Vec::new();
    if has("rq_ivf") {
        for &nlist in &nlists {
            let t = Instant::now();
            let params = KMeansParams {
                distance_type: dt,
                ..Default::default()
            };
            let km = train_kmeans::<Float32Type>(
                &Float32Array::from(full_base.clone()),
                params,
                dim,
                nlist,
                args.sample_rate,
            )
            .unwrap();
            let c = km.centroids.as_primitive::<Float32Type>().values().to_vec();
            println!(
                "  trained nlist={nlist} centroids in {:.1}s",
                t.elapsed().as_secs_f64()
            );
            centroid_sets.push((nlist, c));
        }
    }

    let mut out = File::create(&args.out).expect("create csv");
    writeln!(
        out,
        "dataset,n,dim,metric,method,variant,k,param,build_vec_per_s,p50_us,p95_us,p99_us,mean_us,qps,recall,index_overhead_bytes"
    )
    .unwrap();
    let mut row = |method: &str,
                   n,
                   k,
                   variant: &str,
                   param: &str,
                   build: f64,
                   lat: Vec<f64>,
                   rsum: f64,
                   nq: usize,
                   bytes: u64| {
        let (p50, p95, p99, mean) = percentiles(lat);
        writeln!(
            out,
            "{},{},{},{},{},{},{},{},{:.0},{:.2},{:.2},{:.2},{:.2},{:.0},{:.4},{}",
            args.dataset,
            n,
            dim,
            args.metric,
            method,
            variant,
            k,
            param,
            build,
            p50,
            p95,
            p99,
            mean,
            1e6 / mean,
            rsum / nq as f64,
            bytes
        )
        .unwrap();
    };

    for &n in &sizes {
        let total = full_base.len() / dim;
        if n > total {
            eprintln!("skip n={n} > base {total}");
            continue;
        }
        let base = &full_base[..n * dim];
        let max_pos = (n - 1) as u64;
        println!("== N={n} ==");
        let gt: Vec<(usize, Vec<Vec<u32>>)> = ks
            .iter()
            .map(|&k| (k, exact_topk(base, queries, dim, k)))
            .collect();
        let qf: Vec<FixedSizeListArray> = (0..n_queries)
            .map(|i| make_fsl(&queries[i * dim..(i + 1) * dim], dim))
            .collect();
        let code_bytes = dim.div_ceil(8) as u64;

        if has("brute") {
            for (k, gtk) in &gt {
                let mut lat = Vec::new();
                let mut rs = 0.0;
                for i in 0..n_queries {
                    let t = Instant::now();
                    let r = search_brute(base, &queries[i * dim..(i + 1) * dim], dim, *k);
                    lat.push(t.elapsed().as_secs_f64() * 1e6);
                    rs += recall(&r, &gtk[i], *k);
                }
                row(
                    "brute",
                    n,
                    *k,
                    "",
                    "-",
                    0.0,
                    lat,
                    rs,
                    n_queries,
                    (n as u64) * (dim as u64) * 4,
                );
            }
        }
        if has("hnsw") {
            let (h, build) = build_hnsw(base, dim, dt, args.batch);
            let bytes = (n as f64 * 16.0 * 3.0 * 4.0) as u64;
            for (k, gtk) in &gt {
                for &ef in &efs {
                    if ef < *k {
                        continue;
                    }
                    let mut lat = Vec::new();
                    let mut rs = 0.0;
                    for (i, q) in qf.iter().enumerate() {
                        let t = Instant::now();
                        let r = search_hnsw(&h, q, *k, ef, max_pos);
                        lat.push(t.elapsed().as_secs_f64() * 1e6);
                        rs += recall(&r, &gtk[i], *k);
                    }
                    row(
                        "hnsw",
                        n,
                        *k,
                        "",
                        &format!("ef{ef}"),
                        build,
                        lat,
                        rs,
                        n_queries,
                        bytes,
                    );
                }
            }
            println!("  hnsw build {build:.0} vec/s");
        }
        if has("rq_flat") {
            for cp in &centroid_policies {
                let centroid = match cp.as_str() {
                    "zero" => vec![0f32; dim],
                    "mean" => {
                        let mut c = vec![0f64; dim];
                        for v in base.chunks_exact(dim) {
                            for j in 0..dim {
                                c[j] += v[j] as f64;
                            }
                        }
                        c.iter().map(|x| (x / n as f64) as f32).collect()
                    }
                    o => panic!("bad centroid {o}"),
                };
                for (rn, rot) in &rotations {
                    let (idx, build) = build_rq_flat(base, dim, dt, *rot, centroid.clone());
                    let bytes = n as u64 * (code_bytes + 8);
                    for (k, gtk) in &gt {
                        for &of in &overfetches {
                            let mut lat = Vec::new();
                            let mut rs = 0.0;
                            for i in 0..n_queries {
                                let t = Instant::now();
                                let r = search_rq_flat(
                                    &idx,
                                    base,
                                    &queries[i * dim..(i + 1) * dim],
                                    *k,
                                    of,
                                );
                                lat.push(t.elapsed().as_secs_f64() * 1e6);
                                rs += recall(&r, &gtk[i], *k);
                            }
                            row(
                                "rq_flat",
                                n,
                                *k,
                                &format!("{rn}+{cp}"),
                                &format!("of{of}"),
                                build,
                                lat,
                                rs,
                                n_queries,
                                bytes,
                            );
                        }
                    }
                    println!("  rq_flat {rn}+{cp} build {build:.0} vec/s");
                }
            }
        }
        if has("rq_ivf") {
            for (nlist, centroids) in &centroid_sets {
                for (rn, rot) in &rotations {
                    let (idx, build) = build_rq_ivf(base, dim, dt, *rot, centroids, *nlist);
                    let bytes = n as u64 * (code_bytes + 8);
                    for (k, gtk) in &gt {
                        for &np in &nprobes {
                            for &of in &overfetches {
                                let mut lat = Vec::new();
                                let mut rs = 0.0;
                                for i in 0..n_queries {
                                    let t = Instant::now();
                                    let r = search_rq_ivf(
                                        &idx,
                                        base,
                                        &queries[i * dim..(i + 1) * dim],
                                        *k,
                                        np,
                                        of,
                                    );
                                    lat.push(t.elapsed().as_secs_f64() * 1e6);
                                    rs += recall(&r, &gtk[i], *k);
                                }
                                row(
                                    "rq_ivf",
                                    n,
                                    *k,
                                    &format!("{rn}+nlist{nlist}"),
                                    &format!("np{np}_of{of}"),
                                    build,
                                    lat,
                                    rs,
                                    n_queries,
                                    bytes,
                                );
                            }
                        }
                    }
                    println!("  rq_ivf {rn}+nlist{nlist} build {build:.0} vec/s");
                }
            }
        }
    }
    println!("wrote {}", args.out);
}
