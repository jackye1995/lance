// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Verify "high-quality HNSW graph + low-bit RaBitQ search": build one HNSW graph
//! with full-precision (f32) distances, then search the SAME graph using RaBitQ
//! stores at 1/3/5 bits over the same vector ids. The graph is storage-agnostic
//! (adjacency over ids), so build and search can use different stores. f32 build
//! is an upper bound on a 5-bit-built graph's quality, so this answers whether
//! 1-bit search can ride a high-quality graph. `harness = false`.

#![allow(clippy::needless_range_loop)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Write as _};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::types::Float32Type;
use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::ROW_ID;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::storage::RabitQuantizationStorage;
use lance_index::vector::flat::storage::FlatFloatStorage;
use lance_index::vector::hnsw::HNSW;
use lance_index::vector::hnsw::builder::{HnswBuildParams, HnswQueryParams};
use lance_index::vector::ivf::new_ivf_transformer_with_quantizer;
use lance_index::vector::quantizer::{Quantization, Quantizer, QuantizerStorage};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_index::vector::transform::Transformer;
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

const COL: &str = "vector";

#[derive(Parser, Debug, Clone)]
struct Args {
    #[arg(long)]
    base_path: String,
    #[arg(long)]
    query_path: String,
    #[arg(long, default_value_t = 100000)]
    memtable_size: usize,
    /// RaBitQ search bit-widths to evaluate (separate stores).
    #[arg(long, default_value = "1,3,5")]
    search_bits: String,
    #[arg(long, default_value = "64,128,256")]
    ef: String,
    #[arg(long, default_value = "10,100")]
    k: String,
    #[arg(long, default_value_t = 500)]
    queries: usize,
    #[arg(long, default_value_t = 16)]
    m: usize,
    #[arg(long, default_value_t = 100)]
    ef_construction: usize,
    #[arg(long, default_value = "rq_hnsw_bench.csv")]
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

fn pctl(us: &[f64], p: f64) -> f64 {
    let mut v = us.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    v[(((v.len() as f64 - 1.0) * p).round()) as usize]
}

fn recall(ids: &[u32], gt: &HashSet<u32>, k: usize) -> f64 {
    let kk = k.min(gt.len());
    ids.iter().take(k).filter(|i| gt.contains(i)).count() as f64 / kk as f64
}

/// Build a single-centroid (mean) RaBitQ store over `vecs`; returns the store and
/// its code-batch bytes. Codes are quantized residuals vs the mean, matching the
/// `q - mean` residual the search passes.
fn build_rabitq_store(
    vecs: &[f32],
    dim: usize,
    n: usize,
    num_bits: u8,
    mean: &[f32],
) -> (RabitQuantizationStorage, usize) {
    let centroid = make_fsl(mean, dim);
    let quantizer = RabitQuantizer::new::<Float32Type>(num_bits, dim as i32);
    let transformer = new_ivf_transformer_with_quantizer(
        centroid,
        DistanceType::L2,
        COL,
        Quantizer::Rabit(quantizer.clone()),
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            COL,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            true,
        ),
        Field::new(ROW_ID, DataType::UInt64, false),
    ]));
    let in_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(make_fsl(&vecs[..n * dim], dim)),
            Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let coded = transformer.transform(&in_batch).unwrap();
    let bytes = coded.get_array_memory_size();
    let storage = RabitQuantizationStorage::try_from_batch(
        coded,
        &quantizer.metadata(None),
        DistanceType::L2,
        None,
    )
    .unwrap();
    (storage, bytes)
}

fn main() {
    let args = Args::parse();
    let dt = DistanceType::L2;
    let (full, dim) = read_fvecs(&args.base_path, args.memtable_size);
    let (fq, dq) = read_fvecs(&args.query_path, args.queries);
    assert_eq!(dim, dq);
    let n = full.len() / dim;
    let nq = (fq.len() / dim).min(args.queries);
    let queries = &fq[..nq * dim];
    println!(
        "dim={dim} memtable={n} queries={nq} m={} efc={}",
        args.m, args.ef_construction
    );

    let ks = pl(&args.k);
    let gts: Vec<(usize, Vec<HashSet<u32>>)> = ks
        .iter()
        .map(|&k| (k, exact_topk(&full, queries, dim, k)))
        .collect();

    // Mean centroid for the RaBitQ residual.
    let mut mean = vec![0f32; dim];
    for i in 0..n {
        for j in 0..dim {
            mean[j] += full[i * dim + j];
        }
    }
    for j in 0..dim {
        mean[j] /= n as f32;
    }

    // Build the HNSW graph with full-precision (f32) distances.
    let flat = FlatFloatStorage::new(make_fsl(&full[..n * dim], dim), dt);
    let bp = HnswBuildParams::default()
        .num_edges(args.m)
        .ef_construction(args.ef_construction);
    let t = Instant::now();
    let hnsw = HNSW::index_vectors(&flat, bp).unwrap();
    let build = n as f64 / t.elapsed().as_secs_f64();
    println!(
        "HNSW f32 graph built: {build:.0} vec/s ({:.1}s)",
        t.elapsed().as_secs_f64()
    );

    let mut out = File::create(&args.out).expect("csv");
    writeln!(out, "method,ef,k,recall,bytes_per_vec,p50_us,p95_us,p99_us").unwrap();

    let efs = pl(&args.ef);
    let q_raw: Vec<ArrayRef> = (0..nq)
        .map(|i| Arc::new(Float32Array::from(queries[i * dim..(i + 1) * dim].to_vec())) as ArrayRef)
        .collect();

    // Baseline: f32 graph + f32 search.
    let f32_bytes = dim * 4;
    for &ef in &efs {
        for (k, gt) in &gts {
            if ef < *k {
                continue;
            }
            let mut lat = Vec::with_capacity(nq);
            let mut rs = 0.0;
            for (i, q) in q_raw.iter().enumerate() {
                let params = HnswQueryParams {
                    ef,
                    lower_bound: None,
                    upper_bound: None,
                    dist_q_c: 0.0,
                };
                let t = Instant::now();
                let res = hnsw
                    .search_basic(q.clone(), *k, &params, None, &flat)
                    .unwrap();
                lat.push(t.elapsed().as_secs_f64() * 1e6);
                let ids: Vec<u32> = res.iter().map(|nd| nd.id).collect();
                rs += recall(&ids, &gt[i], *k);
            }
            writeln!(
                out,
                "f32,{ef},{k},{:.4},{f32_bytes},{:.1},{:.1},{:.1}",
                rs / nq as f64,
                pctl(&lat, 0.5),
                pctl(&lat, 0.95),
                pctl(&lat, 0.99)
            )
            .unwrap();
            println!(
                "  f32 ef={ef} k={k}: recall={:.4} p50={:.0}us",
                rs / nq as f64,
                pctl(&lat, 0.5)
            );
        }
    }

    // RaBitQ search at each bit-width over the SAME f32-built graph.
    // Pre-residualize queries vs the mean: dist_calculator expects qr = q - c.
    let q_res: Vec<(ArrayRef, f32)> = (0..nq)
        .map(|i| {
            let q = &queries[i * dim..(i + 1) * dim];
            let mut r = vec![0f32; dim];
            let mut s = 0f32;
            for j in 0..dim {
                r[j] = q[j] - mean[j];
                s += r[j] * r[j];
            }
            (Arc::new(Float32Array::from(r)) as ArrayRef, s)
        })
        .collect();

    for nb in pl(&args.search_bits) {
        let (store, bytes) = build_rabitq_store(&full, dim, n, nb as u8, &mean);
        let bpv = bytes / n;
        // Brute-force flat RaBitQ recall (scan all, no graph) — isolates the
        // quantizer's accuracy from any HNSW-traversal degradation.
        for (k, gt) in &gts {
            let mut rs = 0.0;
            for (i, (qr, dist_q_c)) in q_res.iter().enumerate() {
                let calc = store.dist_calculator(qr.clone(), *dist_q_c);
                let dists = calc.distance_all(0);
                let mut cand: Vec<(f32, u32)> = dists
                    .iter()
                    .enumerate()
                    .map(|(p, &d)| (d, store.row_id(p as u32) as u32))
                    .collect();
                let kk = (*k).min(cand.len());
                cand.select_nth_unstable_by(kk - 1, |a, b| a.0.total_cmp(&b.0));
                cand.truncate(kk);
                let ids: Vec<u32> = cand.iter().map(|(_, id)| *id).collect();
                rs += recall(&ids, &gt[i], *k);
            }
            writeln!(
                out,
                "rq{nb}_flat,0,{k},{:.4},{bpv},0.0,0.0,0.0",
                rs / nq as f64
            )
            .unwrap();
            println!(
                "  rq{nb}_flat k={k}: recall={:.4} (brute-force)",
                rs / nq as f64
            );
        }
        for &ef in &efs {
            for (k, gt) in &gts {
                if ef < *k {
                    continue;
                }
                let mut lat = Vec::with_capacity(nq);
                let mut rs = 0.0;
                for (i, (qr, dist_q_c)) in q_res.iter().enumerate() {
                    let params = HnswQueryParams {
                        ef,
                        lower_bound: None,
                        upper_bound: None,
                        dist_q_c: *dist_q_c,
                    };
                    let t = Instant::now();
                    let res = hnsw
                        .search_basic(qr.clone(), *k, &params, None, &store)
                        .unwrap();
                    lat.push(t.elapsed().as_secs_f64() * 1e6);
                    let ids: Vec<u32> = res.iter().map(|nd| nd.id).collect();
                    rs += recall(&ids, &gt[i], *k);
                }
                writeln!(
                    out,
                    "rq{nb},{ef},{k},{:.4},{bpv},{:.1},{:.1},{:.1}",
                    rs / nq as f64,
                    pctl(&lat, 0.5),
                    pctl(&lat, 0.95),
                    pctl(&lat, 0.99)
                )
                .unwrap();
                println!(
                    "  rq{nb} ef={ef} k={k}: recall={:.4} p50={:.0}us ({bpv} B/vec)",
                    rs / nq as f64,
                    pctl(&lat, 0.5)
                );
            }
        }
    }
    println!("wrote {}", args.out);
}
