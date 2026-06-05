// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! REAL IVF_RQ index recall harness — the faithful test (vs the in-memory
//! reconstruction in `rabitq_vs_hnsw`). Writes vectors to a Lance dataset,
//! builds a production `IVF_RQ(num_bits)` index, searches via the scanner with
//! NO refine, and measures recall@k vs exact ground truth. `harness = false`.
//!
//! cargo bench --bench rq_real_index -p lance -- \
//!   --base-path dbpedia_base.fvecs --query-path dbpedia_query.fvecs \
//!   --sizes 100000,900000 --num-bits 1,3 --nlist 4096 --nprobe 16,64,256 --k 10,100

#![allow(clippy::needless_range_loop)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read, Write as _};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use futures::TryStreamExt;
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_arrow::FixedSizeListArrayExt;
use lance_core::ROW_ID;
use lance_index::IndexType;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

#[derive(Parser, Debug, Clone)]
#[command(about = "Real IVF_RQ index recall harness")]
struct Args {
    #[arg(long)]
    base_path: String,
    #[arg(long)]
    query_path: String,
    #[arg(long, default_value = "100000,900000")]
    sizes: String,
    #[arg(long, default_value = "1,3")]
    num_bits: String,
    #[arg(long, default_value = "4096")]
    nlist: String,
    #[arg(long, default_value = "16,64,256")]
    nprobe: String,
    #[arg(long, default_value = "10,100")]
    k: String,
    #[arg(long, default_value_t = 300)]
    queries: usize,
    #[arg(long, default_value = "rq_real_index.csv")]
    out: String,
    #[arg(long, hide = true, default_value_t = false)]
    bench: bool,
}

fn parse_usize_list(s: &str) -> Vec<usize> {
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

fn schema_of(dim: usize) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            dim as i32,
        ),
        true,
    )]))
}

async fn write_dataset(uri: &str, base: &[f32], dim: usize) -> Dataset {
    let n = base.len() / dim;
    let schema = schema_of(dim);
    let chunk = 50_000;
    let batches: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> = (0..n)
        .step_by(chunk)
        .map(|start| {
            let end = (start + chunk).min(n);
            let fsl = FixedSizeListArray::try_new_from_values(
                Float32Array::from(base[start * dim..end * dim].to_vec()),
                dim as i32,
            )
            .unwrap();
            Ok(RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap())
        })
        .collect();
    let reader = RecordBatchIterator::new(batches.into_iter(), schema.clone());
    // One fragment so `_rowid` == row position == base index for recall.
    let params = WriteParams {
        max_rows_per_file: n.max(1),
        max_rows_per_group: 8192,
        ..Default::default()
    };
    Dataset::write(reader, uri, Some(params)).await.unwrap()
}

async fn search_ids(dataset: &Dataset, query: &[f32], k: usize, nprobe: usize) -> Vec<u32> {
    let q = Float32Array::from(query.to_vec());
    let mut scanner = dataset.scan();
    scanner.with_row_id();
    scanner.nearest("vector", &q, k).unwrap();
    scanner.minimum_nprobes(nprobe);
    // No .refine(): measure raw quantized recall.
    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = Vec::with_capacity(k);
    for b in batches {
        let rid = b
            .column_by_name(ROW_ID)
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>();
        ids.extend(rid.values().iter().map(|&v| v as u32));
    }
    ids
}

fn pctl(mut us: Vec<f64>, p: f64) -> f64 {
    us.sort_by(|a, b| a.total_cmp(b));
    us[(((us.len() as f64 - 1.0) * p).round()) as usize]
}

async fn run(args: Args) {
    let sizes = parse_usize_list(&args.sizes);
    let num_bits_list: Vec<u8> = parse_usize_list(&args.num_bits)
        .iter()
        .map(|&x| x as u8)
        .collect();
    let nlists = parse_usize_list(&args.nlist);
    let nprobes = parse_usize_list(&args.nprobe);
    let ks = parse_usize_list(&args.k);

    let (full_base, dim) = read_fvecs(&args.base_path, usize::MAX);
    let (full_q, dq) = read_fvecs(&args.query_path, args.queries);
    assert_eq!(dim, dq, "dim mismatch");
    let n_queries = (full_q.len() / dim).min(args.queries);
    let queries = &full_q[..n_queries * dim];
    println!(
        "dim={dim} base_total={} queries={n_queries}",
        full_base.len() / dim
    );

    let mut out = File::create(&args.out).expect("csv");
    writeln!(out, "n,dim,num_bits,nlist,nprobe,k,recall,p50_us,mean_us").unwrap();

    for &n in &sizes {
        if n > full_base.len() / dim {
            eprintln!("skip n={n}");
            continue;
        }
        let base = &full_base[..n * dim];
        let tmp = tempfile::TempDir::new().unwrap();
        let uri = tmp.path().to_str().unwrap();
        let t = Instant::now();
        let mut dataset = write_dataset(uri, base, dim).await;
        println!(
            "== N={n}: wrote dataset in {:.1}s ==",
            t.elapsed().as_secs_f64()
        );
        let gts: Vec<(usize, Vec<HashSet<u32>>)> = ks
            .iter()
            .map(|&k| (k, exact_topk(base, queries, dim, k)))
            .collect();

        for &nb in &num_bits_list {
            for &nlist in &nlists {
                let t = Instant::now();
                let params = VectorIndexParams::ivf_rq(nlist, nb, DistanceType::L2);
                // Mutate in place + replace so each build commits on the prior
                // version (avoids CreateIndex version conflicts).
                dataset
                    .create_index(&["vector"], IndexType::Vector, None, &params, true)
                    .await
                    .unwrap();
                println!(
                    "  built IVF_RQ num_bits={nb} nlist={nlist} in {:.1}s",
                    t.elapsed().as_secs_f64()
                );
                for &np in &nprobes {
                    for (k, gt) in &gts {
                        let mut lat = Vec::with_capacity(n_queries);
                        let mut rsum = 0f64;
                        for i in 0..n_queries {
                            let q = &queries[i * dim..(i + 1) * dim];
                            let t = Instant::now();
                            let ids = search_ids(&dataset, q, *k, np).await;
                            lat.push(t.elapsed().as_secs_f64() * 1e6);
                            let hit = ids.iter().take(*k).filter(|id| gt[i].contains(id)).count();
                            rsum += hit as f64 / (*k).min(gt[i].len()) as f64;
                        }
                        let mean = lat.iter().sum::<f64>() / lat.len() as f64;
                        let p50 = pctl(lat, 0.5);
                        let recall = rsum / n_queries as f64;
                        writeln!(
                            out,
                            "{},{},{},{},{},{},{:.4},{:.1},{:.1}",
                            n, dim, nb, nlist, np, k, recall, p50, mean
                        )
                        .unwrap();
                        println!(
                            "    nb={nb} nlist={nlist} np={np} k={k}: recall={recall:.4} p50={p50:.0}us"
                        );
                    }
                }
            }
        }
    }
    println!("wrote {}", args.out);
}

fn main() {
    let args = Args::parse();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run(args));
}
