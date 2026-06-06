// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! In-memory RaBitQ vector index for the MemTable.
//!
//! Unlike [`super::HnswMemIndex`], this index does **not** train its own
//! structure: it **always reuses the base table's IVF-RQ index** — its trained
//! k-means centroids and its `RabitQuantizer` (rotation + num_bits). On the
//! write path each incoming vector is assigned to its nearest base centroid and
//! quantized incrementally; there is no graph and no k-means. Search uses the
//! production split-code estimator, so recall matches the persisted `IVF_RQ`
//! index. The query is rotated once (`R(q)`) and each probed partition subtracts
//! a precomputed rotated centroid (`R(q - c) = R(q) - R(c)`) instead of
//! re-rotating per partition. This index is only usable when the base table
//! already has an IVF-RQ index to reuse.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, UInt32Type};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use lance_arrow::{FixedSizeListArrayExt, RecordBatchExt};
use lance_core::{Error, ROW_ID, Result};
use lance_index::vector::PART_ID_COLUMN;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::storage::{RabitQuantizationMetadata, RabitQuantizationStorage};
use lance_index::vector::flat::storage::FlatFloatStorage;
use lance_index::vector::hnsw::HNSW;
use lance_index::vector::hnsw::builder::{HnswBuildParams, HnswQueryParams};
use lance_index::vector::ivf::{IvfTransformer, new_ivf_transformer_with_quantizer};
use lance_index::vector::kmeans::KMeans;
use lance_index::vector::quantizer::{Quantization, Quantizer, QuantizerStorage};
use lance_index::vector::storage::{DistCalculator, VectorStore};
use lance_index::vector::transform::Transformer;
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;
use rayon::prelude::*;

use super::RowPosition;

/// Configuration for an in-memory RaBitQ index that reuses a base IVF-RQ index.
#[derive(Clone)]
pub struct RabitqIndexConfig {
    pub name: String,
    pub field_id: i32,
    pub column: String,
    pub distance_type: DistanceType,
    /// Base table IVF centroids (`nlist` rows of `dim` f32).
    pub centroids: FixedSizeListArray,
    /// Base table RaBitQ quantizer (rotation + num_bits), reused verbatim.
    pub quantizer: RabitQuantizer,
    /// MemTable row capacity. When the base centroid count exceeds what this
    /// capacity warrants (`~sqrt(max_rows)`), the base centroids are coarsened
    /// to that target so partitions stay dense and centroid overhead stays
    /// bounded. `0` disables coarsening (use the base centroids verbatim).
    pub max_rows: usize,
}

/// Minimum centroid count after coarsening — keep enough partitions to prune.
const MIN_COARSE_CENTROIDS: usize = 64;
/// K-means iterations when coarsening base centroids (small set, converges fast).
const COARSEN_MAX_ITERS: u32 = 20;

/// Partition count appropriate for `max_rows` rows: `~sqrt(max_rows)`, clamped to
/// `[MIN_COARSE_CENTROIDS, base_nlist]`. `max_rows == 0` keeps the base count.
fn target_nlist(max_rows: usize, base_nlist: usize) -> usize {
    if max_rows == 0 {
        return base_nlist;
    }
    let t = (max_rows as f64).sqrt().ceil() as usize;
    t.clamp(MIN_COARSE_CENTROIDS, base_nlist)
}

/// Coarsen `base` centroids down to `target` via k-means over the centroids
/// themselves (no data pass) — produces a MemTable-sized partitioning derived
/// from the base k-means.
fn coarsen_centroids(base: &FixedSizeListArray, target: usize) -> Result<FixedSizeListArray> {
    let dim = base.value_length() as usize;
    let km = KMeans::new(base, target, COARSEN_MAX_ITERS)
        .map_err(|e| Error::index(format!("centroid coarsening k-means failed: {e}")))?;
    let values = km.centroids.as_primitive::<Float32Type>().clone();
    Ok(FixedSizeListArray::try_new_from_values(values, dim as i32)?)
}

#[derive(Default)]
struct PartitionBuf {
    /// Quantized code batches appended on insert.
    batches: Vec<RecordBatch>,
    /// Cached storage built from `batches`; invalidated (None) on insert.
    storage: Option<Arc<RabitQuantizationStorage>>,
}

/// In-memory RaBitQ index, queryable while building, reusing a base IVF-RQ.
pub struct RabitqMemIndex {
    field_id: i32,
    column: String,
    distance_type: DistanceType,
    /// Production IVF-RQ transform pipeline (assignment + residual + quantize).
    /// Assignment is HNSW-accelerated over the reused base centroids when the
    /// centroid set is large enough (see [`SimpleIndex`]); brute force otherwise.
    ///
    /// [`SimpleIndex`]: lance_index::vector::utils
    ivf_transformer: IvfTransformer,
    /// Quantizer metadata (rotation), cached for query rotation + storage builds.
    metadata: RabitQuantizationMetadata,
    /// `nlist` precomputed rotated centroids `R(c)` (each `rotated_dim` long), so
    /// search computes `R(q - c) = R(q) - R(c)` without re-rotating per partition.
    rotated_centroids: Vec<f32>,
    rotated_dim: usize,
    /// HNSW over the centroids for query-time routing: find the nprobe nearest
    /// centroids in ~O(log nlist) instead of a brute-force scan of all `nlist`.
    /// This is Lance's [`SimpleIndex`] technique (already used for build-time
    /// partition assignment) applied to the read path.
    ///
    /// [`SimpleIndex`]: lance_index::vector::utils
    centroid_storage: FlatFloatStorage,
    centroid_hnsw: HNSW,
    nlist: usize,
    dim: usize,
    parts: Vec<Mutex<PartitionBuf>>,
    len: AtomicUsize,
}

/// HNSW build parameters for the centroid routing index (small, fast to build).
const CENTROID_HNSW_EDGES: usize = 16;
const CENTROID_HNSW_EF_CONSTRUCTION: usize = 40;

impl RabitqMemIndex {
    pub fn new(config: RabitqIndexConfig) -> Result<Self> {
        let dim = config.centroids.value_length() as usize;
        // Coarsen the base centroids when there are too many for the MemTable
        // capacity, so partitions stay dense and centroid overhead stays bounded.
        let base_nlist = config.centroids.len();
        let target = target_nlist(config.max_rows, base_nlist);
        let centroids = if target < base_nlist {
            coarsen_centroids(&config.centroids, target)?
        } else {
            config.centroids.clone()
        };
        let nlist = centroids.len();
        let centroids_flat = centroids
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .to_vec();
        // Build the production IVF-RQ transform once; this also trains the
        // HNSW-over-centroids assignment index when beneficial.
        let ivf_transformer = new_ivf_transformer_with_quantizer(
            centroids.clone(),
            config.distance_type,
            &config.column,
            Quantizer::Rabit(config.quantizer.clone()),
            None,
        )?;
        // Precompute each centroid's rotation `R(c)` so query-time partition scans
        // use `R(q - c) = R(q) - R(c)` (one query rotation + a subtraction) instead
        // of re-rotating the query for every probed partition.
        let metadata = config.quantizer.metadata(None);
        let rotated_dim = metadata.rotated_dim();
        let mut rotated_centroids = vec![0f32; nlist * rotated_dim];
        rotated_centroids
            .par_chunks_mut(rotated_dim)
            .enumerate()
            .for_each(|(p, out)| {
                let c = Float32Array::from(centroids_flat[p * dim..(p + 1) * dim].to_vec());
                metadata.rotate_into(&c, out);
            });
        // Build an HNSW over the centroids for fast query-time routing.
        let centroid_storage = FlatFloatStorage::new(centroids, config.distance_type);
        let centroid_hnsw = HNSW::index_vectors(
            &centroid_storage,
            HnswBuildParams::default()
                .num_edges(CENTROID_HNSW_EDGES)
                .ef_construction(CENTROID_HNSW_EF_CONSTRUCTION),
        )?;
        Ok(Self {
            field_id: config.field_id,
            column: config.column,
            distance_type: config.distance_type,
            ivf_transformer,
            metadata,
            rotated_centroids,
            rotated_dim,
            centroid_storage,
            centroid_hnsw,
            nlist,
            dim,
            parts: (0..nlist)
                .map(|_| Mutex::new(PartitionBuf::default()))
                .collect(),
            len: AtomicUsize::new(0),
        })
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }
    pub fn column_name(&self) -> &str {
        &self.column
    }
    pub fn distance_type(&self) -> DistanceType {
        self.distance_type
    }
    pub fn dim(&self) -> usize {
        self.dim
    }
    /// Number of partitions (centroids) after any coarsening.
    pub fn nlist(&self) -> usize {
        self.nlist
    }
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total bytes retained by the quantized code batches (actual Arrow buffers,
    /// including the per-row id column). This is the index's resident footprint.
    pub fn code_bytes(&self) -> usize {
        self.parts
            .iter()
            .map(|p| {
                p.lock()
                    .unwrap()
                    .batches
                    .iter()
                    .map(|b| b.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum()
    }

    /// Append a batch of vectors. Runs the reused base IVF-RQ transform
    /// (HNSW-accelerated centroid assignment + residual + quantize), then groups
    /// the resulting codes by partition. Row ids are `row_offset + i`.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        let col = batch
            .column_by_name(&self.column)
            .ok_or_else(|| Error::invalid_input(format!("column {} not found", self.column)))?;
        let fsl = col
            .as_fixed_size_list_opt()
            .ok_or_else(|| Error::invalid_input("vector column must be FixedSizeList"))?;
        if fsl.value_length() as usize != self.dim {
            return Err(Error::invalid_input(format!(
                "dim mismatch: expected {}, got {}",
                self.dim,
                fsl.value_length()
            )));
        }
        let n = fsl.len();
        if n == 0 {
            return Ok(());
        }

        // Build the pipeline input once ([vector, row_id]).
        let row_ids: Vec<u64> = (0..n as u64).map(|i| row_offset + i).collect();
        let in_schema = Arc::new(Schema::new(vec![
            Field::new(&self.column, col.data_type().clone(), true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));
        let in_batch = RecordBatch::try_new(
            in_schema,
            vec![col.clone(), Arc::new(UInt64Array::from(row_ids))],
        )?;

        // Transform + group in parallel across chunks of the batch, mirroring
        // `HnswMemIndex::insert`, which fans graph insertion across Rayon workers.
        // Quantization is per-vector independent, so a single (single-writer)
        // insert call still saturates all cores.
        let nthreads = rayon::current_num_threads().max(1);
        let chunk = n.div_ceil(nthreads).max(1);
        let stored: usize = (0..n)
            .step_by(chunk)
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|start| self.ingest_chunk(&in_batch.slice(start, chunk.min(n - start))))
            .collect::<Result<Vec<usize>>>()?
            .into_iter()
            .sum();
        self.len.fetch_add(stored, Ordering::Release);
        Ok(())
    }

    /// Run one chunk through the reused IVF-RQ pipeline (HNSW-accelerated centroid
    /// assignment + residual + quantize) and append its codes to the per-partition
    /// buffers. Returns the number of rows actually stored (the pipeline may drop
    /// non-finite rows).
    fn ingest_chunk(&self, in_chunk: &RecordBatch) -> Result<usize> {
        let coded = self.ivf_transformer.transform(in_chunk)?;
        let part_ids = coded
            .column_by_name(PART_ID_COLUMN)
            .ok_or_else(|| {
                Error::index(format!(
                    "{PART_ID_COLUMN} column missing after IVF-RQ transform"
                ))
            })?
            .as_primitive::<UInt32Type>();

        // Group rows by partition and append each partition's code sub-batch.
        let mut groups: HashMap<u32, Vec<u32>> = HashMap::new();
        for (i, &p) in part_ids.values().iter().enumerate() {
            groups.entry(p).or_default().push(i as u32);
        }
        for (p, ids) in groups {
            let sub = coded.take(&UInt32Array::from(ids))?;
            let mut g = self.parts[p as usize].lock().unwrap();
            g.batches.push(sub);
            g.storage = None; // invalidate cache
        }
        Ok(coded.num_rows())
    }

    fn partition_storage(&self, p: usize) -> Result<Option<Arc<RabitQuantizationStorage>>> {
        let mut g = self.parts[p].lock().unwrap();
        if g.batches.is_empty() {
            return Ok(None);
        }
        if g.storage.is_none() {
            let schema = g.batches[0].schema();
            let merged = arrow_select::concat::concat_batches(&schema, g.batches.iter())?;
            let storage = RabitQuantizationStorage::try_from_batch(
                merged,
                &self.metadata,
                self.distance_type,
                None,
            )?;
            g.storage = Some(Arc::new(storage));
        }
        Ok(g.storage.clone())
    }

    /// Search `nprobe` nearest partitions with the production split-code
    /// estimator (no f32 refine). Returns `(distance, row_position)` sorted by
    /// distance, MVCC-filtered to `max_row_position`.
    pub fn search(
        &self,
        query: &FixedSizeListArray,
        k: usize,
        nprobe: usize,
        max_row_position: RowPosition,
    ) -> Result<Vec<(f32, RowPosition)>> {
        if k == 0 || self.is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != 1 || query.value_length() as usize != self.dim {
            return Err(Error::invalid_input("invalid query shape"));
        }
        let q = query.value(0);
        let q = q.as_primitive::<Float32Type>();
        let qv = q.values();

        // Route via the centroid HNSW: nprobe approximate-nearest centroids in
        // ~O(log nlist) instead of a brute-force scan of all `nlist`. `nd.dist`
        // is the centroid L2 distance (= dist_q_c). ef is widened past nprobe so
        // routing stays accurate enough not to cost recall.
        let np = nprobe.min(self.nlist);
        let route_q: ArrayRef = Arc::new(Float32Array::from(qv.to_vec()));
        let route_params = HnswQueryParams {
            ef: (np * 2).max(64),
            lower_bound: None,
            upper_bound: None,
            dist_q_c: 0.0,
        };
        let routed = self.centroid_hnsw.search_basic(
            route_q,
            np,
            &route_params,
            None,
            &self.centroid_storage,
        )?;
        let cd: Vec<(f32, usize)> = routed
            .iter()
            .map(|nd| (nd.dist.0, nd.id as usize))
            .collect();

        // Rotate the query once: `R(q)`. Per partition we then subtract the
        // precomputed `R(c)` to get `R(q - c)` instead of re-rotating the query.
        let query_arr = Float32Array::from(qv.to_vec());
        let mut rotated_query = vec![0f32; self.rotated_dim];
        self.metadata.rotate_into(&query_arr, &mut rotated_query);

        // Scan probed partitions in parallel — they are independent, and the
        // per-partition `distance_all` flat scan is the dominant read cost. (HNSW
        // cannot parallelize a single query's graph walk.) `map_init` reuses a
        // residual + scratch buffer per worker thread.
        let cand: Vec<(f32, RowPosition)> = cd
            .par_iter()
            .map_init(
                || (vec![0f32; self.rotated_dim], Vec::<f32>::new()),
                |(residual, scratch), &(dist_q_c, p)| -> Result<Vec<(f32, RowPosition)>> {
                    let Some(storage) = self.partition_storage(p)? else {
                        return Ok(Vec::new());
                    };
                    let rc =
                        &self.rotated_centroids[p * self.rotated_dim..(p + 1) * self.rotated_dim];
                    for (r, (&rq, &rcv)) in residual.iter_mut().zip(rotated_query.iter().zip(rc)) {
                        *r = rq - rcv;
                    }
                    // Pre-rotated path: R(q - c) = R(q) - R(c), no re-rotation.
                    let calc = storage.dist_calculator_with_rotated_query(
                        residual.as_slice(),
                        dist_q_c,
                        scratch,
                    );
                    let dists = calc.distance_all(0);
                    let mut out = Vec::new();
                    for (local, &d) in dists.iter().enumerate() {
                        let row = storage.row_id(local as u32);
                        if row <= max_row_position && d.is_finite() {
                            out.push((d, row));
                        }
                    }
                    Ok(out)
                },
            )
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        let mut cand = cand;
        let take = k.min(cand.len());
        if take == 0 {
            return Ok(Vec::new());
        }
        cand.select_nth_unstable_by(take - 1, |a, b| a.0.total_cmp(&b.0));
        cand.truncate(take);
        cand.sort_by(|a, b| a.0.total_cmp(&b.0));
        Ok(cand)
    }
}
