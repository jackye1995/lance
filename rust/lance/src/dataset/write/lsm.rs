// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::Mutex;
use std::pin::Pin;
use std::future::Future;
use std::num::{NonZero, NonZeroU64};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use datafusion::execution::SendableRecordBatchStream;
use lance_datafusion::utils::StreamingWriteSource;
use crate::Dataset;
use lance_core::{ArrowResult, Error, Result};
use lance_index::mem_wal::{MemWal, MemWalId};
use object_store::path::Path;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchWriter};
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::RecordBatchStream;
use datafusion_expr::{col, UserDefinedLogicalNode};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, Stream, StreamExt};
use itertools::Itertools;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use uuid::Uuid;
use snafu::location;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use lance_core::datatypes::Projectable;
use lance_index::{is_system_index, DatasetIndexExt};
use lance_index::optimize::OptimizeOptions;
use lance_io::object_store::ObjectStore;
use lance_table::rowids::segment::U64Segment;
use crate::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use crate::index::mem_wal::{find_latest_mem_wal_generation, create_mem_wal_generation, mark_mem_wal_as_sealed, update_mem_table_location, append_mem_wal_entry, advance_mem_wal_generation};
use lance_index::{IndexType, IndexParams, vector::VectorIndexParams, scalar::ScalarIndexParams, infer_index_type};
use lance_index::metrics::NoOpMetricsCollector;
use lance_linalg::distance::MetricType;
use lance_table::format::Index;
use crate::index::DatasetIndexInternalExt;
use crate::index::scalar::infer_index_type;
use crate::index::vector::VectorIndexParams;
use lance_file::writer::FileWriter;
use lance_file::reader::FileReader;
use lance_encoding::decoder::DecoderPlugins;
use lance_core::cache::LanceCache;
use lance_file::writer::ManifestDescribing;
use lance_file::reader::FileReaderOptions;
use lance_file::v2::reader::FileReaderOptions;
use lance_table::io::manifest::ManifestDescribing;

enum WorkerMessage {
    Write {
        source: SendableRecordBatchStream,
        response_sender: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

pub struct LogStructuredMergeWriteFuture {
    receiver: oneshot::Receiver<Result<()>>,
}

impl Future for LogStructuredMergeWriteFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.receiver.poll_unpin(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Internal {
                message: "Worker thread was dropped".to_string(),
                location: location!(),
            })),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct LogStructuredMergeParams {
    /// A unique ID to identify this job.
    /// The job ID is used to produce a unique MemTable location,
    /// which is used to declare the write ownership of a MemTable.
    /// When the job crashes, it is expected that a new job should use a different ID.
    /// Defaults to a v4 UUID.
    pub job_id: String,

    /// The region for this LSM job. Defaults to "GLOBAL" if not provided.
    pub region: String,

    /// The size threshold in bytes for flushing MemTables. Defaults to 128MB.
    pub mem_table_flush_size: u64,

    /// The number of writes to accumulate before writing to the WAL and MemTable.
    /// This helps the case of many concurrent tiny-size writes,
    /// trading off the durability because all writes in a batch either all succeed or all fail.
    /// Defaults to 10.
    pub batch_size: u64,

    /// The interval for which a batch write execution must be triggered.
    /// This ensures we do not wait for too long if the batch size is not fulfilled.
    /// Defaults to 500ms.
    pub batch_interval_millis: i64,
}

impl Default for LogStructuredMergeParams {
    fn default() -> Self {
        Self {
            job_id: Uuid::new_v4().to_string(),
            region: "GLOBAL".to_string(),
            mem_table_flush_size: 128 * 1024 * 1024,
            batch_size: 10,
            batch_interval_millis: 500,
        }
    }
}

pub struct LogStructuredMergeJobBuilder {
    dataset: Arc<RwLock<Dataset>>,
    params: LogStructuredMergeParams,
}

impl LogStructuredMergeJobBuilder {

    pub fn new(dataset: Arc<RwLock<Dataset>>) -> Self {
        Self {
            dataset,
            params: LogStructuredMergeParams::default(),
        }
    }

    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.params.region = region.into();
        self
    }

    pub fn mem_table_flush_size(mut self, size_bytes: u64) -> Self {
        self.params.mem_table_flush_size = size_bytes;
        self
    }

    pub fn batch_size(mut self, size: u64) -> Self {
        self.params.batch_size = size;
        self
    }

    pub async fn try_start(self) -> Result<LogStructuredMergeJob> {
        LogStructuredMergeJob::start(self.dataset, self.params).await
    }
}

pub struct LogStructuredMergeJob {
    // store job ID for logging purpose during shutdown
    job_id: String,
    runtime: Arc<Runtime>,
    sender: Sender<WorkerMessage>,
    worker_handle: JoinHandle<()>,
}

/// Log-structured Merge (LSM) Job is a long-running job.
/// It is expected that there is only 1 LSM job per region writing globally.
/// If there are concurrent LSM jobs against the same region, one is guaranteed to fail all the time.
///
/// Once started, it continues to receive writes to the underlying dataset.
/// Writes are queued and executed sequentially following the LSM semantics:
/// (1) write to WAL first for durability
/// (2) write to MemTable for fast access
/// (3) when the MemTable reaches `mem_table_flush_size`, asynchronously flush it and
/// create a new MemTable and WAL to write to (a.k.a. advance MemWAL generation).
/// Note that currently this is a simple 2-level LSM tree.
/// A flush directly runs merge_insert to merge data into the underlying Lance dataset.
///
/// When reading a dataset, the job can be supplied to the scanner.
/// The scanner will produce a N-way merge execution plan to merge results of MemTables and
/// the underlying Lance dataset.
impl LogStructuredMergeJob {

    async fn start(dataset: Arc<RwLock<Dataset>>, params: LogStructuredMergeParams) -> Result<(Self)> {
        let runtime = Arc::new(Runtime::new()
            .expect(&format!("Failed to create tokio runtime for LogStructuredMergeJob {}", params.job_id)));

        let (sender, receiver) = mpsc::channel::<WorkerMessage>(100);
        let mut mem_wal_map: HashMap<u64, MemWal> = HashMap::new();
        let mut mem_table_map: HashMap<u64, Arc<RwLock<Dataset>>> = HashMap::new();
        let mut mem_table_size_map: HashMap<u64, AtomicU64> = HashMap::new();
        let mut wal_watermark_map: HashMap<u64, AtomicU64> = HashMap::new();
        let current_generation =
            if let Some(latest_gen) = find_latest_mem_wal_generation(dataset.read().unwrap().deref(), &params.region).await? {
                let new_mem_table_location = Self::mem_table_location(&params.job_id, &params.region, latest_gen.id.generation);
                if latest_gen.sealed {
                    let new_gen = latest_gen.id.generation + 1;
                    let new_mem_wal = create_mem_wal_generation(
                        dataset.write().unwrap().deref_mut(),
                        &params.region,
                        new_gen,
                        &new_mem_table_location,
                        &Self::wal_location(dataset.read().unwrap().deref(), &params.region, new_gen),
                    ).await?;

                    let empty_mem_table = Self::init_empty_mem_table(dataset.read().unwrap().deref(), &new_mem_table_location).await?;

                    mem_wal_map.insert(new_gen, new_mem_wal);
                    mem_table_map.insert(new_gen, Arc::new(RwLock::new(empty_mem_table)));
                    mem_table_size_map.insert(new_gen, AtomicU64::new(0));
                    new_gen
                } else {
                    // MemWAL is unsealed. This is likely from a crash,
                    // so replay the WAL to revive the MemTable.
                    let current_gen = latest_gen.id.generation;
                    let current_mem_wal = update_mem_table_location(
                        dataset.write().unwrap().deref_mut(),
                        &params.region,
                        current_gen,
                        &new_mem_table_location).await?;

                    let (mem_table, mem_table_size) = Self::replay_wal_entries_to_mem_table(
                        dataset.read().unwrap().deref(),
                        &latest_gen.wal_location,
                        latest_gen.wal_entries(),
                        &new_mem_table_location).await?;

                    if let Some(wal_entries_range) = current_mem_wal.wal_entries().range() {
                        wal_watermark_map.insert(current_gen, AtomicU64::new(wal_entries_range.end() - 1));
                    }

                    mem_wal_map.insert(current_gen, current_mem_wal);
                    mem_table_map.insert(current_gen, Arc::new(RwLock::new(mem_table)));
                    mem_table_size_map.insert(current_gen, AtomicU64::new(mem_table_size));
                    current_gen
                }
            } else {
                let new_mem_table_location = Self::mem_table_location(&params.job_id, &params.region, 0);
                let new_mem_wal = create_mem_wal_generation(
                    dataset.write().unwrap().deref_mut(),
                    &params.region,
                    0,
                    &Self::mem_table_location(&params.job_id, &params.region, 0),
                    &Self::wal_location(dataset.read().unwrap().deref(), &params.region, 0),
                ).await?;
                mem_wal_map.insert(0, new_mem_wal);

                let empty_mem_table = Self::init_empty_mem_table(dataset.read().unwrap().deref(), &new_mem_table_location).await?;
                mem_table_map.insert(0, Arc::new(RwLock::new(empty_mem_table)));

                0
            };

        let current_generation = Arc::new(AtomicU64::new(current_generation));
        let mem_wal_map = Arc::new(RwLock::new(mem_wal_map));
        let mem_table_map = Arc::new(RwLock::new(mem_table_map));
        let mem_table_size_map = Arc::new(RwLock::new(mem_table_size_map));
        let wal_watermark_map = Arc::new(RwLock::new(wal_watermark_map));
        let worker_handle = tokio::spawn(Self::write_worker_loop(
            receiver,
            dataset,
            params.clone(),
            current_generation,
            mem_wal_map,
            mem_table_map,
            mem_table_size_map,
            wal_watermark_map,
        ));

        let job_id = params.job_id;
        log::info!("LogStructuredMergeJob {} successfully started", job_id);

        Ok(Self {
            job_id,
            runtime,
            sender,
            worker_handle,
        })
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Err(err) = self.sender.send(WorkerMessage::Shutdown).await {
            log::error!("Error shutdown sender for LogStructuredMergeJob {}: {}", self.job_id, err);
        }

        if let Err(err) = self.worker_handle.await {
            log::error!("Error shutdown receiver for LogStructuredMergeJob {}: {}", self.job_id, err);
        }

        log::info!("LogStructuredMergeJob {} successfully shutdown", self.job_id);
        Ok(())
    }

    pub fn write_reader(
        &self,
        source: impl StreamingWriteSource,
    ) -> Result<LogStructuredMergeWriteFuture> {
        self.write(source.into_stream())
    }

    pub fn write(&self, source: SendableRecordBatchStream) -> Result<LogStructuredMergeWriteFuture> {
        let (response_sender, response_receiver) = oneshot::channel();
        let send_result = self.sender.send(WorkerMessage::Write {
            source,
            response_sender,
        });
        if send_result.is_err() {
            Err(Error::Execution {
                message: format!("LogStructuredMerge worker thread was dropped: {:?}", send_result.unwrap()),
                location: location!(),
            })
        } else {
            Ok(LogStructuredMergeWriteFuture {
                receiver: response_receiver,
            })
        }
    }

    async fn write_worker_loop(
        mut receiver: Receiver<WorkerMessage>,
        dataset: Arc<RwLock<Dataset>>,
        params: LogStructuredMergeParams,
        current_generation: Arc<AtomicU64>,
        mem_wal_map: Arc<RwLock<HashMap<u64, MemWal>>>,
        mem_table_map: Arc<RwLock<HashMap<u64, Arc<RwLock<Dataset>>>>>,
        mem_table_size_map: Arc<RwLock<HashMap<u64, AtomicU64>>>,
        wal_watermark_map: Arc<RwLock<HashMap<u64, AtomicU64>>>,
    ) {
        let mut batch: Vec<SendableRecordBatchStream> = Vec::with_capacity(params.batch_size as usize);
        let mut current_time = chrono::Utc::now().timestamp_millis();
        while let Some(message) = receiver.recv().await {
            match message {
                WorkerMessage::Write { source, response_sender } => {

                    let schema_ref = source.schema().clone();
                    batch.push(source);

                    if batch.len() >= params.batch_size as usize ||
                        chrono::Utc::now().timestamp_millis() - current_time > params.batch_interval_millis {
                        let chained = futures::stream::iter(batch).flatten();
                        let concatenated_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(schema_ref, Box::pin(chained)));
                        let result = Self::do_execute(
                            concatenated_stream,
                            dataset.clone(),
                            &params,
                            current_generation.clone(),
                            mem_wal_map.clone(),
                            mem_table_map.clone(),
                            mem_table_size_map.clone(),
                            wal_watermark_map.clone(),
                        ).await;
                        let _ = response_sender.send(result);
                        batch = Vec::with_capacity(params.batch_size as usize);
                        current_time = chrono::Utc::now().timestamp_millis();
                    }
                }
                WorkerMessage::Shutdown => {
                    // mark the current MemWAL as sealed
                    let current_gen = current_generation.load(Ordering::Relaxed);
                    let current_mem_wal = &mem_wal_map.read().unwrap()[&current_gen];
                    mark_mem_wal_as_sealed(
                        dataset.write().unwrap().deref_mut(),
                        &params.region,
                        current_gen,
                        &current_mem_wal.mem_table_location).await?;
                    break;
                }
            }
        }
    }

    /// Actual execution logic for a batch of
    async fn do_execute(
        source: SendableRecordBatchStream,
        dataset: Arc<RwLock<Dataset>>,
        params: &LogStructuredMergeParams,
        current_generation: Arc<AtomicU64>,
        mem_wal_map: Arc<RwLock<HashMap<u64, MemWal>>>,
        mem_table_map: Arc<RwLock<HashMap<u64, Arc<RwLock<Dataset>>>>>,
        mem_table_size_map: Arc<RwLock<HashMap<u64, AtomicU64>>>,
        wal_watermark_map: Arc<RwLock<HashMap<u64, AtomicU64>>>) -> Result<()> {

        let current_gen_id = current_generation.load(Ordering::Relaxed);
        let curr_mem_wal = &mem_wal_map.read().unwrap()[&current_gen_id];
        let mut entry_id = &wal_watermark_map.read().unwrap()[&current_gen_id];

        // TODO: might not be the best way when data is large and spills to disk
        //  but sufficient for small write use case which LSM is trying to solve
        let mut source_iter =
            super::new_source_iter(source, true).await?;

        // 1. write and commit to WAL.
        // in case of failure, this writer should continue to try writing to the next entry ID.
        let write_size = loop {
            entry_id.fetch_add(1, Ordering::SeqCst);
            let current_entry_id = entry_id.load(Ordering::SeqCst);

            let wal_file_path = Self::wal_entry_location(&curr_mem_wal.wal_location, current_entry_id);
            match Self::write_wal_entry(
                &dataset.read().unwrap().object_store,
                source_iter.next().unwrap(),
                &wal_file_path.into()).await {
                Ok(write_size) => {
                    match append_mem_wal_entry(
                        dataset.write().unwrap().deref_mut(),
                        &params.region,
                        current_gen_id,
                        current_entry_id,
                        &curr_mem_wal.mem_table_location,
                    ).await {
                        Ok(mem_wal) => {
                            mem_wal_map.write().unwrap().deref_mut().insert(current_gen_id, mem_wal);
                            break write_size;
                        }
                        Err(e) => {
                            // this should only happen during a failover case that
                            // a hanging writer tries to write to the WAL
                            log::warn!("Failed to commit to WAL with entry ID {}: {}", current_entry_id, e);
                        }
                    }
                }
                Err(e) => {
                    // this should only happen during a failover case that
                    // a hanging writer tries to write to the WAL
                    log::warn!("Failed to write to WAL with entry ID {}: {}", current_entry_id, e);
                }
            }
        };
        // use the buffer size from write to estimate MemTable size
        let mut current_size = mem_table_size_map.read().unwrap().get(&current_gen_id).unwrap();
        current_size.fetch_add(write_size, Ordering::SeqCst);

        // 2. write to MemTable
        let mem_table = &mem_table_map.read().unwrap().deref()[&current_gen_id];
        Self::write_to_mem_table(
            mem_table.clone(),
            &params.region,
            current_gen_id,
            source_iter.next().unwrap()).await?;

        // 3. check for MemTable size, flush and advance MemWAL generation
        if current_size.load(Ordering::SeqCst) > params.mem_table_flush_size {
            let new_gen_id = current_gen_id + 1;
            let new_mem_wal = advance_mem_wal_generation(
                dataset.write().unwrap().deref_mut(),
                &params.region,
                &Self::mem_table_location(&params.job_id, &params.region,  new_gen_id),
                &Self::wal_location(dataset.read().unwrap().deref(), &params.region,  new_gen_id),
                Some(&curr_mem_wal.mem_table_location),
            ).await?;

            tokio::spawn(Self::flush_sealed_mem_wal(
                dataset.clone(),
                sealed_mem_wal_id,
                &mem_table_location,
                mem_table_store,
            ));
        }

        Ok(())
    }

    fn mem_table_location(job_id: &str, region: &str, generation: u64) -> String {
        format!("memory://{}/{}/{}", job_id, region, generation)
    }

    async fn init_empty_mem_table(dataset: &Dataset, mem_table_location: &str) -> Result<Dataset> {
        // create a new dataset that uses the dataset's schema
        let arrow_schema: arrow_schema::Schema = dataset.schema().into();
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema.clone()));
        let reader = RecordBatchIterator::new(vec![Ok(empty_batch)], Arc::new(arrow_schema));
        let mut mem_table = Dataset::write(
            reader,
            mem_table_location,
            None,
        ).await?;

        // Configure all the indexes in the same way as the source dataset
        // Use batch processing for better performance when there are many indexes
        let indices = dataset.load_indices().await?;
        let non_system_indices: Vec<_> = indices.iter()
            .filter(|idx| !is_system_index(idx))
            .collect();
        
        if !non_system_indices.is_empty() {
            for index_meta in non_system_indices {
                let columns = index_meta.fields.iter()
                    .map(|idx| dataset.schema().field_ancestry_by_id(*idx))
                    .map(|fields| fields.unwrap().iter().map(|f| f.name.clone()).join("."))
                    .collect::<Vec<_>>();
                let column = columns[0].as_str();
                let uuid = index_meta.uuid.to_string();
                let index = dataset.open_generic_index(column, &uuid, &NoOpMetricsCollector).await?;
                let index_type = index.index_type();
                let params = Self::get_index_params(dataset, column, index_type, index_meta).await?;

                mem_table.create_index(
                        columns.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice(),
                        index.index_type(),
                        Some(index_meta.name.clone()),
                        params.as_ref(),
                        true,
                    ).await?;
                
                // Get the index type and create appropriate parameters
                let (index_type, params) = Self::get_index_params(dataset, index).await?;
                

            }
        }

        mem_table.optimize_indices(&OptimizeOptions::default()).await?;
        Ok(mem_table)
    }

    async fn get_index_params(
        dataset: &Dataset,
        column_name: &str,
        index_type: IndexType,
        index: &Index) -> Result<(Box<dyn IndexParams>)> {
        // Create appropriate parameters based on the index type
        let params: Box<dyn IndexParams> = match index_type {
            IndexType::Vector => {
                // For vector indices, try to open the index to get its parameters
                if let Ok(vector_index) = dataset.open_vector_index(column_name, &index.uuid.to_string(), &lance_index::metrics::NoOpMetricsCollector).await {
                    // Get the index type from the actual index
                    let actual_index_type = vector_index.index_type();
                    
                    // Create parameters based on the actual index type
                    match actual_index_type {
                        IndexType::IvfPq => {
                            // Try to extract parameters from the index statistics
                            if let Ok(stats) = vector_index.statistics() {
                                // Parse statistics to get parameters
                                if let Some(num_partitions) = stats.get("num_partitions").and_then(|v| v.as_u64()) {
                                    if let Some(num_sub_vectors) = stats.get("num_sub_vectors").and_then(|v| v.as_u64()) {
                                        if let Some(num_bits) = stats.get("num_bits").and_then(|v| v.as_u64()) {
                                            let metric = if let Some(metric_str) = stats.get("metric_type").and_then(|v| v.as_str()) {
                                                match metric_str {
                                                    "L2" => MetricType::L2,
                                                    "Cosine" => MetricType::Cosine,
                                                    "Dot" => MetricType::Dot,
                                                    "Hamming" => MetricType::Hamming,
                                                    _ => MetricType::L2,
                                                }
                                            } else {
                                                MetricType::L2
                                            };
                                            
                                            return Ok((index_type, Box::new(VectorIndexParams::ivf_pq(
                                                num_partitions as usize,
                                                num_sub_vectors as usize,
                                                num_bits as usize,
                                                metric,
                                                1,
                                            ))));
                                        }
                                    }
                                }
                            }
                            
                            // Fallback to defaults if we can't extract parameters
                            Box::new(VectorIndexParams::ivf_pq(256, 8, 8, MetricType::L2, 1))
                        }
                        _ => {
                            // For other vector index types, use defaults
                            Box::new(VectorIndexParams::ivf_pq(256, 8, 8, MetricType::L2, 1))
                        }
                    }
                } else {
                    // If we can't open the vector index, use defaults
                    Box::new(VectorIndexParams::ivf_pq(256, 8, 8, MetricType::L2, 1))
                }
            }
            IndexType::Inverted => {
                // For inverted indices, try to open the index to get its parameters
                if let Ok(inverted_index) = dataset.open_scalar_index(&column_name, &index.uuid.to_string(), &lance_index::metrics::NoOpMetricsCollector).await {
                    // Try to get parameters from the inverted index
                    if let Some(inverted_impl) = inverted_index.as_any().downcast_ref::<lance_index::scalar::inverted::InvertedIndex>() {
                        // Get the parameters from the actual inverted index
                        let params = inverted_impl.params().clone();
                        Box::new(params)
                    } else {
                        Box::new(lance_index::scalar::inverted::tokenizer::InvertedIndexParams::default())
                    }
                } else {
                    Box::new(lance_index::scalar::inverted::tokenizer::InvertedIndexParams::default())
                }
            }
            IndexType::Bitmap | IndexType::BTree | IndexType::LabelList | IndexType::NGram => {
                Box::new(ScalarIndexParams::default())
            }
            _ => {
                Box::new(ScalarIndexParams::default())
            }
        };

        Ok(params)
    }

    async fn write_to_mem_table(
        mem_table: Arc<RwLock<Dataset>>,
        region: &str,
        generation: u64,
        stream: SendableRecordBatchStream,
    ) -> Result<()> {
        let mut mem_table = mem_table.write().unwrap();
        let write_mem_table = mem_table.deref_mut();
        let on = write_mem_table.schema().unenforced_primary_key().iter()
            .map(|f| write_mem_table.schema().field_ancestry_by_id(f.id))
            .map(|fields| fields.unwrap().iter().join("."))
            .collect::<Vec<_>>();
        let merge_insert_job = MergeInsertBuilder::try_new(Arc::new(write_mem_table.clone()), on)?
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()?;

        merge_insert_job.execute(stream).await?;
        // TODO: workaround to be compatible between Arc and &mut self
        write_mem_table.checkout_latest().await?;
        // TODO: can potentially be more efficient since we know exactly
        //   what are the changed fragments and new data stream
        write_mem_table.optimize_indices(&OptimizeOptions::default()).await?;
        Ok(())
    }

    /// Use the reversed bit string as the name of the file
    /// This is used to maximize throughput in object storage for the directory
    fn wal_entry_location(wal_location: &str, entry_id: u64) -> String {
        let bit_string = format!("{:064b}", entry_id);
        let reversed: String = bit_string.chars().rev().collect();
        format!("{}/{}.lance", wal_location, reversed)
    }

    fn wal_location(dataset: &Dataset, region: &str, generation: u64) -> String {
        dataset.base.child(region).child(generation.to_string()).to_string()
    }


    /// Write a WAL entry file to object store.
    /// We write to Lance format since this is a direct dump,
    /// there is no need to optimize it to a different format.
    async fn write_wal_entry(
        object_store: &ObjectStore,
        source: SendableRecordBatchStream,
        wal_entry_path: &Path,
    ) -> Result<u64> {
        let obj_writer = object_store.create(wal_entry_path).await?;
        let schema_ref = source.schema().clone();
        let lance_schema = lance_core::datatypes::Schema::try_from(schema_ref.as_ref())?;
        let mut file_writer = FileWriter::<ManifestDescribing>::with_object_writer(
            obj_writer,
            lance_schema,
            &Default::default(),
        )?;
        let mut source_pinned = source;

        while let Some(batch) = source_pinned.next().await {
            let batch = batch?;
            file_writer.write(&[batch]).await?;
        }

        file_writer.finish().await?;
        let size_bytes = file_writer.tell().await?;
        
        Ok(size_bytes as u64)
    }
    
    async fn read_wal_entry(
        object_store: &ObjectStore,
        wal_entry_path: &Path,
    ) -> Result<SendableRecordBatchStream> {
        let obj_reader = object_store.open(wal_entry_path).await?;
        let file_reader = FileReader::with_object_reader(
            obj_reader,
            None,
            Arc::new(DecoderPlugins::default()),
            Arc::new(LanceCache::new()),
            FileReaderOptions::default(),
        ).await?;
        
        let schema_ref = Arc::new(file_reader.schema().as_ref().into());
        let stream = file_reader.read_stream(
            lance_io::ReadBatchParams::RangeFull,
            1024, // batch_size
            16,   // batch_readahead
            lance_encoding::decoder::FilterExpression::no_filter(),
        )?;
        
        let stream: SendableRecordBatchStream = Box::pin(stream);
        Ok(stream)
    }

    async fn replay_wal_entries_to_mem_table(
        dataset: &Dataset,
        wal_location: &str,
        wal_entries: U64Segment,
        mem_table_location: &str,
    ) -> Result<(Dataset, u64)> {
        // 1. for each WAL entry, do read_wal_entry_file to get its stream
        // 2. concatenate them using stream::iter(streams).flatten()
        // 3. init_empty_mem_table
        // 3. write all to the MemTable dataset
        // 4. update all the related indexes
        todo!()
    }

    async fn flush_sealed_mem_wal(
        dataset: Arc<RwLock<Dataset>>,
        mem_table: Arc<RwLock<Dataset>>,
        mem_wal_id: MemWalId,
        expected_mem_table_location: &str,
        mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
    ) -> Result<()> {

        let merge_insert_job = crate::dataset::MergeInsertBuilder::try_new(
            dataset.clone(),
            // TODO: this currently only support top level fields
            dataset.schema().unenforced_primary_key().iter().map(|f| f.to_string()).collect(),
        )?
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
            .mark_mem_wal_as_flushed(mem_wal_id, expected_mem_table_location)
            .await?
            .try_build()?;

        let stream = mem_table.scan().try_into_stream().await?;

        // TODO: how to ensure the flushed data is indexed?
        // we track the transaction file to know what are the newly produced fragments.
        // we also track the version of this new dataset
        // the next round of flush, we check for each previous flushed MemWAL,
        // compare the transactions from current version to latest version
        // if there is a compaction, readjust the fragments
        // and then check the frag coverage bitmap
        // if for all indexes, the index is covered by the bitmap, we can finally drop the MemWAL.
        let (dataset, _) = merge_insert_job.execute(stream.into()).await?;

        Ok(())
    }
}

