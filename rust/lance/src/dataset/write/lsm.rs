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
use lance_core::{Error, Result};
use lance_index::mem_wal::{MemWal, MemWalId};
use object_store::path::Path;
use arrow::ipc::writer::StreamWriter;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchWriter};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{FutureExt, StreamExt};
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
use crate::index::mem_wal::{find_latest_mem_wal_generation, create_mem_wal_generation, mark_mem_wal_as_sealed, update_mem_table_location, append_mem_wal_entry};

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

    // // latest generation
    // current_generation: Arc<AtomicU64>,
    // // MemWALs maintained within this job
    // mem_wal_map: Arc<RwLock<HashMap<u64, MemWal>>>,
    // // A mapping of MemTable location to its dataset
    // mem_table_map: Arc<RwLock<HashMap<u64, Arc<RwLock<Dataset>>>>>,
    // // A mapping of the latest used WAL entry ID
    // wal_watermark_map: Arc<RwLock<HashMap<u64, AtomicU64>>>,
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

                    let mem_table = Self::replay_wal_entries_to_mem_table(
                        dataset.read().unwrap().deref(),
                        &latest_gen.wal_location,
                        latest_gen.wal_entries(),
                        &new_mem_table_location).await?;

                    if let Some(wal_entries_range) = current_mem_wal.wal_entries().range() {
                        wal_watermark_map.insert(current_gen, AtomicU64::new(wal_entries_range.end() - 1));
                    }

                    mem_wal_map.insert(current_gen, current_mem_wal);
                    mem_table_map.insert(current_gen, Arc::new(RwLock::new(mem_table)));

                    current_gen
                }
            } else {
                let new_mem_table_location = Self::mem_table_location(&params.job_id, &params.region, 0);
                create_mem_wal_generation(
                    dataset.write().unwrap().deref_mut(),
                    &params.region,
                    0,
                    &Self::mem_table_location(&params.job_id, &params.region, 0),
                    &Self::wal_location(dataset.read().unwrap().deref(), &params.region, 0),
                ).await?;

                let empty_mem_table = Self::init_empty_mem_table(dataset.read().unwrap().deref(), &new_mem_table_location).await?;
                mem_table_map.insert(0, Arc::new(RwLock::new(empty_mem_table)));

                0
            };

        let current_generation = Arc::new(AtomicU64::new(current_generation));
        let mem_wal_map = Arc::new(RwLock::new(mem_wal_map));
        let mem_table_map = Arc::new(RwLock::new(mem_table_map));
        let wal_watermark_map = Arc::new(RwLock::new(wal_watermark_map));

        // let worker_runtime = runtime.clone();
        // let worker_dataset = dataset.clone();

        // let worker_current_generation = current_generation.clone();
        // let worker_mem_wal_map =  mem_wal_map.clone();
        // let worker_mem_table_map = mem_table_map.clone();
        // let worker_wal_watermark_map = wal_watermark_map.clone();

        // let worker_params = params.clone();
        let worker_handle = tokio::spawn(Self::write_worker_loop(
            receiver,
            dataset,
            params.clone(),
            current_generation,
            mem_wal_map,
            mem_table_map,
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
        loop {
            entry_id.fetch_add(1, Ordering::SeqCst);
            let current_entry_id = entry_id.load(Ordering::SeqCst);

            let wal_file_path = Self::wal_entry_location(&curr_mem_wal.wal_location, current_entry_id);
            match Self::write_wal_entry(
                &dataset.read().unwrap().object_store,
                source_iter.next().unwrap(),
                &wal_file_path.into()).await {
                Ok(_) => {
                    match append_mem_wal_entry(
                        dataset.write().unwrap().deref_mut(),
                        &params.region,
                        current_gen_id,
                        current_entry_id,
                        &curr_mem_wal.mem_table_location,
                    ).await {
                        Ok(mem_wal) => {
                            mem_wal_map.write().unwrap().deref_mut().insert(current_gen_id, mem_wal);
                            break;
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

        // 2. write to MemTable
        let mem_table = &mem_table_map.read().unwrap().deref()[&current_gen_id];
        Self::write_to_mem_table(
            mem_table.clone(),
            &params.region,
            current_gen_id,
            source_iter.next().unwrap()).await?;

        // 3. check for MemTable size, flush and advance MemWAL generation
        let mem_table_size = Self::get_mem_table_size_internal(region, generation, mem_table_store).await?;
        if mem_table_size > mem_table_flush_size {
            Self::advance_mem_wal_generation_internal(
                dataset,
                region,
                generation,
                mem_table_location,
                wal_location,
                mem_table_store,
            ).await?;
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
        for index in dataset.load_indices().await?.iter() {
            if !is_system_index(index) {
                let columns = index.fields.iter()
                    .map(|idx| dataset.schema().field_ancestry_by_id(*idx))
                    .map(|fields| fields.unwrap().iter().map(|f| f.name.clone()).join(".").as_str())
                    .collect::<Vec<_>>();
                mem_table.create_index(
                    columns.as_slice(),
                    ,
                    Some(index.name),
                    index.
                );
            }
        }

        mem_table.optimize_indices(&OptimizeOptions::default()).await?;
        Ok(mem_table)
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

    async fn read_wal_entry_file(
        dataset: &Dataset,
        wal_entry_location: &str) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    async fn write_wal_entry(
        object_store: &ObjectStore,
        source: SendableRecordBatchStream,
        wal_path: &Path,
    ) -> Result<()> {

        let mut obj_writer = object_store.create(wal_path).await?;
        let schema_ref = source.schema().clone();
        let mut ipc_writer = StreamWriter::try_new(obj_writer, &schema_ref)?;
        let mut source_pinned = source;

        while let Some(batch) = source_pinned.next().await {
            let batch = batch?;
            ipc_writer.write(&batch)?;
        }

        ipc_writer.finish()?;
        obj_writer.shutdown().await?;

        Ok(())
    }

    async fn replay_wal_entries_to_mem_table(
        dataset: &Dataset,
        wal_location: &str,
        wal_entries: U64Segment,
        mem_table_location: &str,
    ) -> Result<Dataset> {
        // 1. for each WAL entry, do read_wal_entry_file to get its stream
        // 2. concatenate them using stream::iter(streams).flatten()
        // 3. init_empty_mem_table
        // 3. write all to the MemTable dataset
        // 4. update all the related indexes
        todo!()
    }

    fn get_primary_key_column_internal(dataset: &Arc<Dataset>) -> Result<String> {
        // Get the primary key column from the dataset
        // This is a simplified implementation - in practice, you'd need to get the actual primary key
        let schema = dataset.schema();
        if let Some(field) = schema.fields.first() {
            Ok(field.name.clone())
        } else {
            Err(Error::invalid_input("No fields in schema", location!()))
        }
    }

    fn mem_table_size(dataset: &Arc<RwLock<Dataset>>) -> u64 {
        let dataset = dataset.read().unwrap();
        let dataset_read = dataset.deref();
        dataset_read.fragments().as_ref().iter()
            .flat_map(|f| f.files.iter())
            .map(|f| f.file_size_bytes.get().unwrap_or(NonZeroU64::new(1).unwrap()).get())
            .sum::<u64>()
    }

    async fn advance_mem_wal_generation_internal(
        dataset: &Arc<Dataset>,
        region: &str,
        generation: u64,
        mem_table_location: &str,
        wal_location: &str,
        mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
    ) -> Result<()> {
        let new_generation = generation + 1;
        // Advance the MemWAL generation
        crate::index::mem_wal::advance_mem_wal_generation(
            &mut dataset_clone,
            region,
            &new_mem_table_location,
            &new_wal_location,
            Some(mem_table_location),
        ).await?;
        
        // Seal the current MemWAL
        crate::index::mem_wal::mark_mem_wal_as_sealed(
            &mut dataset_clone,
            region,
            generation,
            mem_table_location,
        ).await?;
        
        // Spawn a background task to flush the sealed MemWAL
        let sealed_mem_wal_id = MemWalId::new(region, generation);
        let dataset_clone_for_flush = dataset.clone();
        let mem_table_location = mem_table_location.to_string();
        
        tokio::spawn(async move {
            if let Err(e) = Self::flush_sealed_mem_wal(
                dataset_clone_for_flush,
                sealed_mem_wal_id,
                &mem_table_location,
                mem_table_store,
            ).await {
                eprintln!("Failed to flush sealed MemWAL: {}", e);
            }
        });
        
        Ok(())
    }

    async fn flush_sealed_mem_wal(
        dataset: Arc<Dataset>,
        mem_wal_id: MemWalId,
        expected_mem_table_location: &str,
        mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
    ) -> Result<()> {
        // Get the MemTable data
        let mem_wal_id_str = format!("{}_{}", mem_wal_id.region, mem_wal_id.generation);
        let mem_table = {
            let store = mem_table_store.lock().unwrap();
            store.get(&mem_wal_id_str).cloned()
        };
        
        if let Some(mem_table) = mem_table {
            // Create merge insert job to flush the MemTable to the main dataset
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
            let (dataset, _) = merge_insert_job.execute(stream.into()).await?;

            // Remove the MemTable from the store after successful flush
            // TODO: how to ensure the flushed data is indexed?
            // we track the transaction file to know what are the newly produced fragments.
            // we also track the version of this new dataset
            // the next round of flush, we check for each previous flushed MemWAL,
            // compare the transactions from current version to latest version
            // if there is a compaction, readjust the fragments
            // and then check the frag coverage bitmap
            // if for all indexes, the index is covered by the bitmap, we can finally drop the MemWAL.
            let mut store = mem_table_store.lock().unwrap();
            store.remove(&mem_wal_id_str);
        }
        
        Ok(())
    }

    // Keep the old methods for backward compatibility
    async fn write_to_wal(&self, batches: &[RecordBatch], wal_path: &Path) -> Result<()> {
        let object_store = self.dataset.object_store();
        let mut writer = object_store.create(wal_path).await?;
        
        if batches.is_empty() {
            return Err(Error::invalid_input("Empty batches", location!()));
        }
        
        let schema = batches[0].schema();
        let batches = batches.to_vec();
        
        // Write all batches using async write operations
        let mut buffer = Vec::new();
        {
            let mut ipc_writer = StreamWriter::try_new(&mut buffer, &schema)?;
            for batch in batches {
                ipc_writer.write(&batch)?;
            }
            ipc_writer.finish()?;
        }
        
        // Write the buffer to the object store
        writer.write_all(&buffer).await?;
        writer.shutdown().await?;
        
        Ok(())
    }

    // async fn write_to_mem_table(&self, batches: &[RecordBatch]) -> Result<()> {
    //     Self::write_to_mem_table_internal(&self.dataset, &self.region, self.generation, &self.mem_table_store, batches).await
    // }

    async fn create_mem_table_dataset(&self) -> Result<Dataset> {
        Self::create_mem_table_dataset_internal(&self.dataset).await
    }

    fn get_primary_key_column(&self) -> Result<String> {
        Self::get_primary_key_column_internal(&self.dataset)
    }

    async fn get_mem_table_size(&self) -> Result<usize> {
        Self::get_mem_table_size_internal(&self.region, self.generation, &self.mem_table_store).await
    }

    async fn advance_mem_wal_generation(&self) -> Result<()> {
        Self::advance_mem_wal_generation_internal(
            &self.dataset,
            &self.region,
            self.generation,
            &self.mem_table_location,
            &self.wal_location,
            &self.mem_table_store,
        ).await
    }
}

