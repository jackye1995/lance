// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

mod mem_table;
mod wal;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;
use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::*;
use lance_datafusion::utils::StreamingWriteSource;
use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::mem_wal::MemWalId;
use object_store::path::Path;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use futures::{FutureExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use uuid::Uuid;
use snafu::location;
use tokio::sync::oneshot;
use crate::dataset::write::lsm::mem_table::MemTableStore;
use crate::dataset::write::lsm::wal::WalStore;
use crate::index::DatasetIndexInternalExt;
use crate::index::mem_wal::advance_mem_wal_generation;

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
    /// The region for this LSM job. Defaults to "GLOBAL" if not provided.
    pub region: String,

    /// The size threshold in bytes for flushing MemTables. Defaults to 128MB.
    pub mem_table_flush_size: u64,

    /// The number of writes to accumulate before writing to the WAL and MemTable.
    /// This helps the case of many concurrent tiny-size writes,
    /// trading off the durability because all writes in a batch either all succeed or all fail.
    /// Defaults to 10.
    pub batch_size: u64,
}

impl Default for LogStructuredMergeParams {
    fn default() -> Self {
        Self {
            region: "GLOBAL".to_string(),
            mem_table_flush_size: 128 * 1024 * 1024,
            batch_size: 10,
        }
    }
}

pub struct LogStructuredMergeJobBuilder {
    dataset: Arc<Dataset>,
    params: LogStructuredMergeParams,
}

impl LogStructuredMergeJobBuilder {

    pub fn new(dataset: Arc<Dataset>) -> Self {
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
    job_id: Uuid,
    dataset: Arc<Dataset>,
    params: LogStructuredMergeParams,
    mem_table_store: Arc<MemTableStore>,
    wal_store: Arc<WalStore>,

    runtime: Arc<Runtime>,
    sender: Sender<WorkerMessage>,
    worker_handle: thread::JoinHandle<()>,
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
    async fn start(dataset: Arc<Dataset>, params: LogStructuredMergeParams) -> Result<(Self)> {
        let job_id = Uuid::new_v4();
        let runtime = Arc::new(Runtime::new()
            .expect(&format!("Failed to create tokio runtime for LogStructuredMergeJob {}", job_id)));

        let (sender, receiver) = mpsc::channel();
        let mem_table_store = Arc::new(MemTableStore::new(dataset.clone(), params.region.clone().as_str()));
        let wal_store = Arc::new(WalStore::new());

        // advance the generation.
        // if there is an unsealed MemWAL, it will be sealed.
        let gen_dir = Uuid::new_v4().to_string();
        let new_mem_table_location = format!("memory://{}/{}", job_id, gen_dir);
        let new_wal_location = dataset.base.child("_wal").child(job_id.to_string()).child(gen_dir).to_string();

        advance_mem_wal_generation(
            &mut dataset,
            params.region,
            new_mem_table_location,
            new_wal_location,

        ).await?;

        let worker_runtime = runtime.clone();
        let worker_dataset = dataset.clone();
        let worker_params = params.clone();
        let worker_mem_table_store = mem_table_store.clone();
        let worker_wal_store = wal_store.clone();
        let worker_handle = thread::spawn(move || {
            Self::worker_loop(
                worker_runtime,
                receiver,
                worker_dataset,
                worker_params,
                worker_mem_table_store,
                worker_wal_store,
            );
        });

        Ok(Self {
            job_id,
            dataset,
            params,
            mem_table_store,
            wal_store,
            runtime,
            sender,
            worker_handle,
        })
    }

    fn worker_loop(
        runtime: Arc<Runtime>,
        receiver: Receiver<WorkerMessage>,
        dataset: Arc<Dataset>,
        params: LogStructuredMergeParams,
        mem_table_store: Arc<MemTableStore>,
        wal_store: Arc<WalStore>,
    ) {
        for message in receiver {
            // TODO: can accumulate a batch before executing.
            match message {
                WorkerMessage::Write { source, response_sender } => {
                    let result = runtime.block_on(async {
                        Self::do_execute(
                            dataset.clone(),
                            params.clone(),
                            mem_table_store.clone(),
                            wal_store.clone(),
                            source,
                        ).await
                    });
                    let _ = response_sender.send(result);
                }
                WorkerMessage::Shutdown => {
                    break;
                }
            }
        }
    }

    /// Stop the background worker thread.
    pub fn stop(&mut self) -> Result<()> {
        self.sender.send(WorkerMessage::Shutdown)?;

        
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
        
        Ok(())
    }

    /// Submit an execution task and return a future that resolves when the task completes.
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

    /// Get all MemTables for the current region
    async fn mem_tables_for_region(&self) -> Result<Vec<Arc<Dataset>>> {
        let store = self.mem_table_store.lock().unwrap();
        let mut mem_tables = Vec::new();
        
        // Collect all MemTables for this region
        for (mem_wal_id, mem_table) in store.iter() {
            if mem_wal_id.starts_with(&format!("{}_{}", self.region, self.generation)) {
                mem_tables.push(mem_table.clone());
            }
        }
        
        Ok(mem_tables)
    }

    /// Actual execution logic for a batch of
    async fn do_execute(
        dataset: Arc<Dataset>,
        params: LogStructuredMergeParams,
        mem_table_store: Arc<MemTableStore>,
        wal_store: Arc<WalStore>,
        source: SendableRecordBatchStream,
    ) -> Result<()> {
        // 1. Retry WAL write until success, incrementing entry_id on each retry
        let (batches, final_entry_id) = loop {
            let current_entry_id = {
                let mut entry_id_guard = entry_id.lock().unwrap();
                let current = *entry_id_guard;
                *entry_id_guard += 1;
                current
            };

            // 1-1. write to WAL and collect batches simultaneously
            let wal_file_path = Self::wal_file_path_with_entry_id(region, generation, current_entry_id);
            match Self::write_to_wal_and_collect_batches(dataset, source, &wal_file_path).await {
                Ok(batches) => {
                    if batches.is_empty() {
                        return Err(Error::invalid_input("Empty source stream", location!()));
                    }
                    break (batches, current_entry_id);
                }
                Err(e) => {
                    log::warn!("Failed to write to WAL with entry ID {}: {}", current_entry_id, e);
                    // Continue to next iteration with incremented entry_id
                }
            }
        };

        // 2. commit to dataset
        let mut dataset_clone = (**dataset).clone();
        crate::index::mem_wal::append_mem_wal_entry(
            &mut dataset_clone,
            region,
            generation,
            final_entry_id,
            mem_table_location,
        ).await?;

        // 3. write to MemTable
        Self::write_to_mem_table_internal(dataset, region, generation, mem_table_store, &batches).await?;

        // 4. check for criteria, if MemTable size > mem_table_flush_size, advance generation
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

    pub async fn execute_reader(
        &self,
        source: impl StreamingWriteSource,
    ) -> lance_core::Result<()> {
        let stream = source.into_stream();
        self.write(stream).await
    }

    /// Generate the WAL file path using the reversed 64-bit binary value of entry_id as a 64-character bit string.
    fn wal_file_path(&self) -> Path {
        let entry_id = *self.entry_id.lock().unwrap();
        Self::wal_file_path_with_entry_id(&self.region, self.generation, entry_id)
    }

    /// Write batches to WAL while simultaneously collecting them in memory for later use.
    async fn write_to_wal_and_collect_batches(
        dataset: &Arc<Dataset>,
        source: SendableRecordBatchStream,
        wal_path: &Path,
    ) -> Result<Vec<RecordBatch>> {
        let object_store = dataset.object_store();
        let mut writer = object_store.create(wal_path).await?;
        
        let mut batches = Vec::new();
        let mut source_pinned = source;
        let mut buffer = Vec::new();
        let mut ipc_writer = None;
        
        while let Some(batch) = source_pinned.next().await {
            let batch = batch?;
            
            // Initialize IPC writer on first batch
            if ipc_writer.is_none() {
                let schema = batch.schema();
                ipc_writer = Some(StreamWriter::try_new(&mut buffer, &schema)?);
            }
            
            // Write to IPC buffer
            if let Some(ref mut writer) = ipc_writer {
                writer.write(&batch)?;
            }
            
            // Create an in-memory copy and append to batches list
            batches.push(batch);
        }
        
        // Finish the IPC writer
        if let Some(mut writer) = ipc_writer {
            writer.finish()?;
        }
        
        // Write the buffer to the object store
        writer.write_all(&buffer).await?;
        writer.shutdown().await?;
        
        Ok(batches)
    }

    async fn write_to_mem_table_internal(
        dataset: &Arc<Dataset>,
        region: &str,
        generation: u64,
        mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let mem_wal_id = format!("{}_{}", region, generation);
        
        // Get or create MemTable from the singleton store
        let mem_table = {
            let mut store = mem_table_store.lock().unwrap();
            if let Some(mem_table) = store.get(&mem_wal_id) {
                mem_table.clone()
            } else {
                // Create new MemTable dataset
                let mem_table_dataset = Self::create_mem_table_dataset_internal(dataset).await?;
                let mem_table_arc = Arc::new(mem_table_dataset);
                store.insert(mem_wal_id.clone(), mem_table_arc.clone());
                mem_table_arc
            }
        };

        // Create a stream from the batches for merge insert
        let batches = batches.to_vec();
        let stream = futures::stream::iter(batches.into_iter().map(Ok::<_, arrow::error::ArrowError>))
            .boxed();

        // Perform merge insert on the MemTable
        let merge_insert_job = crate::dataset::MergeInsertBuilder::try_new(
            mem_table.clone(),
            vec![Self::get_primary_key_column_internal(dataset)?],
        )?
        .when_matched(crate::dataset::WhenMatched::UpdateAll)
        .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
        .try_build()?;

        let (_, _) = merge_insert_job.execute(stream).await?;
        Ok(())
    }

    async fn create_mem_table_dataset_internal(dataset: &Arc<Dataset>) -> Result<Dataset> {
        // Create an empty dataset in memory with the same schema as the main dataset
        let schema = dataset.schema();
        let arrow_schema: arrow_schema::Schema = schema.as_ref().into();
        let empty_batch = RecordBatch::new_empty(Arc::new(arrow_schema.clone()));
        
        // Use RecordBatchIterator for proper RecordBatchReader implementation
        let batches = vec![Ok(empty_batch)];
        let reader = arrow::record_batch::RecordBatchIterator::new(batches, Arc::new(arrow_schema));
        
        let dataset = crate::Dataset::write(
            reader,
            &format!("memory://temp/{}", Uuid::new_v4()),
            None,
        ).await?;
        
        Ok(dataset)
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

    async fn get_mem_table_size_internal(region: &str, generation: u64, mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>) -> Result<usize> {
        let mem_wal_id = format!("{}_{}", region, generation);
        let store = mem_table_store.lock().unwrap();
        
        if let Some(mem_table) = store.get(&mem_wal_id) {
            // Calculate approximate size by counting rows and estimating row size
            let row_count = mem_table.count_rows(None).await?;
            let schema = mem_table.schema();
            let estimated_row_size = schema.fields.len() * 8; // Rough estimate
            Ok(row_count as usize * estimated_row_size)
        } else {
            Ok(0)
        }
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
                dataset.unenforced_primary_key(),
            )?
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
            .mark_mem_wal_as_flushed(mem_wal_id, expected_mem_table_location)
            .await?
            .try_build()?;
            
            let stream = mem_table.scan().try_into_stream().await?;
            let (_, _) = merge_insert_job.execute(stream.into()).await?;
            
            // Remove the MemTable from the store after successful flush
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

    async fn write_to_mem_table(&self, batches: &[RecordBatch]) -> Result<()> {
        Self::write_to_mem_table_internal(&self.dataset, &self.region, self.generation, &self.mem_table_store, batches).await
    }

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

impl Drop for LogStructuredMergeJob {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

