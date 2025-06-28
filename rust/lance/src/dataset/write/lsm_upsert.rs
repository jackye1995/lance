// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;
use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};
use datafusion::execution::SendableRecordBatchStream;
use lance_datafusion::utils::StreamingWriteSource;
use crate::Dataset;
use lance_core::{Error, Result};
use lance_index::mem_wal::MemWalId;
use object_store::path::Path;
use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use uuid::Uuid;
use snafu::location;
use tokio::sync::oneshot;

// Message types for the worker thread
enum WorkerMessage {
    Write {
        source: SendableRecordBatchStream,
        response_sender: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

// Future that represents the result of a write operation
pub struct WriteExecFuture {
    receiver: oneshot::Receiver<Result<()>>,
}

impl Future for WriteExecFuture {
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

/// Parameters for configuring a LogStructuredMergeJob
#[derive(Debug, Clone)]
pub struct LogStructuredMergeParams {
    /// The region for this LSM job. Defaults to "GLOBAL" if not provided.
    pub region: String,
    /// The size threshold in bytes for flushing MemTables. Defaults to 128MB.
    pub mem_table_flush_size: u64,
}

impl Default for LogStructuredMergeParams {
    fn default() -> Self {
        Self {
            region: "GLOBAL".to_string(),
            mem_table_flush_size: 128 * 1024 * 1024, // 128MB
        }
    }
}

/// Builder for creating LogStructuredMergeJob instances
pub struct LogStructuredMergeJobBuilder {
    dataset: Arc<Dataset>,
    params: LogStructuredMergeParams,
}

impl LogStructuredMergeJobBuilder {
    /// Create a new builder with the given dataset
    pub fn new(dataset: Arc<Dataset>) -> Self {
        Self {
            dataset,
            params: LogStructuredMergeParams::default(),
        }
    }

    /// Set the region for the LSM job
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.params.region = region.into();
        self
    }

    /// Set the MemTable flush size threshold in bytes
    pub fn mem_table_flush_size(mut self, size_bytes: usize) -> Self {
        self.params.mem_table_flush_size = size_bytes as u64;
        self
    }

    /// Build and start the LogStructuredMergeJob
    pub fn try_start(self) -> Result<LogStructuredMergeJob> {
        let mut job = LogStructuredMergeJob::new(self.dataset, self.params);
        job.start()?;
        Ok(job)
    }
}

pub struct LogStructuredMergeJob {
    dataset: Arc<Dataset>,
    job_id: Uuid,
    region: String,
    generation: u64,
    entry_id: std::sync::Mutex<u64>,
    mem_table_location: String,
    wal_location: String,
    sender: Option<Sender<WorkerMessage>>,
    worker_handle: Option<thread::JoinHandle<()>>,
    runtime: Arc<Runtime>,
    mem_table_store: Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
    mem_table_flush_size: u64,
}

impl LogStructuredMergeJob {
    fn new(dataset: Arc<Dataset>, params: LogStructuredMergeParams) -> Self {
        let job_id = Uuid::new_v4();
        let entry_id = std::sync::Mutex::new(0); // Start with entry 0
        
        // Create a tokio runtime for the worker thread
        let runtime = Arc::new(Runtime::new().expect("Failed to create tokio runtime"));
        
        // Determine generation from MemWAL index
        let (generation, mem_table_location, wal_location) = {
            let runtime_clone = runtime.clone();
            runtime.block_on(async {
                Self::determine_generation_and_locations(&dataset, &params.region, &job_id).await
            })
        };
        
        let mut job = Self {
            dataset,
            job_id,
            region: params.region,
            generation,
            entry_id,
            mem_table_location,
            wal_location,
            sender: None,
            worker_handle: None,
            runtime,
            mem_table_store: Arc::new(Mutex::new(HashMap::new())),
            mem_table_flush_size: params.mem_table_flush_size,
        };
        
        // Start the worker thread immediately
        job.start().expect("Failed to start worker thread");
        job
    }

    /// Determine the generation and locations based on existing MemWAL index
    async fn determine_generation_and_locations(
        dataset: &Arc<Dataset>,
        region: &str,
        job_id: &Uuid,
    ) -> (u64, String, String) {
        // Try to open existing MemWAL index
        let mem_wal_index = match dataset.open_mem_wal_index(&lance_index::metrics::NoOpMetricsCollector).await {
            Ok(Some(index)) => index,
            Ok(None) => {
                // No MemWAL index exists, start with generation 0
                let generation = 0;
                let mem_table_location = format!("memory://{}/{}/{}/{}", 
                    job_id, dataset.base.to_string(), region, generation);
                let wal_location = format!("{}/_wal/{}/{}", 
                    dataset.base.to_string(), region, generation);
                
                // Advance the generation to create generation 0
                let mut dataset_clone = (**dataset).clone();
                if let Err(e) = crate::index::mem_wal::advance_mem_wal_generation(
                    &mut dataset_clone,
                    region,
                    &mem_table_location,
                    &wal_location,
                    None,
                ).await {
                    eprintln!("Failed to advance MemWAL generation: {}", e);
                }
                
                return (generation, mem_table_location, wal_location);
            }
            Err(_) => {
                // Failed to load indices, start with generation 0
                let generation = 0;
                let mem_table_location = format!("memory://{}/{}/{}/{}", 
                    job_id, dataset.base.to_string(), region, generation);
                let wal_location = format!("{}/_wal/{}/{}", 
                    dataset.base.to_string(), region, generation);
                
                // Advance the generation to create generation 0
                let mut dataset_clone = (**dataset).clone();
                if let Err(e) = crate::index::mem_wal::advance_mem_wal_generation(
                    &mut dataset_clone,
                    region,
                    &mem_table_location,
                    &wal_location,
                    None,
                ).await {
                    eprintln!("Failed to advance MemWAL generation: {}", e);
                }
                
                return (generation, mem_table_location, wal_location);
            }
        };

        // Check if there are MemWALs for this region
        let region_mem_wals = mem_wal_index.mem_wal_map.get(region);
        
        if let Some(generations) = region_mem_wals {
            if generations.is_empty() {
                // No MemWAL for this region, advance to create generation 0
                let generation = 0;
                let mem_table_location = format!("memory://{}/{}/{}/{}", 
                    job_id, dataset.base.to_string(), region, generation);
                let wal_location = format!("{}/_wal/{}/{}", 
                    dataset.base.to_string(), region, generation);
                
                // Advance the generation to create generation 0
                let mut dataset_clone = (**dataset).clone();
                if let Err(e) = crate::index::mem_wal::advance_mem_wal_generation(
                    &mut dataset_clone,
                    region,
                    &mem_table_location,
                    &wal_location,
                    None,
                ).await {
                    eprintln!("Failed to advance MemWAL generation: {}", e);
                }
                
                return (generation, mem_table_location, wal_location);
            } else {
                // Find the latest generation
                let latest_mem_wal = generations.values()
                    .max_by_key(|mem_wal| mem_wal.id.generation)
                    .unwrap();
                
                if !latest_mem_wal.sealed {
                    // Use the unsealed latest generation and replay WAL
                    let generation = latest_mem_wal.id.generation;
                    let mem_table_location = latest_mem_wal.mem_table_location.clone();
                    let wal_location = latest_mem_wal.wal_location.clone();
                    
                    // TODO: Replay WAL entries to populate MemTable
                    // This would involve reading the WAL files and reconstructing the MemTable
                    
                    return (generation, mem_table_location, wal_location);
                } else {
                    // All MemWALs are sealed, advance to create new generation
                    let new_generation = latest_mem_wal.id.generation + 1;
                    let mem_table_location = format!("memory://{}/{}/{}/{}", 
                        job_id, dataset.base.to_string(), region, new_generation);
                    let wal_location = format!("{}/_wal/{}/{}", 
                        dataset.base.to_string(), region, new_generation);
                    
                    // Advance the generation
                    let mut dataset_clone = (**dataset).clone();
                    if let Err(e) = crate::index::mem_wal::advance_mem_wal_generation(
                        &mut dataset_clone,
                        region,
                        &mem_table_location,
                        &wal_location,
                        Some(&latest_mem_wal.mem_table_location),
                    ).await {
                        eprintln!("Failed to advance MemWAL generation: {}", e);
                    }
                    
                    return (new_generation, mem_table_location, wal_location);
                }
            }
        } else {
            // No MemWAL for this region, advance to create generation 0
            let generation = 0;
            let mem_table_location = format!("memory://{}/{}/{}/{}", 
                job_id, dataset.base.to_string(), region, generation);
            let wal_location = format!("{}/_wal/{}/{}", 
                dataset.base.to_string(), region, generation);
            
            // Advance the generation to create generation 0
            let mut dataset_clone = (**dataset).clone();
            if let Err(e) = crate::index::mem_wal::advance_mem_wal_generation(
                &mut dataset_clone,
                region,
                &mem_table_location,
                &wal_location,
                None,
            ).await {
                eprintln!("Failed to advance MemWAL generation: {}", e);
            }
            
            return (generation, mem_table_location, wal_location);
        }
    }

    /// Create a new builder for LogStructuredMergeJob
    pub fn builder(dataset: Arc<Dataset>) -> LogStructuredMergeJobBuilder {
        LogStructuredMergeJobBuilder::new(dataset)
    }

    /// Start the background worker thread that processes execution tasks sequentially.
    fn start(&mut self) -> Result<()> {
        if self.sender.is_some() {
            return Err(Error::invalid_input("Worker already started", location!()));
        }

        let (sender, receiver) = mpsc::channel();
        let dataset = self.dataset.clone();
        let region = self.region.clone();
        let generation = self.generation;
        let entry_id = self.entry_id.clone();
        let mem_table_location = self.mem_table_location.clone();
        let wal_location = self.wal_location.clone();
        let runtime = self.runtime.clone();
        let mem_table_store = self.mem_table_store.clone();
        let mem_table_flush_size = self.mem_table_flush_size;

        let worker_handle = thread::spawn(move || {
            Self::worker_loop(
                receiver,
                dataset,
                region,
                generation,
                entry_id,
                mem_table_location,
                wal_location,
                runtime,
                mem_table_store,
                mem_table_flush_size,
            );
        });

        self.sender = Some(sender);
        self.worker_handle = Some(worker_handle);
        Ok(())
    }

    /// Stop the background worker thread.
    pub fn stop(&mut self) -> Result<()> {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(WorkerMessage::Shutdown);
        }
        
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
        
        Ok(())
    }

    /// Submit an execution task and return a future that resolves when the task completes.
    pub fn write(&self, source: SendableRecordBatchStream) -> WriteExecFuture {
        let (response_sender, response_receiver) = oneshot::channel();
        
        if let Some(ref sender) = self.sender {
            let _ = sender.send(WorkerMessage::Write {
                source,
                response_sender,
            });
        } else {
            // If worker is not started, immediately return an error
            let _ = response_sender.send(Err(Error::invalid_input(
                "Worker not started. Call start() first.",
                location!(),
            )));
        }
        
        WriteExecFuture {
            receiver: response_receiver,
        }
    }

    /// Background worker loop that processes execution tasks sequentially.
    fn worker_loop(
        receiver: Receiver<WorkerMessage>,
        dataset: Arc<Dataset>,
        region: String,
        generation: u64,
        entry_id: std::sync::Mutex<u64>,
        mem_table_location: String,
        wal_location: String,
        runtime: Arc<Runtime>,
        mem_table_store: Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
        mem_table_flush_size: u64,
    ) {
        for message in receiver {
            match message {
                WorkerMessage::Write { source, response_sender } => {
                    let result = runtime.block_on(async {
                        Self::execute_internal(
                            &dataset,
                            &region,
                            generation,
                            &entry_id,
                            &mem_table_location,
                            &wal_location,
                            &mem_table_store,
                            mem_table_flush_size,
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

    /// Internal execution logic (moved from the original execute method).
    async fn execute_internal(
        dataset: &Arc<Dataset>,
        region: &str,
        generation: u64,
        entry_id: &std::sync::Mutex<u64>,
        mem_table_location: &str,
        wal_location: &str,
        mem_table_store: &Arc<Mutex<HashMap<String, Arc<Dataset>>>>,
        mem_table_flush_size: u64,
        source: SendableRecordBatchStream,
    ) -> Result<()> {
        // Retry WAL write until success, incrementing entry_id on each retry
        let (batches, final_entry_id) = loop {
            let current_entry_id = {
                let mut entry_id_guard = entry_id.lock().unwrap();
                let current = *entry_id_guard;
                *entry_id_guard += 1;
                current
            };

            // 1. write to WAL and collect batches simultaneously
            let wal_file_path = Self::wal_file_path_with_entry_id(region, generation, current_entry_id);
            match Self::write_to_wal_and_collect_batches(dataset, source.clone(), &wal_file_path).await {
                Ok(batches) => {
                    if batches.is_empty() {
                        return Err(Error::invalid_input("Empty source stream", location!()));
                    }
                    break (batches, current_entry_id);
                }
                Err(e) => {
                    eprintln!("Failed to write to WAL with entry_id {}: {}", current_entry_id, e);
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

    /// Generate the WAL file path for a specific entry_id using the reversed 64-bit binary value.
    fn wal_file_path_with_entry_id(region: &str, generation: u64, entry_id: u64) -> Path {
        let reversed = entry_id.reverse_bits();
        let reversed_bits = format!("{:064b}", reversed);
        Path::from(format!("_wal/{}/{}/{}.arrow", region, generation, reversed_bits))
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
        let new_mem_table_location = format!("memory://{}/{}/{}/{}", 
            Uuid::new_v4(), dataset.base.to_string(), region, new_generation);
        let new_wal_location = format!("{}/_wal/{}/{}", 
            dataset.base.to_string(), region, new_generation);
        
        let mut dataset_clone = (**dataset).clone();
        
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
                vec!["id".to_string()], // Assuming 'id' is the primary key
            )?
            .when_matched(crate::dataset::WhenMatched::UpdateAll)
            .when_not_matched(crate::dataset::WhenNotMatched::InsertAll)
            .mark_mem_wal_as_flushed(mem_wal_id, expected_mem_table_location)
            .await?
            .try_build()?;

            // Execute the merge insert to flush the MemTable
            // Convert the dataset scan to a proper stream
            let scan = mem_table.scan();
            let stream = scan.try_into_stream().await?;
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


