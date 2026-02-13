// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Read path for MemWAL - Trailing WAL Reader for consumers.
//!
//! This module provides read-only access to MemWAL data without claiming
//! writer epoch. It's designed for streaming consumers (like Kafka) that
//! need to trail the WAL from a separate process.
//!
//! ## Architecture
//!
//! The [`TrailingWalReader`] reconstructs an in-memory view of a region's
//! data by:
//!
//! 1. Reading the region manifest (without claiming epoch)
//! 2. Loading WAL entries after `replay_after_wal_entry_position`
//! 3. Reconstructing a MemTable from WAL entries
//! 4. Providing a [`RegionSnapshot`] for use with [`LsmScanner`]
//!
//! ## Usage
//!
//! ```ignore
//! use lance::dataset::mem_wal::read::TrailingWalReader;
//!
//! // Create a reader for a region
//! let reader = TrailingWalReader::open(object_store, base_path, region_id).await?;
//!
//! // Get snapshot for LsmScanner
//! let snapshot = reader.snapshot();
//!
//! // Read batches from a specific offset
//! let (batches, next_offset) = reader.read_from(0, 1000)?;
//! ```

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use lance_core::Result;
use lance_index::mem_wal::RegionManifest;
use lance_io::object_store::ObjectStore;
use log::{debug, info, warn};
use object_store::path::Path;
use uuid::Uuid;

use super::manifest::RegionManifestStore;
use super::memtable::batch_store::BatchStore;
use super::scanner::RegionSnapshot;
use super::util::{parse_bit_reversed_filename, region_wal_path, wal_entry_filename};
use super::wal::WalEntryData;

/// Configuration for the trailing WAL reader.
#[derive(Debug, Clone)]
pub struct TrailingWalReaderConfig {
    /// Batch size for parallel HEAD requests when scanning manifest versions.
    pub manifest_scan_batch_size: usize,
    /// Maximum capacity for the batch store (number of batches).
    pub max_batches: usize,
}

impl Default for TrailingWalReaderConfig {
    fn default() -> Self {
        Self {
            manifest_scan_batch_size: 2,
            max_batches: 100_000,
        }
    }
}

/// A read-only view of a MemWAL region.
///
/// This reader trails the WAL without claiming writer epoch, allowing
/// consumers in separate processes to read data written by producers.
///
/// The reader reconstructs an in-memory MemTable from WAL entries and
/// provides a snapshot that can be used with [`LsmScanner`] for queries.
pub struct TrailingWalReader {
    /// Object store for reading WAL and manifest files.
    object_store: Arc<ObjectStore>,
    /// Base path within the object store.
    base_path: Path,
    /// Region UUID.
    region_id: Uuid,
    /// Configuration.
    config: TrailingWalReaderConfig,
    /// The latest manifest read.
    manifest: Option<RegionManifest>,
    /// Reconstructed batch store from WAL entries.
    batch_store: Option<Arc<BatchStore>>,
    /// Schema of the data (derived from first batch).
    schema: Option<SchemaRef>,
    /// Current generation (from manifest).
    current_generation: u64,
    /// Last WAL entry position that has been replayed.
    last_replayed_wal_position: u64,
    /// Total rows in the batch store.
    total_rows: u64,
}

impl TrailingWalReader {
    /// Create a new trailing reader without loading any data.
    pub fn new(
        object_store: Arc<ObjectStore>,
        base_path: Path,
        region_id: Uuid,
        config: TrailingWalReaderConfig,
    ) -> Self {
        Self {
            object_store,
            base_path,
            region_id,
            config,
            manifest: None,
            batch_store: None,
            schema: None,
            current_generation: 1,
            last_replayed_wal_position: 0,
            total_rows: 0,
        }
    }

    /// Open an existing region and load its state.
    ///
    /// This reads the manifest and replays WAL entries to reconstruct
    /// the in-memory state.
    pub async fn open(
        object_store: Arc<ObjectStore>,
        base_path: Path,
        region_id: Uuid,
    ) -> Result<Self> {
        Self::open_with_config(
            object_store,
            base_path,
            region_id,
            TrailingWalReaderConfig::default(),
        )
        .await
    }

    /// Open with custom configuration.
    pub async fn open_with_config(
        object_store: Arc<ObjectStore>,
        base_path: Path,
        region_id: Uuid,
        config: TrailingWalReaderConfig,
    ) -> Result<Self> {
        let mut reader = Self::new(object_store, base_path, region_id, config);
        reader.refresh().await?;
        Ok(reader)
    }

    /// Refresh the reader by checking for new WAL entries.
    ///
    /// This method:
    /// 1. Re-reads the manifest to get latest state
    /// 2. Replays any new WAL entries since last refresh
    ///
    /// Call this periodically to pick up new data written by producers.
    pub async fn refresh(&mut self) -> Result<()> {
        // Read latest manifest
        let manifest_store = RegionManifestStore::new(
            self.object_store.clone(),
            &self.base_path,
            self.region_id,
            self.config.manifest_scan_batch_size,
        );

        let manifest = manifest_store.read_latest().await?;

        if manifest.is_none() {
            debug!(
                "No manifest found for region {}, region may not exist yet",
                self.region_id
            );
            return Ok(());
        }

        let manifest = manifest.unwrap();

        // Update manifest state
        self.current_generation = manifest.current_generation;

        // Determine which WAL entries to replay
        let replay_after = manifest.replay_after_wal_entry_position;
        let last_seen = manifest.wal_entry_position_last_seen;

        // We need to replay from max(replay_after + 1, last_replayed + 1) to last_seen
        let start_position = if self.batch_store.is_some() {
            // We already have some data, continue from where we left off
            self.last_replayed_wal_position + 1
        } else {
            // Fresh start - replay from after the flushed position
            replay_after + 1
        };

        if last_seen >= start_position {
            self.replay_wal_entries(start_position, last_seen).await?;
        }

        // Store flushed generations from manifest
        self.manifest = Some(manifest);

        Ok(())
    }

    /// Replay WAL entries in range [start, end] into the batch store.
    async fn replay_wal_entries(&mut self, start: u64, end: u64) -> Result<()> {
        let wal_dir = region_wal_path(&self.base_path, &self.region_id);

        // List WAL files to find which entries exist
        let wal_entries = self.list_wal_entries_in_range(&wal_dir, start, end).await?;

        if wal_entries.is_empty() {
            debug!(
                "No WAL entries found in range [{}, {}] for region {}",
                start, end, self.region_id
            );
            return Ok(());
        }

        info!(
            "Replaying {} WAL entries (positions {}-{}) for region {}",
            wal_entries.len(),
            wal_entries.first().unwrap(),
            wal_entries.last().unwrap(),
            self.region_id
        );

        // Ensure we have a batch store
        if self.batch_store.is_none() {
            self.batch_store = Some(Arc::new(BatchStore::with_capacity(self.config.max_batches)));
        }

        let batch_store = self.batch_store.as_ref().unwrap();

        // Replay each WAL entry
        for position in wal_entries {
            let path = wal_dir.child(wal_entry_filename(position).as_str());

            match WalEntryData::read(&self.object_store, &path).await {
                Ok(wal_data) => {
                    // Set schema from first batch if not already set
                    if self.schema.is_none() && !wal_data.batches.is_empty() {
                        self.schema = Some(wal_data.batches[0].schema());
                    }

                    // Append batches to store
                    for batch in wal_data.batches {
                        match batch_store.append(batch) {
                            Ok(_) => {
                                self.total_rows = batch_store.total_rows() as u64;
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to append batch from WAL entry {} for region {}: {:?}",
                                    position, self.region_id, e
                                );
                            }
                        }
                    }

                    self.last_replayed_wal_position = position;
                }
                Err(e) => {
                    // WAL entry might not exist yet if we're reading too fast
                    debug!(
                        "Failed to read WAL entry {} for region {}: {}",
                        position, self.region_id, e
                    );
                }
            }
        }

        Ok(())
    }

    /// List WAL entry positions in the given range.
    async fn list_wal_entries_in_range(
        &self,
        wal_dir: &Path,
        start: u64,
        end: u64,
    ) -> Result<Vec<u64>> {
        let mut positions = Vec::new();

        let list_result = self
            .object_store
            .inner
            .list(Some(wal_dir))
            .collect::<Vec<_>>()
            .await;

        for item in list_result {
            match item {
                Ok(meta) => {
                    if let Some(filename) = meta.location.filename() {
                        if filename.ends_with(".arrow") {
                            if let Some(position) = parse_bit_reversed_filename(filename) {
                                if position >= start && position <= end {
                                    positions.push(position);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Error listing WAL directory: {}", e);
                }
            }
        }

        positions.sort_unstable();
        Ok(positions)
    }

    /// Get a snapshot of this region for use with LsmScanner.
    ///
    /// The snapshot includes flushed generations from the manifest
    /// and can be combined with the active memtable data.
    pub fn snapshot(&self) -> RegionSnapshot {
        let mut snapshot =
            RegionSnapshot::new(self.region_id).with_current_generation(self.current_generation);

        // Add flushed generations from manifest
        if let Some(ref manifest) = self.manifest {
            for fg in &manifest.flushed_generations {
                snapshot = snapshot.with_flushed_generation(fg.generation, fg.path.clone());
            }
        }

        snapshot
    }

    /// Get the current high watermark (next row offset to be written).
    pub fn high_watermark(&self) -> u64 {
        self.total_rows
    }

    /// Get the log start offset (earliest available offset).
    ///
    /// Currently always 0, will change when log compaction is implemented.
    pub fn log_start_offset(&self) -> u64 {
        0
    }

    /// Get the durable watermark from the manifest.
    ///
    /// This is the WAL entry position last seen by the writer.
    pub fn durable_wal_position(&self) -> u64 {
        self.manifest
            .as_ref()
            .map(|m| m.wal_entry_position_last_seen)
            .unwrap_or(0)
    }

    /// Read batches starting from a row offset.
    ///
    /// Returns batches and the next offset to continue from.
    /// Returns `None` if the offset is at or beyond the high watermark.
    pub fn read_from(
        &self,
        from_row_offset: u64,
        max_rows: usize,
    ) -> Option<(Vec<RecordBatch>, u64)> {
        let batch_store = self.batch_store.as_ref()?;

        let high_watermark = batch_store.total_rows() as u64;
        if from_row_offset >= high_watermark {
            return None;
        }

        batch_store.read_rows_from(from_row_offset, max_rows, Some(high_watermark))
    }

    /// Get the batch store for direct access.
    ///
    /// Returns `None` if no WAL entries have been replayed yet.
    pub fn batch_store(&self) -> Option<&Arc<BatchStore>> {
        self.batch_store.as_ref()
    }

    /// Get the schema of the data.
    ///
    /// Returns `None` if no data has been read yet.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }

    /// Get the region ID.
    pub fn region_id(&self) -> Uuid {
        self.region_id
    }

    /// Get the current generation.
    pub fn current_generation(&self) -> u64 {
        self.current_generation
    }

    /// Get the last replayed WAL entry position.
    pub fn last_replayed_wal_position(&self) -> u64 {
        self.last_replayed_wal_position
    }

    /// Get the manifest (if loaded).
    pub fn manifest(&self) -> Option<&RegionManifest> {
        self.manifest.as_ref()
    }
}

impl std::fmt::Debug for TrailingWalReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrailingWalReader")
            .field("region_id", &self.region_id)
            .field("current_generation", &self.current_generation)
            .field(
                "last_replayed_wal_position",
                &self.last_replayed_wal_position,
            )
            .field("total_rows", &self.total_rows)
            .field("has_manifest", &self.manifest.is_some())
            .field("has_batch_store", &self.batch_store.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::TempDir;

    async fn create_local_store() -> (Arc<ObjectStore>, Path, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}", temp_dir.path().display());
        let (store, path) = ObjectStore::from_uri(&uri).await.unwrap();
        (store, path, temp_dir)
    }

    #[tokio::test]
    async fn test_trailing_reader_no_manifest() {
        let (store, base_path, _temp_dir) = create_local_store().await;
        let region_id = Uuid::new_v4();

        let reader = TrailingWalReader::open(store, base_path, region_id)
            .await
            .unwrap();

        assert!(reader.manifest().is_none());
        assert!(reader.batch_store().is_none());
        assert_eq!(reader.high_watermark(), 0);
    }

    #[tokio::test]
    async fn test_trailing_reader_snapshot() {
        let (store, base_path, _temp_dir) = create_local_store().await;
        let region_id = Uuid::new_v4();

        let reader = TrailingWalReader::new(
            store,
            base_path,
            region_id,
            TrailingWalReaderConfig::default(),
        );

        let snapshot = reader.snapshot();
        assert_eq!(snapshot.region_id, region_id);
        assert_eq!(snapshot.current_generation, 1);
        assert!(snapshot.flushed_generations.is_empty());
    }
}
