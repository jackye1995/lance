// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array, UInt8Array};
use arrow_schema::Schema;
use arrow_select;
use datafusion::common::Result as DFResult;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::scalar::ScalarValue;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, ExecutionPlan, PlanProperties,
    },
};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::{stream, StreamExt};
use lance_arrow::RecordBatchExt;
use roaring::RoaringTreemap;

use crate::dataset::transaction::UpdateMode::RewriteRows;
use crate::dataset::utils::CapturedRowIds;
use crate::dataset::write::merge_insert::{
    create_duplicate_row_error, format_key_values_on_columns, DedupeOrdering,
};
use crate::{
    dataset::{
        transaction::{Operation, Transaction},
        write::{
            merge_insert::{
                assign_action::Action, exec::MergeInsertMetrics, MergeInsertParams, MergeStats,
                MERGE_ACTION_COLUMN,
            },
            write_fragments_internal, WriteParams,
        },
    },
    Dataset, Result,
};
use lance_core::{Error, ROW_ADDR, ROW_ID};
use lance_table::format::{Fragment, RowIdMeta};
use snafu::location;
use std::collections::BTreeMap;

/// Accumulated update row position for running deduplication
struct AccumulatedUpdate {
    /// The dedupe column value for this row
    dedupe_value: ScalarValue,
    /// The row address of the target row
    row_addr: u64,
    /// Index of the batch in buffered_batches
    batch_idx: usize,
    /// Row index within the batch
    row_idx: usize,
}

/// Shared state for merge insert operations to simplify lock management
struct MergeState {
    /// Row addresses that need to be deleted, due to a row update or delete action
    delete_row_addrs: RoaringTreemap,
    /// Shared collection to capture row ids that need to be updated
    updating_row_ids: Arc<Mutex<CapturedRowIds>>,
    /// Merge operation metrics
    metrics: MergeInsertMetrics,
    /// Whether the dataset uses stable row ids.
    stable_row_ids: bool,
    /// Set to track processed row IDs to detect duplicates
    processed_row_ids: HashSet<u64>,
    /// The "on" column names for merge operation
    on_columns: Vec<String>,
    /// Dedupe column name (if configured)
    dedupe_by: Option<String>,
    /// Dedupe ordering
    dedupe_ordering: DedupeOrdering,
    /// Accumulated best rows for deduplication: row_id -> AccumulatedUpdate
    accumulated_updates: BTreeMap<u64, AccumulatedUpdate>,
    /// Buffered batches for dedupe mode (contains only data columns, indexed by batch_idx)
    /// Uses Option to allow clearing batches that are no longer referenced
    buffered_batches: Vec<Option<RecordBatch>>,
    /// Reference counts for each buffered batch
    batch_ref_counts: Vec<usize>,
}

impl MergeState {
    fn new(
        metrics: MergeInsertMetrics,
        stable_row_ids: bool,
        on_columns: Vec<String>,
        dedupe_by: Option<String>,
        dedupe_ordering: DedupeOrdering,
    ) -> Self {
        Self {
            delete_row_addrs: RoaringTreemap::new(),
            updating_row_ids: Arc::new(Mutex::new(CapturedRowIds::new(stable_row_ids))),
            metrics,
            stable_row_ids,
            processed_row_ids: HashSet::new(),
            on_columns,
            dedupe_by,
            dedupe_ordering,
            accumulated_updates: BTreeMap::new(),
            buffered_batches: Vec::new(),
            batch_ref_counts: Vec::new(),
        }
    }

    /// Check if the new value is better than the existing value based on dedupe_ordering.
    ///
    /// Follows DataFusion/SQL sort semantics where NULL loses to any non-NULL value:
    /// - ASC (NULLS LAST): NULL is considered larger than any non-NULL
    /// - DESC (NULLS FIRST): NULL is considered smaller than any non-NULL
    ///
    /// For non-NULL values, uses ScalarValue's total ordering which handles
    /// floats as: -Inf < normal values < Inf < NaN
    fn is_better_value(&self, new: &ScalarValue, existing: &ScalarValue) -> bool {
        let new_is_null = new.is_null();
        let existing_is_null = existing.is_null();

        match (new_is_null, existing_is_null) {
            // Both NULL: keep existing (no improvement)
            (true, true) => false,
            // New is NULL, existing is non-NULL: existing wins (NULL loses)
            (true, false) => false,
            // New is non-NULL, existing is NULL: new wins (NULL loses)
            (false, true) => true,
            // Both non-NULL: compare based on ordering
            (false, false) => match self.dedupe_ordering {
                DedupeOrdering::Ascending => new < existing,
                DedupeOrdering::Descending => new > existing,
            },
        }
    }

    /// Get the dedupe column array from a batch.
    /// Handles both flat columns (e.g., "ts") and nested columns (e.g., "metrics.ts").
    /// Tries both the original name and with "source." prefix.
    fn get_dedupe_column<'a>(&self, batch: &'a RecordBatch) -> Option<&'a ArrayRef> {
        let col_name = self.dedupe_by.as_ref()?;

        // Try qualified name first (source.col_name or source.struct.col_name)
        let qualified_name = format!("source.{}", col_name);
        if let Some(col) = batch.column_by_qualified_name(&qualified_name) {
            return Some(col);
        }

        // Try unqualified name (col_name or struct.col_name)
        // This uses column_by_qualified_name which handles nested paths like "struct.field"
        batch.column_by_qualified_name(col_name)
    }

    /// Process a single row based on its action, updating internal state
    ///
    /// For dedupe mode, `batch_idx` must be provided (the index in buffered_batches).
    fn process_row_action(
        &mut self,
        action: Action,
        row_idx: usize,
        row_addr_array: &UInt64Array,
        row_id_array: &UInt64Array,
        batch: &RecordBatch,
        batch_idx: Option<usize>,
    ) -> DFResult<Option<usize>> {
        match action {
            Action::Delete => {
                // Delete action - only delete, don't write back
                if !row_addr_array.is_null(row_idx) {
                    let row_addr = row_addr_array.value(row_idx);
                    self.delete_row_addrs.insert(row_addr);
                    self.metrics.num_deleted_rows.add(1);
                }
                Ok(None) // Don't keep this row
            }
            Action::UpdateAll => {
                // Update action - delete old row AND insert new data
                if !row_addr_array.is_null(row_idx) {
                    let row_addr = row_addr_array.value(row_idx);
                    let row_id = row_id_array.value(row_idx);

                    if let Some(dedupe_col) = self.get_dedupe_column(batch) {
                        // Running deduplication mode
                        let dedupe_value = ScalarValue::try_from_array(dedupe_col, row_idx)?;

                        let new_batch_idx = batch_idx.ok_or_else(|| {
                            datafusion::error::DataFusionError::Internal(
                                "batch_idx required for dedupe mode".to_string(),
                            )
                        })?;

                        let should_use_new = match self.accumulated_updates.get(&row_id) {
                            None => true,
                            Some(existing) => {
                                self.is_better_value(&dedupe_value, &existing.dedupe_value)
                            }
                        };

                        if should_use_new {
                            // Handle ref counting when replacing an existing entry
                            if let Some(old) = self.accumulated_updates.get(&row_id) {
                                let old_batch_idx = old.batch_idx;
                                if old_batch_idx != new_batch_idx {
                                    // Only adjust ref counts when switching batches
                                    self.batch_ref_counts[old_batch_idx] -= 1;
                                    // Clear batch if no longer referenced
                                    if self.batch_ref_counts[old_batch_idx] == 0 {
                                        self.buffered_batches[old_batch_idx] = None;
                                    }
                                    self.batch_ref_counts[new_batch_idx] += 1;
                                }
                                // If same batch, ref count stays the same
                            } else {
                                // New entry, increment ref count
                                self.batch_ref_counts[new_batch_idx] += 1;
                            }

                            self.accumulated_updates.insert(
                                row_id,
                                AccumulatedUpdate {
                                    dedupe_value,
                                    row_addr,
                                    batch_idx: new_batch_idx,
                                    row_idx,
                                },
                            );
                        }

                        // In dedupe mode, we return None here and process accumulated rows later
                        return Ok(None);
                    }

                    // Check for duplicate _rowid in the current merge operation
                    if !self.processed_row_ids.insert(row_id) {
                        return Err(create_duplicate_row_error(batch, row_idx, &self.on_columns));
                    }

                    self.delete_row_addrs.insert(row_addr);

                    if self.stable_row_ids {
                        self.updating_row_ids.lock().unwrap().capture(&[row_id])?;
                    }
                    // Don't count as actual delete - this is an update
                }

                self.metrics.num_updated_rows.add(1);
                Ok(Some(row_idx)) // Keep this row for writing
            }
            Action::Insert => {
                // Insert action - just insert new data
                self.metrics.num_inserted_rows.add(1);
                Ok(Some(row_idx)) // Keep this row for writing
            }
            Action::Nothing => {
                // Do nothing action - keep the row but don't count it
                Ok(None)
            }
            Action::Fail => {
                // Fail action - return an error to fail the operation
                Err(datafusion::error::DataFusionError::Execution(format!(
                    "Merge insert failed: found matching row with key values: {}",
                    format_key_values_on_columns(batch, row_idx, &self.on_columns)
                )))
            }
        }
    }

    /// Finalize the accumulated updates after all batches have been processed.
    /// Returns a RecordBatch containing all the deduplicated rows to write.
    fn finalize_dedupe(&mut self, output_schema: &Arc<Schema>) -> DFResult<Option<RecordBatch>> {
        if self.accumulated_updates.is_empty() {
            return Ok(None);
        }

        // Group accumulated updates by batch_idx for efficient extraction
        let mut batch_rows: BTreeMap<usize, Vec<(u64, u64, usize)>> = BTreeMap::new(); // batch_idx -> [(row_id, row_addr, row_idx)]
        for (row_id, acc) in std::mem::take(&mut self.accumulated_updates) {
            batch_rows
                .entry(acc.batch_idx)
                .or_default()
                .push((row_id, acc.row_addr, acc.row_idx));
        }

        let mut result_batches: Vec<RecordBatch> = Vec::new();

        for (batch_idx, rows) in batch_rows {
            let buffered_batch = self.buffered_batches[batch_idx].as_ref().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Buffered batch {} was unexpectedly cleared",
                    batch_idx
                ))
            })?;

            // Update state for each row
            for (row_id, row_addr, _) in &rows {
                self.delete_row_addrs.insert(*row_addr);
                self.metrics.num_updated_rows.add(1);
                if self.stable_row_ids {
                    let _ = self.updating_row_ids.lock().unwrap().capture(&[*row_id]);
                }
            }

            // Extract rows from this batch
            let row_indices: Vec<u32> = rows.iter().map(|(_, _, idx)| *idx as u32).collect();
            let indices = arrow_array::UInt32Array::from(row_indices);
            let extracted = arrow_select::take::take_record_batch(buffered_batch, &indices)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            result_batches.push(extracted);
        }

        // Concatenate all extracted batches
        if result_batches.is_empty() {
            Ok(None)
        } else {
            let concatenated = arrow_select::concat::concat_batches(output_schema, &result_batches)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(Some(concatenated))
        }
    }

    /// Check if running in dedupe mode
    fn is_dedupe_mode(&self) -> bool {
        self.dedupe_by.is_some()
    }

    /// Buffer a batch with only data columns for deduplication.
    /// Returns the batch index in buffered_batches.
    fn buffer_data_batch(
        &mut self,
        batch: &RecordBatch,
        data_column_indices: &[usize],
        output_schema: &Arc<Schema>,
    ) -> DFResult<usize> {
        // Create a batch with only data columns
        let columns: Vec<_> = data_column_indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect();
        let data_batch = RecordBatch::try_new(output_schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
        let batch_idx = self.buffered_batches.len();
        self.buffered_batches.push(Some(data_batch));
        self.batch_ref_counts.push(0); // Will be incremented when referenced
        Ok(batch_idx)
    }
}

/// Inserts new rows and updates existing rows in the target table.
///
/// This does the actual write.
///
/// This is implemented by moving updated rows to new fragments. This mode
/// is most optimal when updating the full schema.
///
#[derive(Debug)]
pub struct FullSchemaMergeInsertExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: Arc<Dataset>,
    params: MergeInsertParams,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    merge_stats: Arc<Mutex<Option<MergeStats>>>,
    transaction: Arc<Mutex<Option<Transaction>>>,
    affected_rows: Arc<Mutex<Option<RoaringTreemap>>>,
}

impl FullSchemaMergeInsertExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        params: MergeInsertParams,
    ) -> DFResult<Self> {
        let empty_schema = Arc::new(arrow_schema::Schema::empty());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(empty_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            input,
            dataset,
            params,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            merge_stats: Arc::new(Mutex::new(None)),
            transaction: Arc::new(Mutex::new(None)),
            affected_rows: Arc::new(Mutex::new(None)),
        })
    }

    /// Returns the merge statistics if the execution has completed.
    /// Returns `None` if the execution is still in progress or hasn't started.
    pub fn merge_stats(&self) -> Option<MergeStats> {
        self.merge_stats.lock().ok().and_then(|guard| guard.clone())
    }

    /// Returns the transaction if the execution has completed.
    /// Returns `None` if the execution is still in progress or hasn't started.
    pub fn transaction(&self) -> Option<Transaction> {
        self.transaction.lock().ok().and_then(|guard| guard.clone())
    }

    /// Returns the affected rows (deleted/updated row addresses) if the execution has completed.
    /// Returns `None` if the execution is still in progress or hasn't started.
    pub fn affected_rows(&self) -> Option<RoaringTreemap> {
        self.affected_rows
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Creates a filtered stream that captures row addresses for deletion and returns
    /// a stream with only the source data columns (no _rowaddr or __action columns)
    fn create_filtered_write_stream(
        &self,
        input_stream: SendableRecordBatchStream,
        merge_state: Arc<Mutex<MergeState>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let enable_stable_row_ids = {
            let state = merge_state.lock().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to lock merge state: {}",
                    e
                ))
            })?;
            state.stable_row_ids
        };

        if enable_stable_row_ids {
            self.create_ordered_update_insert_stream(input_stream, merge_state)
        } else {
            self.create_streaming_write_stream(input_stream, merge_state)
        }
    }

    /// High-performance streaming implementation for non-stable row ID scenarios
    ///
    /// It processes batches one at a time as they arrive from the input stream,
    /// immediately filtering and transforming each batch without buffering.
    ///
    /// For dedupe mode, accumulated UpdateAll rows are emitted at the end of the stream.
    fn create_streaming_write_stream(
        &self,
        input_stream: SendableRecordBatchStream,
        merge_state: Arc<Mutex<MergeState>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let (_, rowaddr_idx, rowid_idx, action_idx, data_column_indices, output_schema) =
            self.prepare_stream_schema(input_stream.schema())?;

        // Check if we're in dedupe mode
        let is_dedupe_mode = {
            let state = merge_state.lock().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to lock merge state: {}",
                    e
                ))
            })?;
            state.is_dedupe_mode()
        };

        let output_schema_clone = output_schema.clone();
        let data_column_indices_clone = data_column_indices;
        let merge_state_clone = merge_state.clone();

        let main_stream = input_stream.map(move |batch_result| -> DFResult<RecordBatch> {
            let batch = batch_result?;
            let (row_addr_array, row_id_array, action_array) =
                Self::extract_control_arrays(&batch, rowaddr_idx, rowid_idx, action_idx)?;

            // Process each row using the shared state
            let mut keep_rows: Vec<u32> = Vec::with_capacity(batch.num_rows());

            let mut merge_state = merge_state_clone.lock().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to lock merge state: {}",
                    e
                ))
            })?;

            // In dedupe mode, buffer the data columns from this batch
            let batch_idx = if merge_state.is_dedupe_mode() {
                Some(merge_state.buffer_data_batch(
                    &batch,
                    &data_column_indices_clone,
                    &output_schema_clone,
                )?)
            } else {
                None
            };

            for row_idx in 0..batch.num_rows() {
                let action_code = action_array.value(row_idx);
                let action = Action::try_from(action_code).map_err(|e| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "Invalid action code {}: {}",
                        action_code, e
                    ))
                })?;

                if merge_state
                    .process_row_action(
                        action,
                        row_idx,
                        row_addr_array,
                        row_id_array,
                        &batch,
                        batch_idx,
                    )?
                    .is_some()
                {
                    keep_rows.push(row_idx as u32);
                }
            }

            Self::create_filtered_batch(
                &batch,
                keep_rows,
                &data_column_indices_clone,
                output_schema_clone.clone(),
            )
        });

        if is_dedupe_mode {
            // In dedupe mode, chain a finalization stream to emit accumulated rows
            let output_schema_for_finalize = output_schema.clone();
            let finalize_stream = stream::once(async move {
                let mut state = merge_state.lock().map_err(|e| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "Failed to lock merge state for finalization: {}",
                        e
                    ))
                })?;
                state.finalize_dedupe(&output_schema_for_finalize)
            })
            .filter_map(|result| async {
                match result {
                    Ok(Some(batch)) => Some(Ok(batch)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                output_schema,
                main_stream.chain(finalize_stream),
            )))
        } else {
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                output_schema,
                main_stream,
            )))
        }
    }

    /// Creates an ordered update-insert stream ensuring updated data before inserted data.
    ///
    /// 1. Separating the input stream into update and insert streams
    /// 2. Using chain operations to guarantee all update batches are processed before any insert batches
    /// 3. Returning the combined ordered stream
    fn create_ordered_update_insert_stream(
        &self,
        input_stream: SendableRecordBatchStream,
        merge_state: Arc<Mutex<MergeState>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let (update_stream, insert_stream) =
            self.split_updates_and_inserts(input_stream, merge_state)?;

        let output_schema = update_stream.schema();

        // Chain the update and insert streams to ensure order
        let combined_stream = update_stream.chain(insert_stream);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            combined_stream,
        )))
    }

    /// Common schema preparation logic
    #[allow(clippy::type_complexity)]
    fn prepare_stream_schema(
        &self,
        input_schema: arrow_schema::SchemaRef,
    ) -> DFResult<(
        arrow_schema::SchemaRef,
        usize,
        usize,
        usize,
        Vec<usize>,
        Arc<Schema>,
    )> {
        // Find column indices
        let (rowaddr_idx, _) = input_schema.column_with_name(ROW_ADDR).ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "Expected _rowaddr column in merge insert input".to_string(),
            )
        })?;

        let (rowid_idx, _) = input_schema.column_with_name(ROW_ID).ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "Expected _rowid column in merge insert input".to_string(),
            )
        })?;

        let (action_idx, _) = input_schema
            .column_with_name(MERGE_ACTION_COLUMN)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Expected {} column in merge insert input",
                    MERGE_ACTION_COLUMN
                ))
            })?;

        // Check if we need to exclude the dedupe column from output
        // (when it exists in source but not in target)
        let exclude_dedupe_col = self.should_exclude_dedupe_column();

        // Find all data columns to write (everything except special columns)
        // The schema from DataFusion optimization may have collapsed duplicate columns
        // from the logical join, leaving us with the merged data columns plus special columns
        let total_fields = input_schema.fields().len();

        // Select all columns that are data columns (not _rowaddr or __action)
        // These represent the final merged data values to write
        let data_column_indices: Vec<usize> = (0..total_fields)
            .filter(|&idx| {
                let field = input_schema.field(idx);
                let name = field.name();
                // Skip special columns: _rowaddr, _rowid, __action
                if idx == rowaddr_idx
                    || idx == action_idx
                    || name == ROW_ADDR
                    || name == ROW_ID
                    || name == MERGE_ACTION_COLUMN
                {
                    return false;
                }
                // Skip dedupe column if it should be excluded (not in target schema)
                if let Some(ref dedupe_col) = exclude_dedupe_col {
                    // Check if this field is the dedupe column
                    // Field name is like "source.ts" or just "ts", dedupe_col is "ts"
                    let base_name = name.strip_prefix("source.").unwrap_or(name);
                    // Get top-level field name for nested paths
                    let dedupe_top_level = dedupe_col.split('.').next().unwrap_or(dedupe_col);
                    if base_name == dedupe_top_level {
                        return false;
                    }
                }
                true
            })
            .collect();

        if data_column_indices.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "No data columns found in merge insert input".to_string(),
            ));
        }

        // Create output schema with only data columns
        let output_fields: Vec<_> = data_column_indices
            .iter()
            .map(|&idx| {
                let field = input_schema.field(idx);
                Arc::new(arrow_schema::Field::new(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            })
            .collect();
        let output_schema = Arc::new(Schema::new(output_fields));

        Ok((
            input_schema,
            rowaddr_idx,
            rowid_idx,
            action_idx,
            data_column_indices,
            output_schema,
        ))
    }

    /// Check if the dedupe column should be excluded from output.
    /// Returns Some(dedupe_col_name) if it should be excluded, None otherwise.
    fn should_exclude_dedupe_column(&self) -> Option<String> {
        let dedupe_col = self.params.dedupe_by.as_ref()?;

        // Get the top-level field name from the dedupe column path
        let dedupe_top_level = dedupe_col.split('.').next().unwrap_or(dedupe_col);

        // Check if the dedupe column exists in target schema
        let target_schema = self.dataset.schema();
        if target_schema.field(dedupe_top_level).is_some() {
            // Dedupe column exists in target, don't exclude
            None
        } else {
            // Dedupe column doesn't exist in target, exclude it from output
            Some(dedupe_col.clone())
        }
    }

    /// Extract control arrays from batch
    fn extract_control_arrays(
        batch: &RecordBatch,
        rowaddr_idx: usize,
        rowid_idx: usize,
        action_idx: usize,
    ) -> DFResult<(&UInt64Array, &UInt64Array, &UInt8Array)> {
        // Get row address, row id and __action arrays
        let row_addr_array = batch
            .column(rowaddr_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Expected UInt64Array for _rowaddr column".to_string(),
                )
            })?;

        let row_id_array = batch
            .column(rowid_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "Expected UInt64Array for _rowid column".to_string(),
                )
            })?;

        let action_array = batch
            .column(action_idx)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Expected UInt8Array for {} column",
                    MERGE_ACTION_COLUMN
                ))
            })?;

        Ok((row_addr_array, row_id_array, action_array))
    }

    /// Create filtered batch from selected rows
    fn create_filtered_batch(
        batch: &RecordBatch,
        keep_rows: Vec<u32>,
        data_column_indices: &[usize],
        output_schema: Arc<Schema>,
    ) -> DFResult<RecordBatch> {
        // If no rows to keep, return empty batch
        if keep_rows.is_empty() {
            let empty_columns: Vec<_> = output_schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_empty_array(field.data_type()))
                .collect();
            return RecordBatch::try_new(output_schema, empty_columns)
                .map_err(datafusion::error::DataFusionError::from);
        }

        // Create indices for rows to keep
        let indices = arrow_array::UInt32Array::from(keep_rows);

        // Take only the rows we want to keep
        let filtered_batch = arrow_select::take::take_record_batch(batch, &indices)?;

        // Project only the data columns
        let output_columns: Vec<_> = data_column_indices
            .iter()
            .map(|&idx| filtered_batch.column(idx).clone())
            .collect();

        RecordBatch::try_new(output_schema, output_columns)
            .map_err(datafusion::error::DataFusionError::from)
    }

    /// Calculate write metrics from new fragments
    fn calculate_write_metrics(new_fragments: &[lance_table::format::Fragment]) -> (usize, usize) {
        let mut total_bytes = 0u64;
        let mut total_files = 0usize;

        for fragment in new_fragments {
            for data_file in &fragment.files {
                if let Some(size) = data_file.file_size_bytes.get() {
                    total_bytes += u64::from(size);
                }
                total_files += 1;
            }
        }

        (total_bytes as usize, total_files)
    }

    /// Delete a batch of rows by row address, returns the fragments modified and the fragments removed
    async fn apply_deletions(
        dataset: &Dataset,
        removed_row_addrs: &RoaringTreemap,
    ) -> Result<(Vec<Fragment>, Vec<u64>)> {
        let bitmaps = Arc::new(removed_row_addrs.bitmaps().collect::<BTreeMap<_, _>>());

        enum FragmentChange {
            Unchanged,
            Modified(Box<Fragment>),
            Removed(u64),
        }

        let mut updated_fragments = Vec::new();
        let mut removed_fragments = Vec::new();

        let mut stream = futures::stream::iter(dataset.get_fragments())
            .map(move |fragment| {
                let bitmaps_ref = bitmaps.clone();
                async move {
                    let fragment_id = fragment.id();
                    if let Some(bitmap) = bitmaps_ref.get(&(fragment_id as u32)) {
                        match fragment.extend_deletions(*bitmap).await {
                            Ok(Some(new_fragment)) => {
                                Ok(FragmentChange::Modified(Box::new(new_fragment.metadata)))
                            }
                            Ok(None) => Ok(FragmentChange::Removed(fragment_id as u64)),
                            Err(e) => Err(e),
                        }
                    } else {
                        Ok(FragmentChange::Unchanged)
                    }
                }
            })
            .buffer_unordered(dataset.object_store.io_parallelism());

        while let Some(res) = stream.next().await.transpose()? {
            match res {
                FragmentChange::Unchanged => {}
                FragmentChange::Modified(fragment) => updated_fragments.push(*fragment),
                FragmentChange::Removed(fragment_id) => removed_fragments.push(fragment_id),
            }
        }

        Ok((updated_fragments, removed_fragments))
    }

    fn split_updates_and_inserts(
        &self,
        input_stream: SendableRecordBatchStream,
        merge_state: Arc<Mutex<MergeState>>,
    ) -> DFResult<(SendableRecordBatchStream, SendableRecordBatchStream)> {
        let (_, rowaddr_idx, rowid_idx, action_idx, data_column_indices, output_schema) =
            self.prepare_stream_schema(input_stream.schema())?;

        let (update_tx, update_rx) = tokio::sync::mpsc::unbounded_channel();
        let (insert_tx, insert_rx) = tokio::sync::mpsc::unbounded_channel();

        let output_schema_clone = output_schema.clone();
        let merge_state_clone = merge_state;

        tokio::spawn(async move {
            let mut input_stream = input_stream;

            while let Some(batch_result) = input_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        match Self::process_and_split_batch(
                            &batch,
                            rowaddr_idx,
                            rowid_idx,
                            action_idx,
                            &data_column_indices,
                            output_schema_clone.clone(),
                            merge_state_clone.clone(),
                        ) {
                            Ok((update_batch_opt, insert_batch_opt)) => {
                                if let Some(update_batch) = update_batch_opt {
                                    if update_tx.send(Ok(update_batch)).is_err() {
                                        break;
                                    }
                                }

                                if let Some(insert_batch) = insert_batch_opt {
                                    if insert_tx.send(Ok(insert_batch)).is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                Self::handle_stream_processing_error(e, &update_tx, &insert_tx);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        Self::handle_stream_processing_error(e, &update_tx, &insert_tx);
                        break;
                    }
                }
            }
        });

        let update_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(update_rx);
        let update_stream = Box::pin(RecordBatchStreamAdapter::new(
            output_schema.clone(),
            update_stream,
        ));

        let insert_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(insert_rx);
        let insert_stream = Box::pin(RecordBatchStreamAdapter::new(output_schema, insert_stream));

        Ok((update_stream, insert_stream))
    }

    fn process_and_split_batch(
        batch: &RecordBatch,
        rowaddr_idx: usize,
        rowid_idx: usize,
        action_idx: usize,
        data_column_indices: &[usize],
        output_schema: Arc<Schema>,
        merge_state: Arc<Mutex<MergeState>>,
    ) -> DFResult<(Option<RecordBatch>, Option<RecordBatch>)> {
        let (row_addr_array, row_id_array, action_array) =
            Self::extract_control_arrays(batch, rowaddr_idx, rowid_idx, action_idx)?;

        let mut update_indices: Vec<u32> = Vec::new();
        let mut insert_indices: Vec<u32> = Vec::new();

        {
            let mut merge_state = merge_state.lock().map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to lock merge state: {}",
                    e
                ))
            })?;

            // In dedupe mode, buffer the data columns from this batch
            let batch_idx = if merge_state.is_dedupe_mode() {
                Some(merge_state.buffer_data_batch(batch, data_column_indices, &output_schema)?)
            } else {
                None
            };

            for row_idx in 0..batch.num_rows() {
                let action_code = action_array.value(row_idx);
                let action = Action::try_from(action_code).map_err(|e| {
                    datafusion::error::DataFusionError::Internal(format!(
                        "Invalid action code {}: {}",
                        action_code, e
                    ))
                })?;

                if merge_state
                    .process_row_action(
                        action,
                        row_idx,
                        row_addr_array,
                        row_id_array,
                        batch,
                        batch_idx,
                    )?
                    .is_some()
                {
                    match action {
                        Action::UpdateAll => update_indices.push(row_idx as u32),
                        Action::Insert => insert_indices.push(row_idx as u32),
                        _ => {}
                    }
                }
            }
        }

        let update_batch = if !update_indices.is_empty() {
            Some(Self::create_filtered_batch(
                batch,
                update_indices,
                data_column_indices,
                output_schema.clone(),
            )?)
        } else {
            None
        };

        let insert_batch = if !insert_indices.is_empty() {
            Some(Self::create_filtered_batch(
                batch,
                insert_indices,
                data_column_indices,
                output_schema,
            )?)
        } else {
            None
        };

        Ok((update_batch, insert_batch))
    }

    fn handle_stream_processing_error(
        error: datafusion::error::DataFusionError,
        update_tx: &tokio::sync::mpsc::UnboundedSender<DFResult<RecordBatch>>,
        insert_tx: &tokio::sync::mpsc::UnboundedSender<DFResult<RecordBatch>>,
    ) {
        let error_msg = format!("Stream processing failed: {}", error);

        let update_error = datafusion::error::DataFusionError::Internal(error_msg.clone());
        let insert_error = datafusion::error::DataFusionError::Internal(error_msg);

        let _ = update_tx.send(Err(update_error));
        let _ = insert_tx.send(Err(insert_error));
    }
}

impl DisplayAs for FullSchemaMergeInsertExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default
            | datafusion::physical_plan::DisplayFormatType::Verbose => {
                let on_keys = self.params.on.join(", ");
                let when_matched = match &self.params.when_matched {
                    crate::dataset::WhenMatched::DoNothing => "DoNothing".to_string(),
                    crate::dataset::WhenMatched::UpdateAll => "UpdateAll".to_string(),
                    crate::dataset::WhenMatched::UpdateIf(condition) => {
                        format!("UpdateIf({})", condition)
                    }
                    crate::dataset::WhenMatched::Fail => "Fail".to_string(),
                };
                let when_not_matched = if self.params.insert_not_matched {
                    "InsertAll"
                } else {
                    "DoNothing"
                };
                let when_not_matched_by_source = match &self.params.delete_not_matched_by_source {
                    crate::dataset::WhenNotMatchedBySource::Keep => "Keep",
                    crate::dataset::WhenNotMatchedBySource::Delete => "Delete",
                    crate::dataset::WhenNotMatchedBySource::DeleteIf(_) => "DeleteIf",
                };

                write!(
                    f,
                    "MergeInsert: on=[{}], when_matched={}, when_not_matched={}, when_not_matched_by_source={}",
                    on_keys,
                    when_matched,
                    when_not_matched,
                    when_not_matched_by_source
                )
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                write!(f, "MergeInsert[{}]", self.dataset.uri())
            }
        }
    }
}

impl ExecutionPlan for FullSchemaMergeInsertExec {
    fn name(&self) -> &str {
        "FullSchemaMergeInsertExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::new(arrow_schema::Schema::empty())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "FullSchemaMergeInsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            input: children[0].clone(),
            dataset: self.dataset.clone(),
            params: self.params.clone(),
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
            merge_stats: self.merge_stats.clone(),
            transaction: self.transaction.clone(),
            affected_rows: self.affected_rows.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        // We require a single partition for the merge operation to ensure all data is processed
        vec![datafusion_physical_expr::Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // We just want one stream.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let _baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        // Input schema structure based on our logical plan:
        // - target._rowaddr: Address of existing rows to update/delete
        // - source.*: Source data columns (variable schema)
        // - __action: Merge action (1=update, 2=insert, 0=delete, etc.)

        // Execute the input plan to get the merge data stream
        let input_stream = self.input.execute(partition, context)?;

        // Step 1: Create shared state and streaming processor for row addresses and write data
        let merge_state = Arc::new(Mutex::new(MergeState::new(
            MergeInsertMetrics::new(&self.metrics, partition),
            self.dataset.manifest.uses_stable_row_ids(),
            self.params.on.clone(),
            self.params.dedupe_by.clone(),
            self.params.dedupe_ordering,
        )));
        let write_data_stream =
            self.create_filtered_write_stream(input_stream, merge_state.clone())?;

        // Use flat_map to handle the async write operation
        let dataset = self.dataset.clone();
        let merge_stats_holder = self.merge_stats.clone();
        let transaction_holder = self.transaction.clone();
        let affected_rows_holder = self.affected_rows.clone();
        let mem_wal_to_merge = self.params.mem_wal_to_merge.clone();
        let updating_row_ids = {
            let state = merge_state.lock().unwrap();
            state.updating_row_ids.clone()
        };

        let result_stream = stream::once(async move {
            // Step 2: Write new fragments using the filtered data (inserts + updates)
            let (mut new_fragments, _) = write_fragments_internal(
                Some(&dataset),
                dataset.object_store.clone(),
                &dataset.base,
                dataset.schema().clone(),
                write_data_stream,
                WriteParams::default(),
                None, // Merge insert doesn't use target_bases
            )
            .await?;

            if let Some(row_id_sequence) = updating_row_ids.lock().unwrap().row_id_sequence() {
                let fragment_sizes = new_fragments
                    .iter()
                    .map(|f| f.physical_rows.unwrap() as u64);

                let sequences = lance_table::rowids::rechunk_sequences(
                    [row_id_sequence.clone()],
                    fragment_sizes,
                    true,
                )
                .map_err(|e| Error::Internal {
                    message: format!(
                        "Captured row ids not equal to number of rows written: {}",
                        e
                    ),
                    location: location!(),
                })?;

                for (fragment, sequence) in new_fragments.iter_mut().zip(sequences) {
                    let serialized = lance_table::rowids::write_row_ids(&sequence);
                    fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
                }
            }

            // Step 2.5: Calculate write metrics from new fragments
            let (total_bytes_written, total_files_written) =
                Self::calculate_write_metrics(&new_fragments);

            // Step 3: Apply deletions to existing fragments
            let merge_state =
                Arc::into_inner(merge_state).expect("MergeState should only have 1 reference now");
            let merge_state =
                Mutex::into_inner(merge_state).expect("MergeState lock should be available");
            let delete_row_addrs_clone = merge_state.delete_row_addrs;

            let (updated_fragments, removed_fragment_ids) =
                Self::apply_deletions(&dataset, &delete_row_addrs_clone).await?;

            // Step 4: Create the transaction operation
            let operation = Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified: vec![], // No fields are modified in schema for upsert
                mem_wal_to_merge,
                fields_for_preserving_frag_bitmap: dataset
                    .schema()
                    .fields
                    .iter()
                    .map(|f| f.id as u32)
                    .collect(),
                update_mode: Some(RewriteRows),
            };

            // Step 5: Create and store the transaction
            let transaction = Transaction::new(dataset.manifest.version, operation, None);

            // Step 6: Store transaction, merge stats, and affected rows for later retrieval
            {
                // Update write metrics before converting to stats
                merge_state.metrics.bytes_written.add(total_bytes_written);
                merge_state
                    .metrics
                    .num_files_written
                    .add(total_files_written);

                // Get the final stats from the shared state
                let stats = MergeStats::from(&merge_state.metrics);

                if let Ok(mut transaction_guard) = transaction_holder.lock() {
                    transaction_guard.replace(transaction);
                }
                if let Ok(mut merge_stats_guard) = merge_stats_holder.lock() {
                    merge_stats_guard.replace(stats);
                }
                if let Ok(mut affected_rows_guard) = affected_rows_holder.lock() {
                    affected_rows_guard.replace(delete_row_addrs_clone);
                }
            };

            // Step 7: Return empty result (write operations don't return data)
            let empty_schema = Arc::new(arrow_schema::Schema::empty());
            let empty_batch = RecordBatch::new_empty(empty_schema);
            Ok(empty_batch)
        });

        let empty_schema = Arc::new(arrow_schema::Schema::empty());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            empty_schema,
            result_stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::UInt64Array;

    #[test]
    fn test_merge_state_duplicate_rowid_detection() {
        let metrics = MergeInsertMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut merge_state = MergeState::new(
            metrics,
            false,
            Vec::new(),
            None,                      // no dedupe_by
            DedupeOrdering::default(), // default ordering
        );

        let row_addr_array = UInt64Array::from(vec![1000, 2000, 3000]);
        let row_id_array = UInt64Array::from(vec![100, 100, 300]); // Duplicate row_id 100

        let result1 = merge_state.process_row_action(
            Action::UpdateAll,
            0,
            &row_addr_array,
            &row_id_array,
            &RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
            None, // no batch_idx for non-dedupe mode
        );
        assert!(result1.is_ok(), "First call should succeed");

        let result2 = merge_state.process_row_action(
            Action::UpdateAll,
            1,
            &row_addr_array,
            &row_id_array,
            &RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
            None,
        );
        assert!(
            result2.is_err(),
            "Second call with duplicate _rowid should fail"
        );

        let error_msg = result2.unwrap_err().to_string();
        assert!(
            error_msg.contains("Ambiguous merge insert")
                && error_msg.contains("multiple source rows"),
            "Error message should mention ambiguous merge insert and multiple source rows, got: {}",
            error_msg
        );

        let result3 = merge_state.process_row_action(
            Action::UpdateAll,
            2,
            &row_addr_array,
            &row_id_array,
            &RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
            None,
        );
        assert!(
            result3.is_ok(),
            "Third call with different _rowid should succeed"
        );
    }
}
