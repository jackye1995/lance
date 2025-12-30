// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Index-based merge join execution node.
//!
//! This node uses a btree index to efficiently perform merge-insert lookups.
//! It has two modes of operation:
//!
//! ## Mode 1: Lookup Only (join_type = None)
//!
//! Used for hybrid execution where the join happens externally.
//! Input: key projection (just the key column from source)
//! Output: matching target rows [projection_columns, _rowid, _rowaddr]
//!
//! ## Mode 2: Lookup with Join (join_type = Some)
//!
//! Used for fully indexed execution where the join is internal.
//! Input: full source data
//! Output: joined rows [source_cols, target_cols with _rowid, _rowaddr]
//!
//! The internal DAG structure (ReplayExec used twice) is hidden from DataFusion
//! by only exposing the original input as a child. This prevents DataFusion's
//! tree transformations from breaking the shared ReplayExec references.

use std::any::Any;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::NullEquality;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::EquivalenceProperties;
use lance_core::utils::futures::Capacity;
use lance_core::{ROW_ADDR_FIELD, ROW_ID_FIELD};
use lance_table::format::IndexMetadata;

use crate::io::exec::scalar_index::MapIndexExec;
use crate::io::exec::utils::ReplayExec;
use crate::io::exec::{project, AddRowAddrExec, TakeExec};
use crate::Dataset;

/// Index-based lookup execution node.
///
/// This node uses a btree index to efficiently lookup target rows.
///
/// ## Modes
///
/// - **Lookup only** (`join_type = None`): Takes key projection as input,
///   outputs matching target rows.
/// - **Lookup with join** (`join_type = Some`): Takes source as input,
///   outputs joined rows with source and target columns.
///
/// ## Output Schema
///
/// Lookup only mode:
/// - `projection_columns` from target
/// - `_rowid`
/// - `_rowaddr` (if with_row_addr)
///
/// Join mode:
/// - Source columns
/// - Target columns (including `_rowid`, `_rowaddr`)
#[derive(Debug)]
pub struct IndexedLookupExec {
    /// The inner execution plan
    inner: Arc<dyn ExecutionPlan>,
    /// The original input (exposed as child for proper plan display)
    input: Arc<dyn ExecutionPlan>,
    /// The dataset (needed for rebuilding)
    dataset: Arc<Dataset>,
    /// The index metadata
    index: IndexMetadata,
    /// Columns to project from target
    projection_columns: Vec<String>,
    /// Whether to include row address
    with_row_addr: bool,
    /// Join type - None for lookup only, Some for lookup with join
    join_type: Option<datafusion::logical_expr::JoinType>,
    /// Key column name
    key_column: String,
    /// Index name
    index_name: String,
    /// Plan properties
    properties: PlanProperties,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl IndexedLookupExec {
    /// Create a new IndexedLookupExec for lookup only (no join).
    ///
    /// # Arguments
    /// * `key_projection` - The input containing just the key column
    /// * `dataset` - The target dataset
    /// * `index` - The scalar index on the key column
    /// * `key_column` - The key column name
    /// * `projection_columns` - Columns to fetch from target (must include key_column)
    /// * `with_row_addr` - Whether to include row address in output
    pub fn try_new(
        key_projection: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        index: IndexMetadata,
        key_column: String,
        projection_columns: Vec<String>,
        with_row_addr: bool,
    ) -> DFResult<Self> {
        Self::try_new_internal(
            key_projection,
            dataset,
            index,
            key_column,
            projection_columns,
            with_row_addr,
            None, // No join
        )
    }

    /// Create a new IndexedLookupExec with internal join.
    ///
    /// This mode wraps the source in ReplayExec internally and performs
    /// the join as part of the execution.
    ///
    /// # Arguments
    /// * `source` - The full source execution plan
    /// * `dataset` - The target dataset
    /// * `index` - The scalar index on the key column
    /// * `key_column` - The key column name
    /// * `projection_columns` - Columns to fetch from target (must include key_column)
    /// * `with_row_addr` - Whether to include row address in output
    /// * `join_type` - Left for upsert, Inner for update-only
    pub fn try_new_with_join(
        source: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        index: IndexMetadata,
        key_column: String,
        projection_columns: Vec<String>,
        with_row_addr: bool,
        join_type: datafusion::logical_expr::JoinType,
    ) -> DFResult<Self> {
        Self::try_new_internal(
            source,
            dataset,
            index,
            key_column,
            projection_columns,
            with_row_addr,
            Some(join_type),
        )
    }

    fn try_new_internal(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        index: IndexMetadata,
        key_column: String,
        projection_columns: Vec<String>,
        with_row_addr: bool,
        join_type: Option<datafusion::logical_expr::JoinType>,
    ) -> DFResult<Self> {
        // Validate that key_column is in projection_columns
        if !projection_columns.contains(&key_column) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Key column '{}' must be included in projection_columns",
                key_column
            )));
        }

        let index_name = index.name.clone();

        // Build the inner pipeline
        // For join mode, we wrap the source in ReplayExec FIRST, then pass the ReplayExec
        // to build_join_pipeline. The ReplayExec becomes our exposed child.
        let (inner, exposed_input) = if let Some(ref jt) = join_type {
            // Join mode: wrap source in ReplayExec FIRST
            // BUT if input is already a ReplayExec, reuse it (happens in with_new_children)
            let source_replay: Arc<dyn ExecutionPlan> =
                if input.as_any().downcast_ref::<ReplayExec>().is_some() {
                    input // Already a ReplayExec, use as-is
                } else {
                    Arc::new(ReplayExec::new(Capacity::Unbounded, input))
                };
            let pipeline = Self::build_join_pipeline_with_replay(
                source_replay.clone(),
                dataset.clone(),
                index.clone(),
                &key_column,
                &projection_columns,
                with_row_addr,
                jt,
            )?;
            (pipeline, source_replay)
        } else {
            // Lookup only mode: just the lookup pipeline
            let pipeline = Self::build_lookup_pipeline(
                input.clone(),
                dataset.clone(),
                index.clone(),
                &key_column,
                &projection_columns,
                with_row_addr,
            )?;
            (pipeline, input)
        };

        let output_schema = inner.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Ok(Self {
            inner,
            input: exposed_input,
            dataset,
            index,
            projection_columns,
            with_row_addr,
            join_type,
            key_column,
            index_name,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Build the lookup-only pipeline (no join).
    ///
    /// Pipeline: input -> MapIndexExec -> AddRowAddrExec -> TakeExec -> project
    fn build_lookup_pipeline(
        key_projection: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        index: IndexMetadata,
        key_column: &str,
        projection_columns: &[String],
        with_row_addr: bool,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Step 1: MapIndexExec to get row IDs from the index
        let mut index_mapper: Arc<dyn ExecutionPlan> = Arc::new(MapIndexExec::new(
            dataset.clone(),
            key_column.to_string(),
            index.name,
            key_projection,
        ));

        // Step 2: Add row address if requested
        if with_row_addr {
            let pos = index_mapper.schema().fields().len();
            index_mapper = Arc::new(AddRowAddrExec::try_new(index_mapper, dataset.clone(), pos)?);
        }

        // Step 3: TakeExec to fetch target data for matched rows
        let projection_schema = dataset
            .schema()
            .project(
                &projection_columns
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let projection = dataset.empty_projection().union_schema(&projection_schema);

        let take_result = TakeExec::try_new(dataset.clone(), index_mapper, projection)?;
        let target_exec: Arc<dyn ExecutionPlan> = match take_result {
            Some(take) => Arc::new(take),
            None => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "TakeExec returned None - no columns to take".to_string(),
                ));
            }
        };

        // Step 4: Project to output schema order [projection_columns, _rowid, _rowaddr]
        let target_schema: Schema = dataset.schema().into();
        let mut target_columns: Vec<Arc<Field>> = Vec::new();
        for col_name in projection_columns {
            let field = target_schema.field_with_name(col_name).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Projection column '{}' not found in target schema",
                    col_name
                ))
            })?;
            target_columns.push(Arc::new(field.clone()));
        }
        target_columns.push(Arc::new(ROW_ID_FIELD.clone()));
        if with_row_addr {
            target_columns.push(Arc::new(ROW_ADDR_FIELD.clone()));
        }

        Ok(Arc::new(project(
            target_exec,
            &Schema::new(target_columns),
        )?))
    }

    /// Build the complete join pipeline with an already-created ReplayExec.
    ///
    /// Pipeline structure:
    /// ```text
    /// HashJoinExec (Left or Inner join, CollectLeft mode)
    ///   source (ReplayExec) <- LEFT, collected into hash table
    ///   target (lookup result) <- RIGHT, streams and probes
    /// ```
    ///
    /// The source_replay must be a ReplayExec created externally. This ensures
    /// the same Arc is used for both the exposed child and internal DAG.
    fn build_join_pipeline_with_replay(
        source_replay: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        index: IndexMetadata,
        key_column: &str,
        projection_columns: &[String],
        with_row_addr: bool,
        join_type: &datafusion::logical_expr::JoinType,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Step 1: Project source to key column for index lookup
        let key_expr = vec![(
            Arc::new(Column::new_with_schema(
                key_column,
                &source_replay.schema(),
            )?) as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
            key_column.to_string(),
        )];
        let key_projection: Arc<dyn ExecutionPlan> =
            Arc::new(ProjectionExec::try_new(key_expr, source_replay.clone())?);

        // Step 2-5: Build the lookup pipeline
        let target = Self::build_lookup_pipeline(
            key_projection,
            dataset,
            index,
            key_column,
            projection_columns,
            with_row_addr,
        )?;

        // Step 6: HashJoinExec to join source with target (source on LEFT, target on RIGHT)
        let source_key_idx = source_replay.schema().index_of(key_column)?;
        let target_key_idx = target.schema().index_of(key_column)?;
        let join_on = vec![(
            Arc::new(Column::new(key_column, source_key_idx))
                as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
            Arc::new(Column::new(key_column, target_key_idx))
                as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        )];

        let joined: Arc<dyn ExecutionPlan> = Arc::new(HashJoinExec::try_new(
            source_replay,
            target,
            join_on,
            None,
            join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNull,
        )?);

        Ok(joined)
    }
}

impl DisplayAs for IndexedLookupExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default
            | datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IndexedLookup: key={}, index={}",
                    self.key_column, self.index_name
                )
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "IndexedLookup\nkey={}\nindex={}",
                    self.key_column, self.index_name
                )
            }
        }
    }
}

impl ExecutionPlan for IndexedLookupExec {
    fn name(&self) -> &str {
        "IndexedLookupExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Always expose the input as child for proper plan display.
        // For join mode, input is the ReplayExec; for lookup mode, it's the key projection.
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "IndexedLookupExec expects exactly one child".to_string(),
            ));
        }

        // If the child is the same (by Arc pointer), return self unchanged.
        // This is the common case when DataFusion walks the plan tree without changes.
        if Arc::ptr_eq(&children[0], &self.input) {
            return Ok(self);
        }

        // For join mode with a different child, we need to rebuild the entire
        // pipeline with the new source wrapped in ReplayExec.
        // For lookup mode, rebuild with the new key projection.
        Ok(Arc::new(Self::try_new_internal(
            children[0].clone(),
            self.dataset.clone(),
            self.index.clone(),
            self.key_column.clone(),
            self.projection_columns.clone(),
            self.with_row_addr,
            self.join_type,
        )?))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Delegate to the inner pipeline
        self.inner.execute(partition, context)
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Must match children() length - always one child
        vec![false]
    }
}
