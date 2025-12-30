// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Logical plan nodes and planners for merge-insert operations.
//!
//! This module provides:
//! - [`MergeInsertWriteNode`]: A logical node representing the merge-insert write operation
//! - [`MergeInsertPlanner`]: Converts MergeInsertWriteNode to FullSchemaMergeInsertExec,
//!   with automatic optimization to use indexed joins when available
//!
//! # Indexed Join Optimization
//!
//! When a scalar btree index exists on the join key column, the planner replaces
//! the standard `HashJoinExec` with an `IndexedLookupExec` + `HashJoinExec` pattern
//! for more efficient lookups. This is particularly beneficial when:
//! - The target table is large
//! - The source batch is relatively small
//! - There's a btree index on the join key
//!
//! The `IndexedLookupExec` strategy:
//! 1. Collect source keys into batches
//! 2. Batch lookup via `MapIndexExec` + `TakeExec`
//! 3. Output matching target rows
//!
//! The final join is performed by `HashJoinExec`, which uses Arrow-native
//! vectorized hashing for optimal performance.
//!
//! # Hybrid Execution Strategies
//!
//! When an index only covers some fragments (partial coverage), the planner creates
//! a hybrid plan that combines indexed and non-indexed paths:
//!
//! ## Left Join Hybrid (insert_not_matched = true)
//!
//! Used when unmatched source rows need to be inserted. Strategy:
//! 1. **Indexed path**: Use `IndexedLookupExec` to get matching target rows from indexed fragments
//! 2. **Scan path**: Use `LanceScanExec` to scan unindexed fragments
//! 3. **Union**: Combine both target row streams
//! 4. **Final join**: Left outer join with source using `HashJoinExec`
//!
//! ```text
//!                    HashJoinExec (Left, CollectLeft)
//!                     /                    \
//!       Source (ReplayExec)         RepartitionExec
//!                                          |
//!                                      UnionExec
//!                                      /        \
//!                           IndexedLookup    LanceScanExec
//!                          (indexed frags)   (unindexed frags)
//! ```
//!
//! ## Inner Join Hybrid (insert_not_matched = false)
//!
//! Used for update-only operations. Strategy:
//! 1. **Indexed path**: Use `IndexedLookupExec` + `HashJoinExec` (Inner)
//! 2. **Scan path**: `HashJoinExec` (Inner) with scan of unindexed fragments
//! 3. **Union**: Combine both joined result streams
//!
//! ```text
//!                   RepartitionExec
//!                         |
//!                    UnionExec
//!                   /          \
//!     HashJoinExec(Inner)     HashJoinExec(Inner)
//!       /         \              /         \
//!    Source   IndexedLookup   Source    LanceScan
//! (ReplayExec) (indexed)    (ReplayExec) (unindexed)
//! ```
//!
//! The [`ReplayExec`] wrapper on source allows the source stream to be consumed
//! multiple times (once per join path).

use std::cmp::Ordering;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::NullEquality;
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::expressions::Column;
use lance_core::utils::futures::Capacity;
use lance_core::{ROW_ADDR, ROW_ID};
use lance_index::{DatasetIndexExt, IndexCriteria};
use lance_table::format::{Fragment, IndexMetadata};

use super::exec::{FullSchemaMergeInsertExec, IndexedLookupExec};
use super::{MergeInsertParams, MERGE_ACTION_COLUMN};
use crate::io::exec::utils::ReplayExec;
use crate::io::exec::{LanceScanConfig, LanceScanExec};
use crate::Dataset;

/// Metadata about index availability for a Lance join operation.
#[derive(Debug, Clone)]
pub struct LanceJoinIndexInfo {
    /// The scalar index on the join key, if available
    pub index: Option<IndexMetadata>,
    /// Fragments covered by the index
    pub indexed_fragments: Vec<Fragment>,
    /// Fragments not covered by the index
    pub unindexed_fragments: Vec<Fragment>,
}

impl LanceJoinIndexInfo {
    /// Create info with index availability.
    pub fn with_index(
        index: IndexMetadata,
        indexed_fragments: Vec<Fragment>,
        unindexed_fragments: Vec<Fragment>,
    ) -> Self {
        Self {
            index: Some(index),
            indexed_fragments,
            unindexed_fragments,
        }
    }

    /// Check if an index is available.
    pub fn has_index(&self) -> bool {
        self.index.is_some() && !self.indexed_fragments.is_empty()
    }
}

/// Logical plan node for merge insert write.
///
/// Expects input schema (from join output):
/// - Target columns (nullable for unmatched source rows)
/// - `_rowid` (nullable)
/// - `_rowaddr` (nullable, if requested)
/// - Source columns
/// - `__action` column
///
/// Output is empty.
#[derive(Debug)]
pub struct MergeInsertWriteNode {
    input: LogicalPlan,
    pub(crate) dataset: Arc<Dataset>,
    pub(crate) params: MergeInsertParams,
    schema: Arc<DFSchema>,
}

impl PartialEq for MergeInsertWriteNode {
    fn eq(&self, other: &Self) -> bool {
        self.params == other.params
            && self.input == other.input
            && self.dataset.base == other.dataset.base
    }
}

impl Eq for MergeInsertWriteNode {}

impl std::hash::Hash for MergeInsertWriteNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.params.hash(state);
        self.input.hash(state);
        self.dataset.base.hash(state);
    }
}

impl PartialOrd for MergeInsertWriteNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.params.partial_cmp(&other.params) {
            Some(Ordering::Equal) => self.input.partial_cmp(&other.input),
            cmp => cmp,
        }
    }
}

impl MergeInsertWriteNode {
    pub fn new(input: LogicalPlan, dataset: Arc<Dataset>, params: MergeInsertParams) -> Self {
        let empty_schema = Arc::new(arrow_schema::Schema::empty());
        let schema = Arc::new(DFSchema::try_from(empty_schema).unwrap());
        Self {
            input,
            dataset,
            params,
            schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for MergeInsertWriteNode {
    fn name(&self) -> &str {
        "MergeInsertWrite"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &Arc<DFSchema> {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let on_keys = self.params.on.join(", ");
        let when_matched = match &self.params.when_matched {
            crate::dataset::WhenMatched::DoNothing => "DoNothing",
            crate::dataset::WhenMatched::UpdateAll => "UpdateAll",
            crate::dataset::WhenMatched::UpdateIf(_) => "UpdateIf",
            crate::dataset::WhenMatched::Fail => "Fail",
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
            on_keys, when_matched, when_not_matched, when_not_matched_by_source
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::error::Result<Self> {
        if !exprs.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "MergeInsertWriteNode does not accept expressions".to_string(),
            ));
        }
        if inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "MergeInsertWriteNode requires exactly one input".to_string(),
            ));
        }
        let empty_schema = Arc::new(arrow_schema::Schema::empty());
        let schema = Arc::new(DFSchema::try_from(empty_schema).unwrap());
        Ok(Self {
            input: inputs[0].clone(),
            dataset: self.dataset.clone(),
            params: self.params.clone(),
            schema,
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Need all source columns, _rowaddr, _rowid, and __action
        let input_schema = self.input.schema();
        let mut necessary_columns = Vec::new();

        for (i, (qualifier, field)) in input_schema.iter().enumerate() {
            let should_include = match qualifier {
                // Include all source columns
                Some(qualifier) if qualifier.table() == "source" => true,
                // Include target._rowaddr
                Some(qualifier) if qualifier.table() == "target" && field.name() == ROW_ADDR => {
                    true
                }
                // Include target._rowid
                Some(qualifier) if qualifier.table() == "target" && field.name() == ROW_ID => true,
                // Include __action
                None if field.name() == MERGE_ACTION_COLUMN => true,
                _ => false,
            };

            if should_include {
                necessary_columns.push(i);
            }
        }

        Some(vec![necessary_columns])
    }
}

/// Physical planner for MergeInsertWriteNode.
///
/// This planner handles indexed join transformation:
/// 1. Computes index availability from the dataset
/// 2. Transforms the child join plan to use indexed execution if beneficial
/// 3. Wraps the result in FullSchemaMergeInsertExec
pub struct MergeInsertPlanner;

impl MergeInsertPlanner {
    /// Compute index info for the join key column.
    ///
    /// Returns `Some(LanceJoinIndexInfo)` if a scalar index exists on the join key,
    /// `None` otherwise.
    async fn compute_index_info(
        dataset: &Dataset,
        params: &MergeInsertParams,
    ) -> DFResult<Option<LanceJoinIndexInfo>> {
        // Only single-column key is supported for indexed joins
        if params.on.len() != 1 {
            return Ok(None);
        }

        // Check if index usage is enabled and if we can use it
        // (can't use index when deleting unmatched source rows)
        let can_use_index = params.use_index
            && matches!(
                params.delete_not_matched_by_source,
                super::WhenNotMatchedBySource::Keep
            );
        if !can_use_index {
            return Ok(None);
        }

        let key_column = &params.on[0];

        // Try to load scalar index on the join key
        let index = dataset
            .load_scalar_index(
                IndexCriteria::default()
                    .for_column(key_column)
                    .supports_exact_equality(),
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let Some(index) = index else {
            return Ok(None);
        };

        // Get indexed fragment IDs (bitmap contains u32 values)
        let indexed_fragment_ids: std::collections::HashSet<u32> = index
            .fragment_bitmap
            .as_ref()
            .map_or_else(std::collections::HashSet::new, |bitmap| {
                bitmap.iter().collect()
            });

        // Partition fragments
        let all_fragments = dataset.fragments().to_vec();
        let (indexed_fragments, unindexed_fragments): (Vec<Fragment>, Vec<Fragment>) =
            all_fragments
                .into_iter()
                .partition(|f| indexed_fragment_ids.contains(&(f.id as u32)));

        Ok(Some(LanceJoinIndexInfo::with_index(
            index,
            indexed_fragments,
            unindexed_fragments,
        )))
    }
}

#[async_trait]
impl ExtensionPlanner for MergeInsertPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(write_node) = node.as_any().downcast_ref::<MergeInsertWriteNode>() else {
            return Ok(None);
        };

        assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
        assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");

        // Get the physical input plan (the join)
        let input_plan = physical_inputs[0].clone();

        // Compute index info from the dataset
        let index_info = Self::compute_index_info(&write_node.dataset, &write_node.params).await?;

        // Transform the join plan to use indexed execution if beneficial
        let transformed_plan = if let Some(ref index_info) = index_info {
            transform_join_to_indexed(
                input_plan,
                &write_node.dataset,
                index_info,
                &write_node.params,
            )?
        } else {
            // No index available - use the plan as-is (standard HashJoinExec)
            input_plan
        };

        let exec = FullSchemaMergeInsertExec::try_new(
            transformed_plan,
            write_node.dataset.clone(),
            write_node.params.clone(),
        )?;

        Ok(Some(Arc::new(exec)))
    }
}

/// Transform a join physical plan to use indexed execution if possible.
///
/// This function finds the HashJoinExec in the plan and replaces it with
/// IndexedLookupExec + HashJoinExec when an index is available.
///
/// IMPORTANT: This function manually walks the tree to avoid DataFusion's
/// transform_down, which calls with_new_children on all nodes and breaks
/// shared Arc references (like ReplayExec).
fn transform_join_to_indexed(
    plan: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index_info: &LanceJoinIndexInfo,
    params: &MergeInsertParams,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // If no index is available, return the plan as-is
    if !index_info.has_index() {
        return Ok(plan);
    }

    let index = index_info.index.as_ref().unwrap();
    let has_indexed = !index_info.indexed_fragments.is_empty();
    let has_unindexed = !index_info.unindexed_fragments.is_empty();

    // Only the single-column key case is supported for now
    if params.on.len() != 1 {
        return Ok(plan);
    }
    let key_column = &params.on[0];

    // Manually walk the tree to find and replace HashJoinExec
    // This avoids the issues with transform_down breaking shared Arcs
    replace_hash_join_with_indexed(
        plan,
        dataset,
        index,
        key_column,
        has_indexed,
        has_unindexed,
        index_info,
        params.insert_not_matched,
    )
}

/// Recursively find and replace HashJoinExec with indexed version.
/// Returns the same plan if no replacement needed, or a new plan with the replacement.
#[allow(clippy::too_many_arguments)]
fn replace_hash_join_with_indexed(
    plan: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    has_indexed: bool,
    has_unindexed: bool,
    index_info: &LanceJoinIndexInfo,
    insert_not_matched: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Check if this is the HashJoinExec we want to replace
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let left = hash_join.left();

        // Skip HashJoinExecs that we've already created (have ReplayExec as left child)
        if left.as_any().downcast_ref::<ReplayExec>().is_some() {
            return Ok(plan);
        }

        // Skip other patterns that indicate this is our created HashJoinExec
        let is_right_hybrid = left.as_any().downcast_ref::<RepartitionExec>().is_some();
        let is_inner_hybrid = left.as_any().downcast_ref::<LanceScanExec>().is_some();
        let is_indexed = left.as_any().downcast_ref::<IndexedLookupExec>().is_some()
            || hash_join
                .right()
                .as_any()
                .downcast_ref::<IndexedLookupExec>()
                .is_some();

        if is_right_hybrid || is_inner_hybrid || is_indexed {
            return Ok(plan);
        }

        // This is the original HashJoinExec - replace it with indexed version
        return create_indexed_join_plan(
            hash_join,
            dataset,
            index,
            key_column,
            has_indexed,
            has_unindexed,
            index_info,
            insert_not_matched,
        );
    }

    // Not a HashJoinExec - check if we need to recurse into children
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }

    // Check if any child needs replacement
    let mut new_children = Vec::with_capacity(children.len());
    let mut any_changed = false;

    for child in children {
        let new_child = replace_hash_join_with_indexed(
            child.clone(),
            dataset,
            index,
            key_column,
            has_indexed,
            has_unindexed,
            index_info,
            insert_not_matched,
        )?;

        // Check if this child was replaced (different Arc)
        if !Arc::ptr_eq(&new_child, child) {
            any_changed = true;
        }
        new_children.push(new_child);
    }

    if any_changed {
        // Create new plan with replaced children
        plan.with_new_children(new_children)
    } else {
        // No changes needed
        Ok(plan)
    }
}

/// Create the indexed join plan to replace the original HashJoinExec.
#[allow(clippy::too_many_arguments)]
fn create_indexed_join_plan(
    hash_join: &HashJoinExec,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    has_indexed: bool,
    has_unindexed: bool,
    index_info: &LanceJoinIndexInfo,
    insert_not_matched: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Extract source (right side of join)
    let source = hash_join.right().clone();

    // Get projection columns from the target (left) side
    let target_schema = hash_join.left().schema();
    let projection_columns: Vec<String> = target_schema
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID && f.name() != ROW_ADDR)
        .map(|f| f.name().clone())
        .collect();

    // For fully indexed case, IndexedLookupExec wraps source in ReplayExec internally.
    // For hybrid case, we still need to wrap source in ReplayExec for multiple consumption
    // since the source will be used by multiple join paths.
    let indexed_plan: Arc<dyn ExecutionPlan> = match (has_indexed, has_unindexed) {
        (true, false) => {
            // Fully indexed case - IndexedLookupExec handles ReplayExec internally
            create_fully_indexed_plan(
                source,
                dataset,
                index,
                key_column,
                &projection_columns,
                insert_not_matched,
            )?
        }
        (true, true) => {
            // Hybrid case - still needs ReplayExec for multiple paths
            let source_replay = Arc::new(ReplayExec::new(Capacity::Unbounded, source));
            create_hybrid_indexed_plan(
                source_replay,
                dataset,
                index,
                key_column,
                &projection_columns,
                index_info,
                insert_not_matched,
            )?
        }
        _ => {
            // No indexed fragments - shouldn't reach here
            return Err(datafusion::error::DataFusionError::Internal(
                "create_indexed_join_plan called with no indexed fragments".to_string(),
            ));
        }
    };

    // Add projection to match the expected schema from the original HashJoinExec
    let hash_join_schema = hash_join.schema();
    add_schema_mapping_projection(indexed_plan, &hash_join_schema)
}

/// Create a fully indexed plan (all fragments are indexed).
///
/// The IndexedLookupExec includes the join internally:
/// - Wraps source in ReplayExec
/// - Projects source keys for index lookup
/// - Uses MapIndexExec + TakeExec to fetch matching target rows
/// - Performs HashJoinExec to join source with target
fn create_fully_indexed_plan(
    source: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    projection_columns: &[String],
    insert_not_matched: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let join_type = if insert_not_matched {
        datafusion::logical_expr::JoinType::Left
    } else {
        datafusion::logical_expr::JoinType::Inner
    };

    // Use try_new_with_join to get the complete join pipeline internally
    let indexed_lookup: Arc<dyn ExecutionPlan> = Arc::new(IndexedLookupExec::try_new_with_join(
        source,
        dataset.clone(),
        index.clone(),
        key_column.to_string(),
        projection_columns.to_vec(),
        true, // with_row_addr
        join_type,
    )?);

    Ok(indexed_lookup)
}

/// Add a projection to map the indexed join output schema to the expected HashJoinExec schema.
fn add_schema_mapping_projection(
    indexed_plan: Arc<dyn ExecutionPlan>,
    expected_schema: &SchemaRef,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let indexed_schema = indexed_plan.schema();

    // Build projection expressions to reorder columns from indexed schema to expected schema
    let mut projection_exprs: Vec<(Arc<dyn datafusion_physical_expr::PhysicalExpr>, String)> =
        Vec::new();

    for expected_field in expected_schema.fields() {
        let expected_name = expected_field.name();

        // Find the column in the indexed schema
        if let Ok(idx) = indexed_schema.index_of(expected_name) {
            projection_exprs.push((
                Arc::new(Column::new(expected_name, idx)),
                expected_name.clone(),
            ));
        } else {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Column {} not found in indexed join output schema",
                expected_name
            )));
        }
    }

    // Create projection
    let projection = datafusion::physical_plan::projection::ProjectionExec::try_new(
        projection_exprs,
        indexed_plan,
    )?;

    Ok(Arc::new(projection))
}

/// Create a hybrid plan with both indexed and unindexed paths.
fn create_hybrid_indexed_plan(
    source: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    projection_columns: &[String],
    index_info: &LanceJoinIndexInfo,
    insert_not_matched: bool,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let unindexed_fragments = &index_info.unindexed_fragments;

    if insert_not_matched {
        // RIGHT join hybrid: collect targets from both paths, then outer join
        create_hybrid_plan_right_join(
            source,
            dataset,
            index,
            key_column,
            projection_columns,
            unindexed_fragments,
        )
    } else {
        // INNER join hybrid: split into two joins and union
        create_hybrid_plan_inner_join(
            source,
            dataset,
            index,
            key_column,
            projection_columns,
            unindexed_fragments,
        )
    }
}

fn create_hybrid_plan_right_join(
    source: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    projection_columns: &[String],
    unindexed_fragments: &[Fragment],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Source is already wrapped in ReplayExec by the caller.
    // Since source is consumed twice (once by IndexedLookupExec via key_projection,
    // once by the final HashJoinExec), the ReplayExec buffering handles this.

    // Project to key column for index lookup
    let key_expr = vec![(
        Arc::new(Column::new_with_schema(key_column, &source.schema())?)
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        key_column.to_string(),
    )];
    let key_projection: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::projection::ProjectionExec::try_new(key_expr, source.clone())?,
    );

    // Path 1: IndexedLookupExec (target rows only)
    let indexed_path: Arc<dyn ExecutionPlan> = Arc::new(IndexedLookupExec::try_new(
        key_projection,
        dataset.clone(),
        index.clone(),
        key_column.to_string(),
        projection_columns.to_vec(),
        true, // with_row_addr
    )?);

    // Path 2: Scan unindexed fragments
    let scan_config = LanceScanConfig {
        with_row_id: true,
        with_row_address: true,
        ..Default::default()
    };

    let target_schema = dataset
        .schema()
        .project(
            &projection_columns
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let unindexed_scan: Arc<dyn ExecutionPlan> = Arc::new(LanceScanExec::new(
        dataset.clone(),
        Arc::new(unindexed_fragments.to_vec()),
        None,
        Arc::new(target_schema),
        scan_config,
    ));

    // Union both paths (target rows only)
    let union_exec = UnionExec::new(vec![indexed_path, unindexed_scan]);
    let repartitioned =
        RepartitionExec::try_new(Arc::new(union_exec), Partitioning::RoundRobinBatch(1))?;

    // Final LEFT join: source on left (collected), target on right (streams)
    let source_key_idx = source.schema().index_of(key_column)?;
    let target_key_idx = repartitioned.schema().index_of(key_column)?;

    let final_join_on = vec![(
        Arc::new(Column::new(key_column, source_key_idx))
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        Arc::new(Column::new(key_column, target_key_idx))
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    )];

    let final_join = HashJoinExec::try_new(
        source,
        Arc::new(repartitioned),
        final_join_on,
        None,
        &datafusion::logical_expr::JoinType::Left,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
    )?;

    Ok(Arc::new(final_join))
}

fn create_hybrid_plan_inner_join(
    source: Arc<dyn ExecutionPlan>,
    dataset: &Arc<Dataset>,
    index: &IndexMetadata,
    key_column: &str,
    projection_columns: &[String],
    unindexed_fragments: &[Fragment],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Use the same pattern as right join: collect all targets first, then do ONE join.
    // This uses ReplayExec only twice (key projection + final join) instead of 3 times.

    // Project to key column for index lookup
    let key_expr = vec![(
        Arc::new(Column::new_with_schema(key_column, &source.schema())?)
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        key_column.to_string(),
    )];
    let key_projection: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::projection::ProjectionExec::try_new(key_expr, source.clone())?,
    );

    // Path 1: IndexedLookupExec (target rows only, no join)
    let indexed_path: Arc<dyn ExecutionPlan> = Arc::new(IndexedLookupExec::try_new(
        key_projection,
        dataset.clone(),
        index.clone(),
        key_column.to_string(),
        projection_columns.to_vec(),
        true, // with_row_addr
    )?);

    // Path 2: Scan unindexed fragments
    let scan_config = LanceScanConfig {
        with_row_id: true,
        with_row_address: true,
        ..Default::default()
    };

    let target_schema = dataset
        .schema()
        .project(
            &projection_columns
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

    let unindexed_scan: Arc<dyn ExecutionPlan> = Arc::new(LanceScanExec::new(
        dataset.clone(),
        Arc::new(unindexed_fragments.to_vec()),
        None,
        Arc::new(target_schema),
        scan_config,
    ));

    // Union both target paths (no join yet)
    let union_exec = UnionExec::new(vec![indexed_path, unindexed_scan]);
    let repartitioned =
        RepartitionExec::try_new(Arc::new(union_exec), Partitioning::RoundRobinBatch(1))?;

    // Final INNER join: source on left (collected), target on right (streams)
    let source_key_idx = source.schema().index_of(key_column)?;
    let target_key_idx = repartitioned.schema().index_of(key_column)?;

    let final_join_on = vec![(
        Arc::new(Column::new(key_column, source_key_idx))
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
        Arc::new(Column::new(key_column, target_key_idx))
            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    )];

    let final_join = HashJoinExec::try_new(
        source,
        Arc::new(repartitioned),
        final_join_on,
        None,
        &datafusion::logical_expr::JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
    )?;

    Ok(Arc::new(final_join))
}
