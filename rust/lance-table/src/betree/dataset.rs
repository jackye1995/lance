// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Thin Dataset-shaped research adapter for the Bε manifest.
//!
//! This is intentionally not production `lance::Dataset` wiring. It proves
//! the transaction boundary needed by that wiring: Dataset operations translate
//! to [`FragmentAction`](pb::FragmentAction) values and publish through
//! [`BeTree::commit`] without materializing the tree on ordinary commits.

use std::sync::Arc;

use futures::Stream;
use futures::TryStreamExt;
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use object_store::path::Path;

use crate::betree::action;
use crate::betree::node::BeTreeConfig;
use crate::betree::tree::{BeTree, BootstrapStats, CommitStats, TxnOperation};
use crate::format::pb;
use crate::format::{DataFile, DeletionFile, Fragment};

/// Research config key selecting a manifest layout.
pub const MANIFEST_LAYOUT_KEY: &str = "lance.manifest.layout";

/// Research config value selecting the Bε manifest layout.
pub const MANIFEST_LAYOUT_BETREE: &str = "betree";

/// A Dataset-shaped session backed by a Bε-tree manifest.
///
/// Append, add-column, row-delete metadata, and whole-fragment delete map
/// directly to small Bε actions. Whole-table overwrite/restore are deliberately
/// the only helpers that enumerate the current tree to determine removals.
pub struct BeTreeDataset {
    tree: BeTree,
}

impl BeTreeDataset {
    /// Create a research Bε dataset.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        layout: &str,
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
        config: BeTreeConfig,
        fragments: Vec<Fragment>,
        schema_pb: Vec<u8>,
    ) -> Result<(Self, BootstrapStats)> {
        validate_layout(layout)?;
        let (tree, stats) = BeTree::bootstrap(
            object_store,
            base,
            scheduler,
            cache,
            config,
            fragments,
            schema_pb,
        )
        .await?;
        Ok((Self { tree }, stats))
    }

    /// Lazily open a research Bε dataset.
    ///
    /// This resolves and reads only the latest root. It does not materialize
    /// internal nodes or leaves.
    pub async fn open(
        layout: &str,
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
    ) -> Result<Self> {
        validate_layout(layout)?;
        Ok(Self {
            tree: BeTree::open(object_store, base, scheduler, cache).await?,
        })
    }

    /// Latest published manifest version.
    pub fn version(&self) -> u64 {
        self.tree.version()
    }

    /// Logical fragment count from root aggregates, without child IO.
    pub fn count_fragments(&self) -> u64 {
        self.tree.count_fragments()
    }

    /// Logical row count from root aggregates, without child IO.
    pub fn count_rows(&self) -> u64 {
        self.tree.count_rows()
    }

    /// Commit pretranslated fragment actions.
    pub async fn commit_actions(
        &mut self,
        actions: Vec<pb::FragmentAction>,
    ) -> Result<CommitStats> {
        self.tree.commit(actions).await
    }

    async fn commit_actions_as(
        &mut self,
        operation: TxnOperation,
        actions: Vec<pb::FragmentAction>,
    ) -> Result<CommitStats> {
        self.tree.commit_as(operation, actions).await
    }

    /// Append new fragments without enumerating existing fragments.
    ///
    /// As with Lance's production append planner, fragment ids must be freshly
    /// allocated. That precondition makes the aggregate delta known without a
    /// root-to-leaf lookup for every appended fragment.
    pub async fn append(&mut self, fragments: &[Fragment]) -> Result<CommitStats> {
        self.tree.commit_append(fragments).await
    }

    /// Attach add-column data files without enumerating existing fragments.
    pub async fn add_columns(&mut self, files: &[(u64, DataFile)]) -> Result<CommitStats> {
        self.commit_actions_as(
            TxnOperation::AddColumns,
            files
                .iter()
                .map(|(fragment_id, file)| action::add_data_file(*fragment_id, file))
                .collect(),
        )
        .await
    }

    /// Attach row-level deletion metadata without enumerating existing fragments.
    pub async fn set_deletion_files(
        &mut self,
        deletion_files: &[(u64, DeletionFile)],
    ) -> Result<CommitStats> {
        self.commit_actions_as(
            TxnOperation::SetDeletionFiles,
            deletion_files
                .iter()
                .map(|(fragment_id, deletion_file)| {
                    action::add_deletion_file(*fragment_id, deletion_file)
                })
                .collect(),
        )
        .await
    }

    /// Delete whole fragments without enumerating the tree.
    pub async fn delete(&mut self, fragment_ids: &[u64]) -> Result<CommitStats> {
        self.commit_actions_as(
            TxnOperation::Delete,
            fragment_ids
                .iter()
                .copied()
                .map(action::remove_fragment)
                .collect(),
        )
        .await
    }

    /// Remove selected data files, as needed by drop-column intent.
    pub async fn drop_data_files(&mut self, files: &[(u64, String)]) -> Result<CommitStats> {
        self.commit_actions_as(
            TxnOperation::DropDataFiles,
            files
                .iter()
                .map(|(fragment_id, path)| action::remove_data_file(*fragment_id, path.clone()))
                .collect(),
        )
        .await
    }

    /// Replace selected data files, as needed by cast/update intent.
    pub async fn replace_data_files(
        &mut self,
        replacements: &[(u64, String, DataFile)],
    ) -> Result<CommitStats> {
        let mut actions = Vec::with_capacity(replacements.len() * 2);
        for (fragment_id, old_path, replacement) in replacements {
            actions.push(action::remove_data_file(*fragment_id, old_path.clone()));
            actions.push(action::add_data_file(*fragment_id, replacement));
        }
        self.commit_actions_as(TxnOperation::ReplaceDataFiles, actions)
            .await
    }

    /// Replace the whole fragment set.
    ///
    /// Unlike targeted operations, overwrite must enumerate current ids so
    /// fragments absent from `replacement` can be tombstoned.
    pub async fn overwrite(&mut self, replacement: &[Fragment]) -> Result<CommitStats> {
        self.replace_all_as(TxnOperation::Overwrite, replacement)
            .await
    }

    /// Restore a caller-supplied snapshot through the same overwrite action map.
    pub async fn restore(&mut self, snapshot: &[Fragment]) -> Result<CommitStats> {
        self.replace_all_as(TxnOperation::Restore, snapshot).await
    }

    async fn replace_all_as(
        &mut self,
        operation: TxnOperation,
        replacement: &[Fragment],
    ) -> Result<CommitStats> {
        let current = self.tree.iter_fragments().try_collect::<Vec<_>>().await?;
        let mut actions = Vec::with_capacity(current.len() + replacement.len());
        actions.extend(
            current
                .iter()
                .map(|fragment| action::remove_fragment(fragment.id)),
        );
        actions.extend(replacement.iter().map(action::add_fragment));
        self.commit_actions_as(operation, actions).await
    }

    /// Resolve one fragment with root-to-leaf IO only.
    pub async fn resolve_fragment(&self, fragment_id: u64) -> Result<Option<Fragment>> {
        self.tree.resolve_fragment(fragment_id).await
    }

    /// Stream all fragments in id order with bounded leaf memory.
    pub fn iter_fragments(&self) -> impl Stream<Item = Result<Fragment>> + '_ {
        self.tree.iter_fragments()
    }

    /// Materialize all fragments for compatibility checks and explicit global operations.
    pub async fn materialize(&self) -> Result<Vec<Fragment>> {
        self.tree.materialize().await
    }

    /// Access the underlying research tree.
    pub fn tree(&self) -> &BeTree {
        &self.tree
    }
}

fn validate_layout(layout: &str) -> Result<()> {
    if layout == MANIFEST_LAYOUT_BETREE {
        Ok(())
    } else {
        Err(Error::invalid_input(format!(
            "unsupported {MANIFEST_LAYOUT_KEY} value: layout={layout:?}, expected={MANIFEST_LAYOUT_BETREE:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::betree::support::{make_backfill_data_file, make_fragment};
    use crate::format::{DeletionFileType, Fragment};
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::scheduler::SchedulerConfig;

    fn environment() -> (
        TempObjDir,
        Path,
        Arc<ObjectStore>,
        Arc<ScanScheduler>,
        Arc<LanceCache>,
    ) {
        let tempdir = TempObjDir::default();
        let base = tempdir.clone().join("betree");
        let object_store = Arc::new(ObjectStore::local());
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        let cache = Arc::new(LanceCache::with_capacity(64 * 1024 * 1024));
        (tempdir, base, object_store, scheduler, cache)
    }

    #[tokio::test]
    async fn append_add_columns_delete_and_lazy_reopen() {
        let (_tempdir, base, object_store, scheduler, cache) = environment();
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            BeTreeConfig::new(4 * 1024, 4),
            (0..20).map(make_fragment).collect(),
            Vec::new(),
        )
        .await
        .unwrap();

        dataset.append(&[make_fragment(20)]).await.unwrap();
        dataset
            .add_columns(&[
                (3, make_backfill_data_file(3, 0)),
                (17, make_backfill_data_file(17, 0)),
            ])
            .await
            .unwrap();
        dataset.delete(&[4]).await.unwrap();

        let reopened =
            BeTreeDataset::open(MANIFEST_LAYOUT_BETREE, object_store, base, scheduler, cache)
                .await
                .unwrap();
        assert_eq!(reopened.count_fragments(), 20);
        assert_eq!(reopened.resolve_fragment(4).await.unwrap(), None);
        assert_eq!(
            reopened
                .resolve_fragment(3)
                .await
                .unwrap()
                .unwrap()
                .files
                .len(),
            2
        );
        let materialized = reopened.materialize().await.unwrap();
        for fragment in &materialized {
            assert_eq!(
                reopened
                    .resolve_fragment(fragment.id)
                    .await
                    .unwrap()
                    .as_ref(),
                Some(fragment)
            );
        }
    }

    #[tokio::test]
    async fn access_pattern_intents_round_trip() {
        let (_tempdir, base, object_store, scheduler, cache) = environment();
        let original: Vec<Fragment> = (0..6).map(make_fragment).collect();
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            original.clone(),
            Vec::new(),
        )
        .await
        .unwrap();

        let old_path = original[0].files[0].path.clone();
        dataset
            .drop_data_files(&[(0, old_path.clone())])
            .await
            .unwrap();
        assert!(
            dataset
                .resolve_fragment(0)
                .await
                .unwrap()
                .unwrap()
                .files
                .is_empty()
        );

        let cast_file = make_backfill_data_file(1, 7);
        dataset
            .replace_data_files(&[(1, original[1].files[0].path.clone(), cast_file.clone())])
            .await
            .unwrap();
        assert_eq!(
            dataset.resolve_fragment(1).await.unwrap().unwrap().files,
            vec![cast_file]
        );

        let deletion_file = DeletionFile {
            read_version: dataset.version(),
            id: 9,
            file_type: DeletionFileType::Bitmap,
            num_deleted_rows: Some(1),
            base_id: None,
        };
        dataset
            .set_deletion_files(&[(2, deletion_file.clone())])
            .await
            .unwrap();
        assert_eq!(
            dataset
                .resolve_fragment(2)
                .await
                .unwrap()
                .unwrap()
                .deletion_file,
            Some(deletion_file)
        );

        let overwrite = vec![make_fragment(40), make_fragment(41)];
        dataset.overwrite(&overwrite).await.unwrap();
        assert_eq!(dataset.materialize().await.unwrap(), overwrite);

        dataset.restore(&original).await.unwrap();
        assert_eq!(dataset.materialize().await.unwrap(), original);
    }

    #[tokio::test]
    async fn commits_publish_transaction_records_with_operation_kinds() {
        let (_tempdir, base, object_store, scheduler, cache) = environment();
        let original: Vec<Fragment> = (0..20).map(make_fragment).collect();
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            original.clone(),
            Vec::new(),
        )
        .await
        .unwrap();

        let append = dataset.append(&[make_fragment(20)]).await.unwrap();
        dataset
            .add_columns(&[(3, make_backfill_data_file(3, 0))])
            .await
            .unwrap();
        dataset
            .replace_data_files(&[(
                1,
                original[1].files[0].path.clone(),
                make_backfill_data_file(1, 7),
            )])
            .await
            .unwrap();
        dataset.delete(&[4]).await.unwrap();

        assert!(append.transaction_bytes > 0);
        assert_eq!(
            append.total_bytes(),
            append.tree_write_bytes + append.transaction_bytes
        );
        let expectations = [
            (2, "append", 1),
            (3, "add_columns", 1),
            (4, "replace_data_files", 2),
            (5, "delete", 1),
        ];
        for (version, operation, action_count) in expectations {
            let transaction = dataset.tree().read_transaction(version).await.unwrap();
            assert_eq!(transaction.version, version);
            assert_eq!(transaction.base_version, version - 1);
            assert_eq!(transaction.operation, operation);
            assert_eq!(transaction.actions.len(), action_count);
        }
        assert!(!dataset.tree().transaction_exists(1).await.unwrap());
        assert!(!dataset.tree().transaction_exists(6).await.unwrap());
    }

    #[tokio::test]
    async fn rejects_non_betree_layout() {
        let (_tempdir, base, object_store, scheduler, cache) = environment();
        let error = BeTreeDataset::create(
            "tiered",
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::default(),
            vec![make_fragment(0)],
            Vec::new(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("layout=\"tiered\""));
    }

    #[tokio::test]
    async fn append_rejects_duplicate_fragment_ids_without_mutation() {
        let (_tempdir, base, object_store, scheduler, cache) = environment();
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            vec![make_fragment(0)],
            Vec::new(),
        )
        .await
        .unwrap();
        let version = dataset.version();

        let error = dataset
            .append(&[make_fragment(1), make_fragment(1)])
            .await
            .unwrap_err();

        assert!(error.to_string().contains("fragment_id=1"));
        assert_eq!(dataset.version(), version);
        assert_eq!(dataset.count_fragments(), 1);
    }
}
