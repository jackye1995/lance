// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Bε manifest layout behind the production commit path.
//!
//! RESEARCH OPT-IN (discussion lance-format/lance#7499): a dataset created
//! with `lance.manifest.layout=betree` in its Overwrite config stores its
//! fragment metadata in the `lance-table` Bε-tree under `_bt/` instead of a
//! flat manifest under `_versions/`. Commits still enter through
//! [`CommitBuilder::execute`](super::write::CommitBuilder::execute), which
//! dispatches here before any flat manifest machinery runs, writes the
//! production transaction file under `_transactions/`, and publishes through
//! the tree's create-only root CAS.
//!
//! What is wired: create, append, data replacement (which covers both
//! file replacement and disjoint-field add-column), delete, and the lazy
//! [`Reader`]. What is not: `Dataset::open` and every scanner/index call
//! site. A Bε dataset has no `_versions/` manifests, so flat readers fail
//! with `DatasetNotFound` rather than reading anything wrong. The `Dataset`
//! handle returned by a Bε commit carries the schema and config with an
//! empty fragment list and is a commit handle only.

use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_table::betree::{
    BeTree, BeTreeConfig, CommitStats, MANIFEST_LAYOUT_KEY, ManifestLayout, TxnOperation, action,
};
use lance_table::format::{Fragment, Manifest, pb};
use lance_table::io::commit::{
    CommitConfig, CommitHandler, ManifestLocation, ManifestNamingScheme,
};
use object_store::path::Path;
use prost::Message;

use super::transaction::{DataReplacementGroup, Operation, Transaction};
use super::{DataStorageFormat, Dataset};
use crate::io::commit::{cleanup_transaction_file, write_transaction_file};
use crate::session::Session;

/// Bε node-size limit in bytes, read from Overwrite config at create time.
pub const MAX_NODE_BYTES_KEY: &str = "lance.betree.max_node_bytes";
/// Bε branching factor, read from Overwrite config at create time.
pub const MAX_CHILDREN_PER_NODE_KEY: &str = "lance.betree.max_children_per_node";
/// Bε leaf-size limit in bytes, read from Overwrite config at create time.
pub const MAX_LEAF_BYTES_KEY: &str = "lance.betree.max_leaf_bytes";
/// Root-delta-chain length limit, read from Overwrite config at create time.
pub const MAX_ROOT_DELTA_TAIL_KEY: &str = "lance.betree.max_root_delta_tail";

/// Everything the Bε commit backend needs from `CommitBuilder::execute_inner`.
pub(crate) struct CommitTarget {
    pub object_store: Arc<ObjectStore>,
    pub base_path: Path,
    pub uri: String,
    pub session: Arc<Session>,
    pub commit_handler: Arc<dyn CommitHandler>,
}

/// Whether a published Bε root exists under `base/_bt/root`.
///
/// Only consulted after `DatasetBuilder::load` fails with not-found, so flat
/// datasets never pay this listing.
pub(crate) async fn tree_exists(object_store: &ObjectStore, base: &Path) -> Result<bool> {
    let root_dir = base.clone().join("_bt").join("root");
    let mut roots = object_store.list(Some(root_dir));
    Ok(roots.try_next().await?.is_some())
}

/// Whether `operation` is an Overwrite that selects the Bε layout.
///
/// Rejects unknown layout values instead of ignoring them, so a typo cannot
/// silently create a flat dataset.
pub(crate) fn create_requested(operation: &Operation) -> Result<bool> {
    if let Operation::Overwrite {
        config_upsert_values: Some(config),
        ..
    } = operation
    {
        return Ok(ManifestLayout::from_config(config)? == ManifestLayout::BeTree);
    }
    Ok(false)
}

/// Whether an opened dataset's manifest config selects the Bε layout.
pub(crate) fn dataset_uses_betree(manifest: &Manifest) -> bool {
    manifest.config.get(MANIFEST_LAYOUT_KEY).map(String::as_str)
        == Some(lance_table::betree::MANIFEST_LAYOUT_BETREE)
}

/// Bootstrap a Bε dataset from an Overwrite transaction.
pub(crate) async fn execute_create(
    target: CommitTarget,
    transaction: &Transaction,
) -> Result<Dataset> {
    let Operation::Overwrite {
        fragments,
        schema,
        config_upsert_values: Some(config_upsert),
        ..
    } = &transaction.operation
    else {
        return Err(Error::invalid_input(
            "betree create requires an Overwrite operation with config_upsert_values",
        ));
    };
    let tree_config = tree_config_from(config_upsert)?;
    let skeleton = {
        let mut manifest = Manifest::new(
            schema.clone(),
            Arc::new(Vec::new()),
            DataStorageFormat::default(),
            HashMap::new(),
        );
        manifest.config = config_upsert.clone();
        pb::Manifest::from(&manifest).encode_to_vec()
    };
    let transaction_file =
        write_transaction_file(&target.object_store, &target.base_path, transaction).await?;
    let bootstrap = BeTree::bootstrap(
        target.object_store.clone(),
        target.base_path.clone(),
        scheduler_for(&target.object_store),
        Arc::new(LanceCache::with_capacity(0)),
        tree_config,
        fragments.clone(),
        skeleton,
    )
    .await;
    match bootstrap {
        Ok((tree, _)) => commit_handle(target, &tree, Some(transaction_file)),
        Err(error) => {
            cleanup_transaction_file(&target.object_store, &target.base_path, &transaction_file)
                .await;
            Err(error)
        }
    }
}

/// Commit one transaction against an existing Bε dataset.
///
/// Each attempt reopens the latest tree, writes a fresh production
/// transaction file, and publishes through the create-only root CAS. On a
/// conflict the stale transaction file is removed and the actions are
/// replayed against the new tip, up to `commit_config.num_retries` attempts.
/// Replay does not yet run the flat path's semantic conflict checks; actions
/// are last-writer-wins at the leaf.
pub(crate) async fn execute_commit(
    target: CommitTarget,
    commit_config: &CommitConfig,
    transaction: &Transaction,
) -> Result<Dataset> {
    let attempts = commit_config.num_retries.max(1);
    let mut last_error = None;
    for attempt in 1..=attempts {
        let mut tree = BeTree::open(
            target.object_store.clone(),
            target.base_path.clone(),
            scheduler_for(&target.object_store),
            Arc::new(LanceCache::with_capacity(0)),
        )
        .await?;
        let transaction_file =
            write_transaction_file(&target.object_store, &target.base_path, transaction).await?;
        match apply_operation(&mut tree, &transaction.operation).await {
            Ok(_) => return commit_handle(target, &tree, Some(transaction_file)),
            Err(error @ Error::CommitConflict { .. }) => {
                cleanup_transaction_file(
                    &target.object_store,
                    &target.base_path,
                    &transaction_file,
                )
                .await;
                if attempt == attempts {
                    return Err(error);
                }
                last_error = Some(error);
            }
            Err(error) => {
                cleanup_transaction_file(
                    &target.object_store,
                    &target.base_path,
                    &transaction_file,
                )
                .await;
                return Err(error);
            }
        }
    }
    Err(last_error.expect("retry loop runs at least once"))
}

async fn apply_operation(tree: &mut BeTree, operation: &Operation) -> Result<CommitStats> {
    match operation {
        Operation::Append { fragments } => tree.commit_append(fragments).await,
        Operation::DataReplacement { replacements } => {
            let actions = replacements
                .iter()
                .map(|DataReplacementGroup(fragment_id, file)| {
                    action::replace_data_file(*fragment_id, file)
                })
                .collect();
            tree.commit_as(TxnOperation::ReplaceDataFiles, actions)
                .await
        }
        Operation::Delete {
            updated_fragments,
            deleted_fragment_ids,
            ..
        } => {
            let mut actions =
                Vec::with_capacity(updated_fragments.len() + deleted_fragment_ids.len());
            actions.extend(
                deleted_fragment_ids
                    .iter()
                    .copied()
                    .map(action::remove_fragment),
            );
            actions.extend(updated_fragments.iter().map(action::add_fragment));
            tree.commit_as(TxnOperation::Delete, actions).await
        }
        other => Err(Error::not_supported_source(
            format!(
                "operation {} is not wired for lance.manifest.layout=betree yet",
                other.name()
            )
            .into(),
        )),
    }
}

/// The `Dataset` a Bε commit returns: schema and config from the tree's
/// manifest skeleton, version from the tree, and an EMPTY fragment list. It
/// is a commit handle for chaining further `CommitBuilder` calls; reads must
/// use [`Reader`] until scanner adoption.
fn commit_handle(
    target: CommitTarget,
    tree: &BeTree,
    transaction_file: Option<String>,
) -> Result<Dataset> {
    let mut manifest = Manifest::try_from(pb::Manifest::decode(tree.schema_bytes())?)?;
    manifest.version = tree.version();
    manifest.transaction_file = transaction_file;
    let manifest_location = ManifestLocation {
        version: manifest.version,
        path: target
            .base_path
            .clone()
            .join("_bt")
            .join("root")
            .join(format!("{}.root", manifest.version)),
        size: None,
        naming_scheme: ManifestNamingScheme::V2,
        e_tag: None,
    };
    Dataset::checkout_manifest(
        target.object_store,
        target.base_path,
        target.uri,
        Arc::new(manifest),
        manifest_location,
        target.session,
        target.commit_handler,
        None,
        None,
        None,
    )
}

fn tree_config_from(config: &HashMap<String, String>) -> Result<BeTreeConfig> {
    fn parse(config: &HashMap<String, String>, key: &str) -> Result<Option<u64>> {
        config
            .get(key)
            .map(|raw| {
                raw.parse::<u64>().map_err(|_| {
                    Error::invalid_input(format!("{key} must be an integer, got {raw:?}"))
                })
            })
            .transpose()
    }
    let mut tree_config = BeTreeConfig::default();
    if let Some(max_node_bytes) = parse(config, MAX_NODE_BYTES_KEY)? {
        tree_config.max_node_bytes = max_node_bytes;
        tree_config.max_leaf_bytes = max_node_bytes;
    }
    if let Some(max_leaf_bytes) = parse(config, MAX_LEAF_BYTES_KEY)? {
        tree_config.max_leaf_bytes = max_leaf_bytes;
    }
    if let Some(max_children) = parse(config, MAX_CHILDREN_PER_NODE_KEY)? {
        tree_config.max_children_per_node = u32::try_from(max_children).map_err(|_| {
            Error::invalid_input(format!(
                "{MAX_CHILDREN_PER_NODE_KEY} does not fit u32: {max_children}"
            ))
        })?;
    }
    if let Some(max_tail) = parse(config, MAX_ROOT_DELTA_TAIL_KEY)? {
        tree_config.max_root_delta_tail = u32::try_from(max_tail).map_err(|_| {
            Error::invalid_input(format!(
                "{MAX_ROOT_DELTA_TAIL_KEY} does not fit u32: {max_tail}"
            ))
        })?;
    }
    Ok(tree_config)
}

fn scheduler_for(object_store: &Arc<ObjectStore>) -> Arc<ScanScheduler> {
    ScanScheduler::new(
        object_store.clone(),
        SchedulerConfig::max_bandwidth(object_store),
    )
}

/// Lazy read session over a Bε dataset, the read surface until scanner
/// adoption. Open reads the compacted root plus any outstanding delta tail;
/// fragment resolution reads one root-to-leaf path.
pub struct Reader {
    tree: BeTree,
}

impl Reader {
    pub async fn open(object_store: Arc<ObjectStore>, base: Path) -> Result<Self> {
        let scheduler = scheduler_for(&object_store);
        Ok(Self {
            tree: BeTree::open(
                object_store,
                base,
                scheduler,
                Arc::new(LanceCache::with_capacity(0)),
            )
            .await?,
        })
    }

    /// Open from a dataset uri, resolving the object store the same way the
    /// commit path does.
    pub async fn open_uri(uri: &str) -> Result<Self> {
        let session = Arc::new(Session::default());
        let (object_store, base) =
            ObjectStore::from_uri_and_params(session.store_registry(), uri, &Default::default())
                .await?;
        Self::open(object_store, base).await
    }

    pub fn version(&self) -> u64 {
        self.tree.version()
    }

    pub fn count_fragments(&self) -> u64 {
        self.tree.count_fragments()
    }

    pub async fn resolve_fragment(&self, fragment_id: u64) -> Result<Option<Fragment>> {
        self.tree.resolve_fragment(fragment_id).await
    }

    /// Materialize the full fragment list, for benchmarks and global checks.
    pub async fn materialize(&self) -> Result<Vec<Fragment>> {
        self.tree.materialize().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::write::CommitBuilder;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::future::join_all;
    use lance_core::datatypes::Schema;
    use lance_table::betree::support::{
        make_backfill_data_file, make_fragment, make_replacement_data_file,
    };
    use lance_table::format::pb as table_pb;

    fn table_schema() -> Schema {
        Schema::try_from(&ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
        ]))
        .unwrap()
    }

    fn betree_config_upsert() -> HashMap<String, String> {
        HashMap::from([
            (
                MANIFEST_LAYOUT_KEY.to_string(),
                lance_table::betree::MANIFEST_LAYOUT_BETREE.to_string(),
            ),
            (MAX_NODE_BYTES_KEY.to_string(), (16 * 1024).to_string()),
            (MAX_CHILDREN_PER_NODE_KEY.to_string(), 4.to_string()),
            (MAX_ROOT_DELTA_TAIL_KEY.to_string(), 8.to_string()),
        ])
    }

    fn create_transaction(n: u64, config: HashMap<String, String>) -> Transaction {
        Transaction::new_from_version(
            0,
            Operation::Overwrite {
                fragments: (0..n).map(make_fragment).collect(),
                schema: table_schema(),
                config_upsert_values: Some(config),
                initial_bases: None,
            },
        )
    }

    async fn create_betree_dataset(uri: &str, n: u64) -> Dataset {
        CommitBuilder::new(uri)
            .execute(create_transaction(n, betree_config_upsert()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn create_reopen_and_resolve_through_commit_builder() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_betree_dataset(uri, 500).await;
        assert_eq!(dataset.manifest.version, 1);
        assert!(dataset_uses_betree(&dataset.manifest));
        assert_eq!(dataset.schema().fields.len(), 2);

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.version(), 1);
        assert_eq!(reader.count_fragments(), 500);
        assert_eq!(reader.resolve_fragment(123).await.unwrap().unwrap().id, 123);

        // No flat manifests exist, so flat readers fail closed.
        let flat_open = crate::dataset::builder::DatasetBuilder::from_uri(uri)
            .load()
            .await;
        assert!(flat_open.is_err());
    }

    #[tokio::test]
    async fn append_add_column_and_replace_commit_through_dataset_path() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_betree_dataset(uri, 500).await;

        let appended = CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new_from_version(
                1,
                Operation::Append {
                    fragments: vec![make_fragment(500)],
                },
            ))
            .await
            .unwrap();
        assert_eq!(appended.manifest.version, 2);

        // Add-column backfill: a new data file whose fields are disjoint from
        // every existing file on the touched fragments.
        let add_column = CommitBuilder::new(Arc::new(appended))
            .execute(Transaction::new_from_version(
                2,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_backfill_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(add_column.manifest.version, 3);

        // Replacement: same fields and file version as the base file.
        let replaced = CommitBuilder::new(Arc::new(add_column))
            .execute(Transaction::new_from_version(
                3,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_replacement_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();
        assert_eq!(replaced.manifest.version, 4);

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.count_fragments(), 501);
        let touched = reader.resolve_fragment(3).await.unwrap().unwrap();
        assert_eq!(touched.files.len(), 2);
        assert_eq!(touched.files[0].path, make_replacement_data_file(3, 0).path);
        assert_eq!(touched.files[1].path, make_backfill_data_file(3, 0).path);
        let untouched = reader.resolve_fragment(400).await.unwrap().unwrap();
        assert_eq!(untouched.files.len(), 1);
    }

    #[tokio::test]
    async fn production_transaction_file_stays_action_scale() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        let dataset = create_betree_dataset(uri, 2_000).await;

        CommitBuilder::new(Arc::new(dataset))
            .execute(Transaction::new_from_version(
                1,
                Operation::DataReplacement {
                    replacements: (0..10)
                        .map(|id| DataReplacementGroup(id, make_backfill_data_file(id, 0)))
                        .collect(),
                },
            ))
            .await
            .unwrap();

        let transactions_dir = std::fs::read_dir(tempdir.path().join("_transactions")).unwrap();
        let mut backfill_transaction = None;
        for entry in transactions_dir {
            let entry = entry.unwrap();
            let bytes = std::fs::read(entry.path()).unwrap();
            let decoded = table_pb::Transaction::decode(bytes.as_slice()).unwrap();
            if let Some(table_pb::transaction::Operation::DataReplacement(replacement)) =
                decoded.operation
            {
                backfill_transaction = Some((bytes.len(), replacement.replacements.len()));
            }
        }
        let (transaction_bytes, group_count) = backfill_transaction.unwrap();
        assert_eq!(group_count, 10);
        assert!(
            transaction_bytes < 4 * 1024,
            "10-group DataReplacement txn must stay action-scale at N=2000, \
             got {transaction_bytes} bytes"
        );
    }

    #[tokio::test]
    async fn default_layout_still_flat_and_unknown_layout_rejected() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        CommitBuilder::new(uri)
            .execute(create_transaction(5, HashMap::new()))
            .await
            .unwrap();
        assert!(
            tempdir.path().join("_versions").exists(),
            "default create must keep writing flat manifests"
        );
        assert!(!tempdir.path().join("_bt").exists());

        let unknown_dir = tempfile::tempdir().unwrap();
        let unknown_uri = unknown_dir.path().to_str().unwrap();
        let error = CommitBuilder::new(unknown_uri)
            .execute(create_transaction(
                5,
                HashMap::from([(MANIFEST_LAYOUT_KEY.to_string(), "tiered".to_string())]),
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("layout=\"tiered\""), "{error}");
    }

    #[tokio::test]
    async fn concurrent_appends_leave_a_sane_tip() {
        let tempdir = tempfile::tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();
        create_betree_dataset(uri, 100).await;

        let outcomes = join_all((0..4u64).map(|writer| {
            let uri = uri.to_string();
            async move {
                CommitBuilder::new(uri.as_str())
                    .execute(Transaction::new_from_version(
                        1,
                        Operation::Append {
                            fragments: vec![make_fragment(100 + writer)],
                        },
                    ))
                    .await
            }
        }))
        .await;

        let successes = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
        assert!(successes >= 1, "at least one concurrent append must land");

        let reader = Reader::open_uri(uri).await.unwrap();
        assert_eq!(reader.version(), 1 + successes as u64);
        assert_eq!(reader.count_fragments(), 100 + successes as u64);
        for outcome in outcomes.into_iter().flatten() {
            assert!(dataset_uses_betree(&outcome.manifest));
        }
    }
}
