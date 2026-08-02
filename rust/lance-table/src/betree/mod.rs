// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! PROTOTYPE (discussion lance-format/lance#7499): a full recursive,
//! self-balancing Bε-tree manifest.
//!
//! A benchmark artifact, not a production format. Verifies the counter-proposal:
//! for add-column / backfill (data files trickle into fragments over *many*
//! commits), a Bε-tree keeps per-commit write cost bounded (≈ the ε-buffer) and
//! self-balances (split on growth, merge on shrink) as fragments × columns grow,
//! whereas the flat manifest rewrites the full growing fragment list every commit.
//!
//! ```text
//!   root (protobuf manifest) = child refs + fragment_actions ε-buffer + metadata
//!        |  msn-tag actions per commit; flush the fullest child's batch on overflow
//!        v
//!   internal nodes (protobuf) = pivots + ε-buffer   (recurse)
//!        v
//!   leaves (Lance files)      = fragment tables      (messages applied here)
//! ```
//!
//! Modules: [`node`] (types + pure logic), [`store`] (copy-on-write node IO),
//! [`tree`] (bootstrap / commit / flush / split / merge / materialize).

pub mod action;
pub mod commit_builder;
pub mod dataset;
pub mod flat_baseline;
pub mod node;
pub mod store;
pub mod support;
pub mod tree;

pub use commit_builder::{MANIFEST_LAYOUT_FLAT, ManifestLayout, Operation};
pub use dataset::{BeTreeDataset, MANIFEST_LAYOUT_BETREE, MANIFEST_LAYOUT_KEY};
pub use node::{
    BeTreeConfig, DEFAULT_MAX_CHILDREN_PER_NODE, DEFAULT_MAX_LEAF_BYTES, DEFAULT_MAX_NODE_BYTES,
};
pub use tree::{BeTree, BootstrapStats, CommitStats, GcStats, TxnOperation};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::betree::flat_baseline::FlatBaseline;
    use crate::betree::support::{make_backfill_data_file, make_fragment};
    use crate::format::Fragment;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use futures::future::join_all;
    use lance_core::cache::LanceCache;
    use lance_core::datatypes::Schema;
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
    use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
    use lance_io::utils::tracking_store::IOTracker;
    use object_store::path::Path;
    use std::sync::Arc;

    fn test_env() -> (Arc<ObjectStore>, Arc<ScanScheduler>, Arc<LanceCache>) {
        let object_store = Arc::new(ObjectStore::local());
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        let cache = Arc::new(LanceCache::with_capacity(64 * 1024 * 1024));
        (object_store, scheduler, cache)
    }

    fn schema() -> Schema {
        Schema::try_from(&ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
        ]))
        .unwrap()
    }

    async fn tracked_test_env(
        io: &IOTracker,
    ) -> (Arc<ObjectStore>, Path, Arc<ScanScheduler>, Arc<LanceCache>) {
        let params = ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(io.clone())),
            ..Default::default()
        };
        let (object_store, base) = ObjectStore::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            "memory://",
            &params,
        )
        .await
        .unwrap();
        let scheduler =
            ScanScheduler::new(object_store.clone(), SchedulerConfig::default_for_testing());
        let cache = Arc::new(LanceCache::with_capacity(0));
        (object_store, base, scheduler, cache)
    }

    async fn tracked_lazy_tree(
        io: &IOTracker,
    ) -> (
        BeTree,
        BootstrapStats,
        Arc<ObjectStore>,
        Path,
        Arc<ScanScheduler>,
        Arc<LanceCache>,
    ) {
        const NUM_FRAGMENTS: u64 = 400;
        let (object_store, base, scheduler, cache) = tracked_test_env(io).await;
        let fragments = (0..NUM_FRAGMENTS).map(make_fragment).collect();
        let (tree, stats) = BeTree::bootstrap(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            BeTreeConfig::new(4 * 1024, 4),
            fragments,
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(stats.height >= 2, "lazy tests require a multi-level tree");
        io.incremental_stats();
        (tree, stats, object_store, base, scheduler, cache)
    }

    /// A multi-column backfill over a multi-level Bε-tree grows leaves past the
    /// leaf byte limit, so the tree must SPLIT — and still materialize the exact
    /// same fragment state as the flat manifest.
    #[tokio::test]
    async fn recursive_betree_splits_and_matches_flat() {
        let n: u64 = 3_000;
        let f: u64 = 10; // fragments per commit
        let columns: u32 = 3;
        // Tiny nodes so the tree is several levels deep and splits under backfill.
        let config = BeTreeConfig::new(16 * 1024, 4);

        let tempdir = TempObjDir::default();
        let betree_base = tempdir.clone().join("betree");
        let flat_base = tempdir.clone().join("flat");
        let (object_store, scheduler, cache) = test_env();
        let fragments: Vec<Fragment> = (0..n).map(make_fragment).collect();

        let (mut tree, boot) = BeTree::bootstrap(
            object_store.clone(),
            betree_base.clone(),
            scheduler.clone(),
            cache.clone(),
            config,
            fragments.clone(),
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(
            boot.num_leaves > 1 && boot.height >= 2,
            "expect a multi-level tree"
        );

        let mut flat =
            FlatBaseline::new(object_store.clone(), flat_base.clone(), schema(), fragments);
        flat.write().await.unwrap();

        // Backfill `columns` new data files into every fragment, over many small commits.
        let commits = n.div_ceil(f);
        let mut total_splits = 0u64;
        for col in 0..columns {
            for c in 0..commits {
                let start = c * f;
                let end = (start + f).min(n);
                let mut actions = Vec::new();
                let mut flat_adds = Vec::new();
                for id in start..end {
                    let df = make_backfill_data_file(id, col);
                    actions.push(action::add_data_file(id, &df));
                    flat_adds.push((id, df));
                }
                total_splits += tree.commit(actions).await.unwrap().splits;
                flat.commit_add_data_files(&flat_adds).await.unwrap();
            }
        }
        assert!(
            total_splits > 0,
            "growing leaves past max_leaf_bytes should have split the tree"
        );

        // Replace the base data file on a fragment subset through both paths,
        // with production DataReplacement semantics on each side, so the flat
        // baseline's replacement commit is proven against the tree's.
        let replacements: Vec<(u64, crate::format::DataFile)> = (0..n)
            .step_by(97)
            .map(|id| {
                (
                    id,
                    crate::betree::support::make_replacement_data_file(id, 0),
                )
            })
            .collect();
        let replace_actions = replacements
            .iter()
            .map(|(id, replacement)| action::replace_data_file(*id, replacement))
            .collect();
        tree.commit(replace_actions).await.unwrap();
        flat.commit_data_replacements(&replacements).await.unwrap();

        // Materialized Bε-tree state == flat manifest state.
        let mut tree_frags = tree.materialize().await.unwrap();
        let flat_manifest = FlatBaseline::cold_open(&object_store, &flat_base, flat.version())
            .await
            .unwrap();
        let mut flat_frags = flat_manifest.fragments.as_ref().clone();
        tree_frags.sort_by_key(|f| f.id);
        flat_frags.sort_by_key(|f| f.id);

        assert_eq!(tree_frags.len(), n as usize);
        assert_eq!(tree_frags.len(), flat_frags.len());
        for (a, b) in tree_frags.iter().zip(flat_frags.iter()) {
            assert_eq!(a.id, b.id);
            let mut ap: Vec<_> = a.files.iter().map(|f| f.path.clone()).collect();
            let mut bp: Vec<_> = b.files.iter().map(|f| f.path.clone()).collect();
            ap.sort();
            bp.sort();
            assert_eq!(ap, bp, "fragment {} data files differ", a.id);
            assert_eq!(
                a.files.len(),
                1 + columns as usize,
                "fragment {} file count",
                a.id
            );
        }

        // Cold open from storage (root → internal → leaves + buffer overlay) matches.
        let cold = BeTree::cold_open(object_store, betree_base, scheduler, cache)
            .await
            .unwrap();
        assert_eq!(cold.len(), n as usize);
    }

    /// Sparsely deleting 4 of every 5 fragments shrinks each leaf below the merge
    /// floor *without* emptying it, so the tree must MERGE (coalesce underflowing
    /// nodes) — and materialize the correct remainder. Also guards the empty-node
    /// routing invariant: an emptied node must be dropped, not kept with min_key=0.
    #[tokio::test]
    async fn recursive_betree_merges_on_bulk_delete() {
        let n: u64 = 3_000;
        let f: u64 = 20;
        // Small nodes + wide branching factor: a shallow tree so removes reach leaves.
        let config = BeTreeConfig::new(4 * 1024, 32);

        let tempdir = TempObjDir::default();
        let base = tempdir.clone().join("betree");
        let (object_store, scheduler, cache) = test_env();
        let fragments: Vec<Fragment> = (0..n).map(make_fragment).collect();

        let (mut tree, _boot) = BeTree::bootstrap(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            config,
            fragments,
            Vec::new(),
        )
        .await
        .unwrap();

        // Remove 4 of every 5 fragments (keep id % 5 == 4): each leaf keeps ~1/5,
        // dropping below the merge floor → coalesces with neighbors.
        let mut merges = 0u64;
        for c in 0..(n / f) {
            let actions: Vec<_> = (c * f..c * f + f)
                .filter(|id| id % 5 != 4)
                .map(action::remove_fragment)
                .collect();
            merges += tree.commit(actions).await.unwrap().merges;
        }
        assert!(
            merges > 0,
            "shrinking leaves below the merge floor should have coalesced them"
        );

        // Materialize the correct remainder (buffered removes are applied too).
        let mut remaining = tree.materialize().await.unwrap();
        remaining.sort_by_key(|f| f.id);
        assert_eq!(remaining.len(), (n / 5) as usize);
        assert!(
            remaining.iter().all(|f| f.id % 5 == 4),
            "only every fifth fragment survives"
        );
    }

    /// Regression: aggressively flushing a full-prefix delete (tiny min_flush)
    /// fully empties leaves. An emptied node must be *dropped* from its parent,
    /// not kept with min_key=0 — otherwise it sorts to the front and low-id
    /// removes misroute to it and no-op, resurrecting deleted fragments.
    #[tokio::test]
    async fn recursive_betree_empty_leaves_do_not_resurrect() {
        let n: u64 = 3_000;
        let remove: u64 = 2_400; // delete ids [0, 2400); keep [2400, 3000)
        let f: u64 = 20;
        // Force the pathological case: tiny flush gate → leaves empty completely.
        let config = BeTreeConfig {
            max_node_bytes: 4 * 1024,
            max_leaf_bytes: 4 * 1024,
            max_children_per_node: 32,
            min_flush_override: Some(64),
            max_root_delta_tail: 0,
        };

        let tempdir = TempObjDir::default();
        let base = tempdir.clone().join("betree");
        let (object_store, scheduler, cache) = test_env();
        let fragments: Vec<Fragment> = (0..n).map(make_fragment).collect();
        let (mut tree, _boot) = BeTree::bootstrap(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            config,
            fragments,
            Vec::new(),
        )
        .await
        .unwrap();

        for c in 0..(remove / f) {
            let actions: Vec<_> = (c * f..c * f + f).map(action::remove_fragment).collect();
            tree.commit(actions).await.unwrap();
        }

        let mut remaining = tree.materialize().await.unwrap();
        remaining.sort_by_key(|f| f.id);
        assert_eq!(
            remaining.len(),
            (n - remove) as usize,
            "removes must not be lost"
        );
        assert!(
            remaining.iter().all(|f| f.id >= remove),
            "the deleted prefix must not resurrect"
        );
    }

    #[tokio::test]
    async fn lazy_open_does_not_read_leaves() {
        let io = IOTracker::default();
        let (tree, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let expected_height = tree.height();
        let expected_root_children = tree.root_child_count();

        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        let open_io = io.incremental_stats();

        assert_eq!(
            open_io.read_iops, 2,
            "open should list root versions and GET the published root only"
        );
        assert_eq!(opened.height(), expected_height);
        assert_eq!(opened.root_child_count(), expected_root_children);
        assert_eq!(opened.root_buffer_len(), 0);
    }

    #[tokio::test]
    async fn root_publication_allows_one_of_ten_writers_and_restores_losers() {
        let io = IOTracker::default();
        let (_, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let writers = join_all((0..10).map(|_| {
            BeTree::open(
                object_store.clone(),
                base.clone(),
                scheduler.clone(),
                cache.clone(),
            )
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert!(writers.iter().all(|writer| writer.version() == 1));

        let outcomes = join_all(writers.into_iter().enumerate().map(
            |(salt, mut writer)| async move {
                let actions = (0..100)
                    .map(|fragment_id| {
                        action::add_data_file(
                            fragment_id,
                            &make_backfill_data_file(fragment_id, salt as u32),
                        )
                    })
                    .collect();
                let result = writer.commit(actions).await;
                (writer, result)
            },
        ))
        .await;

        let successes = outcomes.iter().filter(|(_, result)| result.is_ok()).count();
        assert_eq!(successes, 1, "exactly one writer may publish version 2");
        for (writer, result) in &outcomes {
            if let Err(error) = result {
                assert!(
                    matches!(error, lance_core::Error::CommitConflict { version: 2, .. }),
                    "expected version-2 commit conflict, got {error}"
                );
                assert_eq!(
                    writer.version(),
                    1,
                    "a failed publish must restore the writer session"
                );
                assert_eq!(writer.count_fragments(), 400);
            }
        }

        let winner = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        assert_eq!(winner.version(), 2);
        let transaction = winner.read_transaction(2).await.unwrap();
        assert_eq!(transaction.version, 2);
        assert_eq!(transaction.base_version, 1);
        assert_eq!(transaction.actions.len(), 100);
        assert!(
            !winner.transaction_exists(3).await.unwrap(),
            "losing writers must not publish a transaction record"
        );
        let gc = winner.gc_unreferenced_offline().await.unwrap();
        assert!(
            gc.objects_deleted > 0,
            "losing writers' pre-publication COW objects should be orphaned"
        );
        assert_eq!(winner.materialize().await.unwrap().len(), 400);
    }

    #[tokio::test]
    async fn delta_commits_defer_root_rewrites_until_fold() {
        const TAIL_CAP: u32 = 4;
        let tempdir = TempObjDir::default();
        let (object_store, scheduler, cache) = test_env();
        let (mut tree, _) = BeTree::bootstrap(
            object_store,
            tempdir.clone().join("betree"),
            scheduler,
            cache,
            BeTreeConfig::new(64 * 1024, 4).with_root_delta_tail(TAIL_CAP),
            (0..400).map(make_fragment).collect(),
            Vec::new(),
        )
        .await
        .unwrap();

        let mut delta_bytes = Vec::new();
        for salt in 0..TAIL_CAP {
            let stats = tree
                .commit(vec![action::add_data_file(
                    salt as u64,
                    &make_backfill_data_file(salt as u64, salt),
                )])
                .await
                .unwrap();
            assert!(stats.delta_bytes > 0);
            assert_eq!(stats.tree_write_bytes, 0);
            assert_eq!(stats.transaction_bytes, 0);
            assert_eq!(stats.folds, 0);
            assert_eq!(stats.delta_tail, salt + 1);
            delta_bytes.push(stats.delta_bytes);
        }

        let fold = tree
            .commit(vec![action::add_data_file(
                40,
                &make_backfill_data_file(40, 99),
            )])
            .await
            .unwrap();
        assert_eq!(fold.delta_bytes, 0);
        assert!(fold.tree_write_bytes > 0);
        assert!(fold.transaction_bytes > 0);
        assert_eq!(fold.folds, 1);
        assert_eq!(fold.delta_tail, 0);
        assert!(
            delta_bytes
                .iter()
                .all(|bytes| *bytes < fold.tree_write_bytes),
            "a delta must be smaller than the compacted root it defers: \
             deltas={delta_bytes:?}, fold={}",
            fold.tree_write_bytes
        );
        let spread = delta_bytes.iter().max().unwrap() - delta_bytes.iter().min().unwrap();
        assert!(
            spread <= 8,
            "same-shape commits must produce near-constant delta sizes, \
             got {delta_bytes:?}"
        );
    }

    #[tokio::test]
    async fn reopen_with_outstanding_deltas_matches_session_state() {
        const TAIL_CAP: u32 = 8;
        let io = IOTracker::default();
        let (object_store, base, scheduler, cache) = tracked_test_env(&io).await;
        let (mut tree, _) = BeTree::bootstrap(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            BeTreeConfig::new(64 * 1024, 4).with_root_delta_tail(TAIL_CAP),
            (0..400).map(make_fragment).collect(),
            Vec::new(),
        )
        .await
        .unwrap();

        tree.commit(vec![action::add_fragment(&make_fragment(400))])
            .await
            .unwrap();
        tree.commit(vec![action::add_data_file(
            7,
            &make_backfill_data_file(7, 0),
        )])
        .await
        .unwrap();
        let outstanding = tree.commit(vec![action::remove_fragment(3)]).await.unwrap();
        assert_eq!(outstanding.delta_tail, 3);
        let mut expected = tree.materialize().await.unwrap();
        expected.sort_by_key(|fragment| fragment.id);
        drop(tree);

        io.incremental_stats();
        let reopened = BeTree::open(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
        )
        .await
        .unwrap();
        let open_reads = io.incremental_stats().read_iops;
        assert_eq!(
            open_reads, 5,
            "open with a 3-delta tail should read list, tip, base, and 2 \
             intermediate deltas, got {open_reads}"
        );
        assert_eq!(reopened.version(), 4);
        assert_eq!(reopened.count_fragments(), 400);
        assert_eq!(reopened.resolve_fragment(3).await.unwrap(), None);
        assert_eq!(
            reopened
                .resolve_fragment(7)
                .await
                .unwrap()
                .unwrap()
                .files
                .len(),
            2
        );
        let mut materialized = reopened.materialize().await.unwrap();
        materialized.sort_by_key(|fragment| fragment.id);
        assert_eq!(materialized, expected);

        let gc = reopened.gc_unreferenced_offline().await.unwrap();
        assert_eq!(
            gc.roots_scanned, 4,
            "compacted root and every delta stay live history"
        );
        let mut survived = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap()
            .materialize()
            .await
            .unwrap();
        survived.sort_by_key(|fragment| fragment.id);
        assert_eq!(survived, expected);
    }

    #[tokio::test]
    async fn delta_publication_allows_single_winner_and_restores_loser() {
        let tempdir = TempObjDir::default();
        let (object_store, scheduler, cache) = test_env();
        let base = tempdir.clone().join("betree");
        let (tree, _) = BeTree::bootstrap(
            object_store.clone(),
            base.clone(),
            scheduler.clone(),
            cache.clone(),
            BeTreeConfig::new(64 * 1024, 4).with_root_delta_tail(8),
            (0..400).map(make_fragment).collect(),
            Vec::new(),
        )
        .await
        .unwrap();
        drop(tree);

        let mut writers = join_all((0..2).map(|_| {
            BeTree::open(
                object_store.clone(),
                base.clone(),
                scheduler.clone(),
                cache.clone(),
            )
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        let mut second = writers.pop().unwrap();
        let mut first = writers.pop().unwrap();

        let first_result = first
            .commit(vec![action::add_data_file(
                1,
                &make_backfill_data_file(1, 1),
            )])
            .await;
        let second_result = second
            .commit(vec![action::add_data_file(
                2,
                &make_backfill_data_file(2, 2),
            )])
            .await;

        assert!(first_result.is_ok());
        assert!(first_result.unwrap().delta_bytes > 0);
        let conflict = second_result.unwrap_err();
        assert!(
            matches!(
                conflict,
                lance_core::Error::CommitConflict { version: 2, .. }
            ),
            "expected version-2 conflict on the shared delta path, got {conflict}"
        );
        assert_eq!(second.version(), 1);
        assert_eq!(second.root_buffer_len(), 0, "loser buffer must be restored");

        let winner = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        assert_eq!(winner.version(), 2);
        assert_eq!(
            winner
                .resolve_fragment(1)
                .await
                .unwrap()
                .unwrap()
                .files
                .len(),
            2
        );
        assert_eq!(
            winner
                .resolve_fragment(2)
                .await
                .unwrap()
                .unwrap()
                .files
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn transaction_bytes_track_actions_not_table_size() {
        let backfill_batch = |salt: u32| {
            (0..10)
                .map(|id| action::add_data_file(id, &make_backfill_data_file(id, salt)))
                .collect::<Vec<_>>()
        };
        let mut transaction_bytes = Vec::new();
        for n in [400u64, 4_000] {
            let tempdir = TempObjDir::default();
            let (object_store, scheduler, cache) = test_env();
            let (mut tree, _) = BeTree::bootstrap(
                object_store,
                tempdir.clone().join("betree"),
                scheduler,
                cache,
                BeTreeConfig::new(16 * 1024, 4),
                (0..n).map(make_fragment).collect(),
                Vec::new(),
            )
            .await
            .unwrap();
            let stats = tree.commit(backfill_batch(0)).await.unwrap();
            assert!(stats.transaction_bytes > 0);
            assert!(
                stats.transaction_bytes < 4 * 1024,
                "10-action transaction must stay action-scale, got {} bytes",
                stats.transaction_bytes
            );
            transaction_bytes.push(stats.transaction_bytes);
        }
        assert_eq!(
            transaction_bytes[0], transaction_bytes[1],
            "transaction record size must not vary with live fragment count"
        );
    }

    #[tokio::test]
    async fn open_reads_root_only_without_transaction_history() {
        let io = IOTracker::default();
        let (mut tree, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        for salt in 0..20u32 {
            tree.commit(
                (0..10)
                    .map(|id| action::add_data_file(id, &make_backfill_data_file(id, salt)))
                    .collect(),
            )
            .await
            .unwrap();
        }
        drop(tree);

        io.incremental_stats();
        let reopened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        let open_reads = io.incremental_stats().read_iops;
        assert_eq!(reopened.version(), 21);
        assert!(
            open_reads <= 2,
            "open must list and read the latest root only, got {open_reads} reads"
        );
    }

    #[tokio::test]
    async fn resolve_matches_materialize() {
        let io = IOTracker::default();
        let (mut tree, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let actions = (0..100)
            .map(|frag_id| action::add_data_file(frag_id, &make_backfill_data_file(frag_id, 0)))
            .collect();
        tree.commit(actions).await.unwrap();
        let expected: std::collections::BTreeMap<_, _> = tree
            .materialize()
            .await
            .unwrap()
            .into_iter()
            .map(|fragment| (fragment.id, fragment))
            .collect();
        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();

        for index in 0..40 {
            let frag_id = (index * 37) % 400;
            assert_eq!(
                opened.resolve_fragment(frag_id).await.unwrap(),
                expected.get(&frag_id).cloned(),
                "resolved fragment differs for frag_id={frag_id}"
            );
        }
        assert_eq!(opened.resolve_fragment(10_000).await.unwrap(), None);
    }

    #[tokio::test]
    async fn resolve_get_bound() {
        let io = IOTracker::default();
        let (_, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        io.incremental_stats();

        assert!(opened.resolve_fragment(211).await.unwrap().is_some());
        let resolve_io = io.incremental_stats();
        assert!(
            resolve_io.read_iops <= u64::from(opened.height()) + 2,
            "resolve used {} reads at height {}",
            resolve_io.read_iops,
            opened.height()
        );
    }

    #[tokio::test]
    async fn bitmap_resolve_prunes_uncovered_subtrees() {
        let io = IOTracker::default();
        let (_, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        let fragment_ids = roaring::RoaringBitmap::from_iter([3, 4, 211, 399]);
        let expected: Vec<_> = opened
            .materialize()
            .await
            .unwrap()
            .into_iter()
            .filter(|fragment| fragment_ids.contains(fragment.id as u32))
            .collect();
        let materialize_gets = io.incremental_stats().read_iops;

        let actual = opened.resolve_fragments(&fragment_ids).await.unwrap();
        let selective_gets = io.incremental_stats().read_iops;
        assert_eq!(actual, expected);
        assert!(
            selective_gets < materialize_gets,
            "bitmap resolve should prune IO: selective={selective_gets}, materialize={materialize_gets}"
        );
    }

    #[tokio::test]
    async fn bitmap_resolve_keeps_buffered_inserts_outside_live_child_bounds() {
        const NUM_FRAGMENTS: u64 = 400;
        const FIRST_FRAGMENT_ID: u64 = 100_000;
        const ID_STRIDE: u64 = 1_000;

        let (object_store, base, scheduler, cache) = tracked_test_env(&IOTracker::default()).await;
        let fragments = (0..NUM_FRAGMENTS)
            .map(|index| make_fragment(FIRST_FRAGMENT_ID + index * ID_STRIDE))
            .collect();
        let (mut tree, stats) = BeTree::bootstrap(
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            fragments,
            Vec::new(),
        )
        .await
        .unwrap();
        assert!(stats.height >= 2);
        assert!(tree.root_children_for_testing().len() >= 2);

        let last_fragment_id = FIRST_FRAGMENT_ID + (NUM_FRAGMENTS - 1) * ID_STRIDE;
        let mut inserted_ids = Vec::new();
        let mut saw_root_only_flush = false;
        for batch in 0..20 {
            let low_id = batch * 2;
            let high_id = last_fragment_id + 1 + batch * 2;
            inserted_ids.extend([low_id, high_id]);
            let stats = tree
                .commit(vec![
                    action::add_fragment(&make_fragment(low_id)),
                    action::add_fragment(&make_fragment(high_id)),
                ])
                .await
                .unwrap();
            if stats.flushes > 0 && stats.max_flush_depth == 0 {
                saw_root_only_flush = true;
                break;
            }
        }
        assert!(
            saw_root_only_flush,
            "test requires inserts buffered below the root but above a leaf"
        );

        let fragment_ids = roaring::RoaringBitmap::from_iter(
            inserted_ids
                .iter()
                .map(|fragment_id| u32::try_from(*fragment_id).unwrap()),
        );
        let resolved = tree.resolve_fragments(&fragment_ids).await.unwrap();
        let resolved_ids: Vec<_> = resolved.iter().map(|fragment| fragment.id).collect();
        inserted_ids.sort_unstable();
        assert_eq!(resolved_ids, inserted_ids);
    }

    #[tokio::test]
    async fn stream_equals_materialize() {
        let io = IOTracker::default();
        let (mut tree, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let actions = (0..100)
            .map(|frag_id| action::add_data_file(frag_id, &make_backfill_data_file(frag_id, 0)))
            .collect();
        tree.commit(actions).await.unwrap();
        let expected = tree.materialize().await.unwrap();
        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();

        let actual = opened
            .iter_fragments()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(actual, expected);
        assert!(actual.windows(2).all(|pair| pair[0].id < pair[1].id));
    }

    #[tokio::test]
    async fn counts_without_io() {
        let io = IOTracker::default();
        let (mut tree, _, object_store, base, scheduler, cache) = tracked_lazy_tree(&io).await;
        let mut added = make_fragment(400);
        added.physical_rows = Some(7);
        tree.commit(vec![
            action::remove_fragment(0),
            action::add_fragment(&added),
        ])
        .await
        .unwrap();
        io.incremental_stats();

        let opened = BeTree::open(object_store, base, scheduler, cache)
            .await
            .unwrap();
        io.incremental_stats();
        assert_eq!(opened.count_fragments(), 400);
        assert_eq!(opened.count_rows(), 406);
        assert_eq!(
            io.incremental_stats().read_iops,
            0,
            "aggregate access should not read child nodes"
        );

        let fragments = opened.materialize().await.unwrap();
        assert_eq!(opened.count_fragments(), fragments.len() as u64);
        assert_eq!(
            opened.count_rows(),
            fragments
                .iter()
                .map(|fragment| fragment.physical_rows.unwrap_or(0) as u64)
                .sum::<u64>()
        );
    }

    #[tokio::test]
    async fn dataset_append_uses_known_aggregate_delta_without_child_reads() {
        let io = IOTracker::default();
        let (object_store, base, scheduler, cache) = tracked_test_env(&io).await;
        let (mut dataset, _) = BeTreeDataset::create(
            MANIFEST_LAYOUT_BETREE,
            object_store,
            base,
            scheduler,
            cache,
            BeTreeConfig::new(4 * 1024, 4),
            (0..400).map(make_fragment).collect(),
            Vec::new(),
        )
        .await
        .unwrap();
        io.incremental_stats();

        let mut fragment = make_fragment(400);
        fragment.physical_rows = Some(7);
        dataset.append(&[fragment]).await.unwrap();
        let append_io = io.incremental_stats();

        assert_eq!(
            append_io.read_iops, 0,
            "fresh-id append should not read a child solely to maintain aggregates"
        );
        assert_eq!(dataset.count_fragments(), 401);
        assert_eq!(dataset.count_rows(), 407);
    }
}
