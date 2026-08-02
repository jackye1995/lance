// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! PROTOTYPE (discussion #7499): the recursive, self-balancing Bε-tree.
//!
//! Algorithm per `LITERATURE.md`: messages are tagged with a monotonic msn and
//! buffered at the root; a full node flushes the batch destined for its fullest
//! child (gated at `min_flush`), recursing down; a node that overflows splits
//! (root split grows height); underflowing children coalesce with a sibling
//! (root with one child shrinks height). Nodes are immutable — every touched
//! node on the root→leaf path is rewritten (copy-on-write) and the root repoint
//! is the commit.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use futures::Stream;
use futures::future::BoxFuture;

use crate::betree::node::{self, BeTreeConfig, InternalNode};
use crate::betree::store::NodeStore;
use crate::format::Fragment;
use crate::format::pb;
use lance_core::cache::LanceCache;
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use object_store::path::Path;
use roaring::RoaringBitmap;

/// Accumulated write work over one commit (for stats / benchmark accounting).
#[derive(Debug, Default, Clone, Copy)]
struct WriteAcc {
    io_bytes: u64,
    flushes: u64,
    splits: u64,
    merges: u64,
    /// Deepest tree level at which a flush occurred this commit (0 = root only).
    /// >0 proves multi-level flushing (internal ε-buffers filled and flushed down).
    max_flush_depth: u32,
}

impl WriteAcc {
    fn add(&mut self, o: Self) {
        self.io_bytes += o.io_bytes;
        self.flushes += o.flushes;
        self.splits += o.splits;
        self.merges += o.merges;
        self.max_flush_depth = self.max_flush_depth.max(o.max_flush_depth);
    }
}

/// Result of flushing an internal node: (possibly split) children, residual
/// buffer, and accumulated write work.
type FlushResult = (Vec<pb::ChildRef>, Vec<pb::TaggedAction>, WriteAcc);
/// Result of ingesting into a subtree: the child ref(s) that now represent it
/// (>1 if it split), and accumulated write work.
type IngestResult = (Vec<pb::ChildRef>, WriteAcc);

/// Bytes/structure written while bootstrapping.
#[derive(Debug, Default, Clone, Copy)]
pub struct BootstrapStats {
    pub io_write_bytes: u64,
    pub num_leaves: u64,
    pub height: u32,
}

/// Result of one commit.
#[derive(Debug, Default, Clone, Copy)]
pub struct CommitStats {
    /// Root, internal-node, and leaf bytes written by this commit.
    pub tree_write_bytes: u64,
    /// Transaction-record bytes written by this commit.
    pub transaction_bytes: u64,
    /// Root-delta bytes written by this commit (delta-chain mode only).
    pub delta_bytes: u64,
    /// 1 when this commit folded an outstanding delta chain into a compacted
    /// root, 0 otherwise. Always 0 with the delta chain disabled.
    pub folds: u64,
    /// Outstanding root deltas after this commit.
    pub delta_tail: u32,
    pub flushes: u64,
    pub splits: u64,
    pub merges: u64,
    pub height: u32,
    /// Deepest level flushed this commit (0 = root buffer only; ≥1 = cascaded
    /// into internal nodes — the deep-flush regime).
    pub max_flush_depth: u32,
}

impl CommitStats {
    /// Everything this commit put in the object store: tree, transaction,
    /// and root delta.
    pub fn total_bytes(&self) -> u64 {
        self.tree_write_bytes + self.transaction_bytes + self.delta_bytes
    }
}

/// Operation kind recorded in a commit's transaction record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnOperation {
    Append,
    AddColumns,
    SetDeletionFiles,
    Delete,
    DropDataFiles,
    ReplaceDataFiles,
    Overwrite,
    Restore,
    /// Pretranslated actions committed without a higher-level intent.
    Actions,
}

impl TxnOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::AddColumns => "add_columns",
            Self::SetDeletionFiles => "set_deletion_files",
            Self::Delete => "delete",
            Self::DropDataFiles => "drop_data_files",
            Self::ReplaceDataFiles => "replace_data_files",
            Self::Overwrite => "overwrite",
            Self::Restore => "restore",
            Self::Actions => "actions",
        }
    }
}

/// Objects reclaimed by an orphan-GC pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    /// Atomically published roots treated as live history.
    pub roots_scanned: u64,
    /// Node and leaf objects examined.
    pub objects_scanned: u64,
    /// Node and leaf objects not reachable from any published root.
    pub objects_deleted: u64,
}

/// A writer session over a recursive Bε-tree. Holds only the root (child refs +
/// ε-buffer + metadata) in memory; interior/leaf nodes are read on demand.
pub struct BeTree {
    store: NodeStore,
    config: BeTreeConfig,
    version: u64,
    children: Vec<pb::ChildRef>,
    buffer: Vec<pb::TaggedAction>,
    next_msn: u64,
    schema_pb: Vec<u8>,
    total_fragments: u64,
    total_rows: u64,
    /// Version of the compacted root the current delta chain extends.
    base_root_version: u64,
    /// Outstanding root deltas since `base_root_version`.
    delta_tail: u32,
}

#[derive(Clone)]
struct MutableState {
    version: u64,
    children: Vec<pb::ChildRef>,
    buffer: Vec<pb::TaggedAction>,
    next_msn: u64,
    total_fragments: u64,
    total_rows: u64,
    base_root_version: u64,
    delta_tail: u32,
}

impl BeTree {
    /// Bootstrap a balanced tree from a full fragment list (held in memory).
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
        config: BeTreeConfig,
        mut fragments: Vec<Fragment>,
        schema_pb: Vec<u8>,
    ) -> Result<(Self, BootstrapStats)> {
        if fragments.is_empty() {
            return Err(Error::invalid_input(
                "BeTree::bootstrap requires at least one fragment",
            ));
        }
        fragments.sort_by_key(|f| f.id);
        let n = fragments.len() as u64;
        let mut iter = fragments.into_iter();
        Self::bootstrap_generate(
            object_store,
            base,
            scheduler,
            cache,
            config,
            n,
            move |_| iter.next().unwrap(),
            schema_pb,
        )
        .await
    }

    /// Bootstrap by *streaming* `num_fragments` fragments from `gen` (called with
    /// ids 0..num_fragments, in order), packing them into ~0.5×-max_leaf_bytes leaves without
    /// ever holding the whole fragment list — this is what lets the tree reach
    /// billion-data-file scale (fat fragments) within a bounded memory budget.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap_generate<F: FnMut(u64) -> Fragment>(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
        config: BeTreeConfig,
        num_fragments: u64,
        mut gen_fn: F,
        schema_pb: Vec<u8>,
    ) -> Result<(Self, BootstrapStats)> {
        if num_fragments == 0 {
            return Err(Error::invalid_input(
                "BeTree::bootstrap requires at least one fragment",
            ));
        }
        let store = NodeStore::new(object_store, base, scheduler, cache);
        let target = config.leaf_split_piece_bytes();

        let mut io = 0u64;
        let mut layer: Vec<pb::ChildRef> = Vec::new();
        let mut buf: Vec<Fragment> = Vec::new();
        let mut buf_bytes = 0u64;
        let mut total_rows = 0u64;
        for id in 0..num_fragments {
            let f = gen_fn(id);
            total_rows = total_rows
                .checked_add(f.physical_rows.unwrap_or(0) as u64)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Bε-tree total_rows overflow while bootstrapping fragment id={}",
                        f.id
                    ))
                })?;
            buf_bytes += node::fragment_logical_bytes(&f);
            buf.push(f);
            if buf_bytes >= target {
                let w = store.write_leaf(&buf).await?;
                io += w.io_bytes;
                layer.push(w.child_ref);
                buf.clear();
                buf_bytes = 0;
            }
        }
        if !buf.is_empty() {
            let w = store.write_leaf(&buf).await?;
            io += w.io_bytes;
            layer.push(w.child_ref);
        }
        let num_leaves = layer.len() as u64;

        // Internal layers until the top fits under max_children_per_node.
        while layer.len() as u32 > config.max_children_per_node {
            let mut next: Vec<pb::ChildRef> = Vec::new();
            for group in layer.chunks(config.max_children_per_node as usize) {
                let w = store.write_internal(group.to_vec(), Vec::new()).await?;
                io += w.io_bytes;
                next.push(w.child_ref);
            }
            layer = next;
        }
        let height = layer.iter().map(|c| c.height).max().unwrap_or(0) + 1;

        let tree = Self {
            store,
            config,
            version: 1,
            children: layer,
            buffer: Vec::new(),
            next_msn: 1,
            schema_pb,
            total_fragments: num_fragments,
            total_rows,
            base_root_version: 1,
            delta_tail: 0,
        };
        io += tree.write_root().await?;
        Ok((
            tree,
            BootstrapStats {
                io_write_bytes: io,
                num_leaves,
                height,
            },
        ))
    }

    pub fn height(&self) -> u32 {
        self.children.iter().map(|c| c.height).max().unwrap_or(0) + 1
    }

    /// Latest atomically published version held by this session.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The opaque schema payload stored at bootstrap, exactly as supplied.
    pub fn schema_bytes(&self) -> &[u8] {
        &self.schema_pb
    }

    /// Read the transaction record published with `version`.
    pub async fn read_transaction(&self, version: u64) -> Result<pb::BeTreeTransaction> {
        self.store.read_transaction(version).await
    }

    /// Whether a transaction record was published with `version`.
    pub async fn transaction_exists(&self, version: u64) -> Result<bool> {
        self.store.transaction_exists(version).await
    }

    /// Number of logical fragments, including buffered adds and removes.
    ///
    /// This reads root metadata only and performs no object-store IO.
    pub fn count_fragments(&self) -> u64 {
        self.total_fragments
    }

    /// Sum of physical rows across logical fragments.
    ///
    /// Fragments whose physical row count is unknown contribute zero. This
    /// reads root metadata only and performs no object-store IO.
    pub fn count_rows(&self) -> u64 {
        self.total_rows
    }

    /// Number of actions currently buffered directly in the in-memory root.
    pub fn root_buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Number of direct child references currently held by the in-memory root.
    pub fn root_child_count(&self) -> usize {
        self.children.len()
    }

    #[cfg(test)]
    pub(crate) fn root_children_for_testing(&self) -> &[pb::ChildRef] {
        &self.children
    }

    fn to_pb_root(&self) -> pb::BeTreeRoot {
        pb::BeTreeRoot {
            version: self.version,
            children: self.children.clone(),
            buffer: self.buffer.clone(),
            next_msn: self.next_msn,
            schema_pb: self.schema_pb.clone(),
            max_node_bytes: self.config.max_node_bytes,
            max_children_per_node: self.config.max_children_per_node,
            total_fragments: self.total_fragments,
            total_rows: self.total_rows,
            max_leaf_bytes: self.config.max_leaf_bytes,
            base_root_version: 0,
            max_root_delta_tail: self.config.max_root_delta_tail,
            operation: String::new(),
        }
    }

    /// A root delta carrying only this commit's tagged actions. Published at
    /// the same versioned root path as a compacted root, so the create-only
    /// CAS race is identical, and it doubles as the commit's transaction
    /// record via the operation label.
    fn to_pb_root_delta(
        &self,
        tagged: Vec<pb::TaggedAction>,
        operation: TxnOperation,
    ) -> pb::BeTreeRoot {
        pb::BeTreeRoot {
            version: self.version,
            children: Vec::new(),
            buffer: tagged,
            next_msn: self.next_msn,
            schema_pb: Vec::new(),
            max_node_bytes: self.config.max_node_bytes,
            max_children_per_node: self.config.max_children_per_node,
            total_fragments: self.total_fragments,
            total_rows: self.total_rows,
            max_leaf_bytes: self.config.max_leaf_bytes,
            base_root_version: self.base_root_version,
            max_root_delta_tail: self.config.max_root_delta_tail,
            operation: operation.as_str().to_string(),
        }
    }

    async fn write_root(&self) -> Result<u64> {
        self.store.write_root(&self.to_pb_root()).await
    }

    /// Inject actions into the root buffer (msn-tagged), flush/split/rebalance,
    /// and copy-on-write the touched path + a new root.
    pub async fn commit(&mut self, actions: Vec<pb::FragmentAction>) -> Result<CommitStats> {
        self.commit_as(TxnOperation::Actions, actions).await
    }

    /// Append freshly allocated fragments without resolving existing ones.
    ///
    /// Fragment ids must be new to the tree, the same precondition Lance's
    /// production append planner guarantees. That makes each aggregate delta
    /// `(1, physical_rows)` without a root-to-leaf lookup.
    pub async fn commit_append(&mut self, fragments: &[Fragment]) -> Result<CommitStats> {
        let mut actions = Vec::with_capacity(fragments.len());
        let mut aggregate_deltas = Vec::with_capacity(fragments.len());
        let mut fragment_ids = HashSet::with_capacity(fragments.len());
        for fragment in fragments {
            if !fragment_ids.insert(fragment.id) {
                return Err(Error::invalid_input(format!(
                    "append contains duplicate fragment id: fragment_id={}",
                    fragment.id
                )));
            }
            let physical_rows =
                i64::try_from(fragment.physical_rows.unwrap_or(0)).map_err(|_| {
                    Error::invalid_input(format!(
                        "appended fragment physical_rows does not fit aggregate delta: \
                         fragment_id={}, physical_rows={:?}",
                        fragment.id, fragment.physical_rows
                    ))
                })?;
            actions.push(crate::betree::action::add_fragment(fragment));
            aggregate_deltas.push((1, physical_rows));
        }
        self.commit_with_aggregate_deltas(TxnOperation::Append, actions, aggregate_deltas)
            .await
    }

    /// Commit actions and record `operation` in the transaction record.
    pub async fn commit_as(
        &mut self,
        operation: TxnOperation,
        actions: Vec<pb::FragmentAction>,
    ) -> Result<CommitStats> {
        let aggregate_deltas = self.action_aggregate_deltas(&actions).await?;
        self.commit_with_aggregate_deltas(operation, actions, aggregate_deltas)
            .await
    }

    /// Commit actions whose exact fragment-count and row-count deltas are known
    /// by the transaction layer.
    ///
    /// This avoids point reads solely for aggregate maintenance. Callers must
    /// derive the deltas from operation semantics that already enforce the
    /// corresponding precondition (for example, append allocates fresh fragment
    /// ids and therefore contributes `(1, physical_rows)` per fragment).
    pub(crate) async fn commit_with_aggregate_deltas(
        &mut self,
        operation: TxnOperation,
        actions: Vec<pb::FragmentAction>,
        aggregate_deltas: Vec<(i64, i64)>,
    ) -> Result<CommitStats> {
        if actions.len() != aggregate_deltas.len() {
            return Err(Error::invalid_input(format!(
                "Bε-tree action/delta length mismatch: actions={}, aggregate_deltas={}",
                actions.len(),
                aggregate_deltas.len()
            )));
        }
        let previous = self.mutable_state();
        let result = self
            .commit_inner(operation, actions, aggregate_deltas)
            .await;
        if result.is_err() {
            self.restore_mutable_state(previous);
        }
        result
    }

    fn mutable_state(&self) -> MutableState {
        MutableState {
            version: self.version,
            children: self.children.clone(),
            buffer: self.buffer.clone(),
            next_msn: self.next_msn,
            total_fragments: self.total_fragments,
            total_rows: self.total_rows,
            base_root_version: self.base_root_version,
            delta_tail: self.delta_tail,
        }
    }

    fn restore_mutable_state(&mut self, state: MutableState) {
        self.version = state.version;
        self.children = state.children;
        self.buffer = state.buffer;
        self.next_msn = state.next_msn;
        self.total_fragments = state.total_fragments;
        self.base_root_version = state.base_root_version;
        self.delta_tail = state.delta_tail;
        self.total_rows = state.total_rows;
    }

    async fn commit_inner(
        &mut self,
        operation: TxnOperation,
        actions: Vec<pb::FragmentAction>,
        aggregate_deltas: Vec<(i64, i64)>,
    ) -> Result<CommitStats> {
        let action_count = u64::try_from(actions.len()).map_err(|_| {
            Error::invalid_input(format!(
                "Bε-tree action count does not fit u64: {}",
                actions.len()
            ))
        })?;
        let next_msn = self.next_msn.checked_add(action_count).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree msn overflow: next_msn={}, action_count={action_count}",
                self.next_msn
            ))
        })?;
        let fragment_count_delta = sum_aggregate_deltas(
            aggregate_deltas
                .iter()
                .map(|(fragment_delta, _)| *fragment_delta),
            "fragment_count_delta",
        )?;
        let total_rows_delta = sum_aggregate_deltas(
            aggregate_deltas.iter().map(|(_, row_delta)| *row_delta),
            "total_rows_delta",
        )?;
        let total_fragments = apply_aggregate_delta(
            self.total_fragments,
            fragment_count_delta,
            "total_fragments",
        )?;
        let total_rows = apply_aggregate_delta(self.total_rows, total_rows_delta, "total_rows")?;
        let transaction = pb::BeTreeTransaction {
            version: self.version + 1,
            base_version: self.version,
            operation: operation.as_str().to_string(),
            actions: actions.clone(),
        };

        let mut tagged = Vec::with_capacity(actions.len());
        for (offset, (action, (fragment_count_delta, total_rows_delta))) in
            actions.into_iter().zip(aggregate_deltas).enumerate()
        {
            tagged.push(pb::TaggedAction {
                msn: self.next_msn + offset as u64,
                action: Some(action),
                fragment_count_delta,
                total_rows_delta,
            });
        }
        self.buffer.extend(tagged.iter().cloned());
        self.next_msn = next_msn;
        self.total_fragments = total_fragments;
        self.total_rows = total_rows;
        self.version += 1;

        // Delta-chain fast path: while no tree work is due, publish only this
        // commit's actions as a root delta and defer flush/split/merge/shrink
        // to the fold. The gates are conservative: any commit that could make
        // the pipeline touch children folds instead, because a delta reader
        // reconstructs state as compacted root plus appended actions and must
        // never observe restructured children or a reordered buffer.
        let delta_chain = self.config.max_root_delta_tail > 0;
        if delta_chain
            && self.delta_tail < self.config.max_root_delta_tail
            && !node::internal_overflows(&self.children, &self.buffer, &self.config)
            && node::internal_logical_bytes(&self.children, &self.buffer)
                < self.config.split_ceiling()
        {
            let delta_bytes = self
                .store
                .write_root(&self.to_pb_root_delta(tagged, operation))
                .await?;
            self.delta_tail += 1;
            return Ok(CommitStats {
                delta_bytes,
                delta_tail: self.delta_tail,
                height: self.height(),
                ..Default::default()
            });
        }

        let mut acc = WriteAcc::default();

        // Flush the root buffer down as far as it will go (root is depth 0).
        let children = std::mem::take(&mut self.children);
        let buffer = std::mem::take(&mut self.buffer);
        let (children, buffer, a) = self.flush_internal(children, buffer, 0).await?;
        acc.add(a);
        self.children = children;
        self.buffer = buffer;

        // Grow: if the root still overflows, split it and lift a new root over the pieces.
        if node::internal_overflows(&self.children, &self.buffer, &self.config) {
            let pieces = node::split_internal(
                std::mem::take(&mut self.children),
                std::mem::take(&mut self.buffer),
                self.config.split_piece_bytes(),
                self.config.max_children_per_node,
            );
            let mut new_children = Vec::with_capacity(pieces.len());
            for (ch, buf) in pieces {
                let w = self.store.write_internal(ch, buf).await?;
                acc.io_bytes += w.io_bytes;
                new_children.push(w.child_ref);
            }
            self.children = new_children;
            acc.splits += 1;
        }

        // Coalesce underflowing children (self-balancing on deletes).
        let children = std::mem::take(&mut self.children);
        let (children, a) = self.merge_small_children(children).await?;
        acc.add(a);
        self.children = children;

        // Shrink: a root with a single internal child pulls that child up.
        self.maybe_shrink_root().await?;

        acc.io_bytes += self.write_root().await?;
        let folds = if delta_chain { 1 } else { 0 };
        self.base_root_version = self.version;
        self.delta_tail = 0;
        // The published root is the commit point. Only the create-only winner
        // reaches this write, so a conflicted commit never publishes a
        // transaction record.
        let transaction_bytes = self.store.write_transaction(&transaction).await?;
        Ok(CommitStats {
            tree_write_bytes: acc.io_bytes,
            transaction_bytes,
            delta_bytes: 0,
            folds,
            delta_tail: 0,
            flushes: acc.flushes,
            splits: acc.splits,
            merges: acc.merges,
            height: self.height(),
            max_flush_depth: acc.max_flush_depth,
        })
    }

    async fn action_aggregate_deltas(
        &self,
        actions: &[pb::FragmentAction],
    ) -> Result<Vec<(i64, i64)>> {
        let mut states: HashMap<u64, Option<Fragment>> = HashMap::new();
        let mut deltas = Vec::with_capacity(actions.len());

        for fragment_action in actions {
            let Some(action) = fragment_action.action.as_ref() else {
                deltas.push((0, 0));
                continue;
            };
            let (frag_id, new_fragment) = match action {
                pb::fragment_action::Action::AddFragment(fragment) => {
                    (fragment.id, Some(Fragment::try_from(fragment.clone())?))
                }
                pb::fragment_action::Action::RemoveFragment(frag_id) => (*frag_id, None),
                _ => {
                    deltas.push((0, 0));
                    continue;
                }
            };

            if let Entry::Vacant(entry) = states.entry(frag_id) {
                entry.insert(self.resolve_fragment(frag_id).await?);
            }
            let previous = states.get(&frag_id).and_then(Option::as_ref);
            let previous_count = i64::from(previous.is_some());
            let new_count = i64::from(new_fragment.is_some());
            let previous_rows = fragment_physical_rows(previous)?;
            let new_rows = fragment_physical_rows(new_fragment.as_ref())?;
            deltas.push((new_count - previous_count, new_rows - previous_rows));
            states.insert(frag_id, new_fragment);
        }

        Ok(deltas)
    }

    /// Flush an internal node's buffer to its children while it overflows,
    /// picking the fullest child each round (gated at `min_flush`). `depth` is
    /// this node's level below the root (0 = root). Returns the (possibly split)
    /// children and the residual buffer.
    fn flush_internal(
        &self,
        mut children: Vec<pb::ChildRef>,
        mut buffer: Vec<pb::TaggedAction>,
        depth: u32,
    ) -> BoxFuture<'_, Result<FlushResult>> {
        Box::pin(async move {
            let mut acc = WriteAcc::default();
            loop {
                if node::internal_logical_bytes(&children, &buffer) < self.config.split_ceiling() {
                    break;
                }
                let mut buckets = node::partition_buffer_by_child(&children, buffer);
                // Fullest child by buffered bytes.
                let (idx, best) = buckets
                    .iter()
                    .enumerate()
                    .map(|(i, b)| (i, node::buffer_bytes(b)))
                    .max_by_key(|(_, b)| *b)
                    .unwrap_or((0, 0));
                if best < self.config.min_flush_bytes() {
                    // No child worth flushing to — reassemble and stop (caller may split).
                    buffer = buckets.into_iter().flatten().collect();
                    break;
                }
                let chosen = std::mem::take(&mut buckets[idx]);
                buffer = buckets.into_iter().flatten().collect();

                let (new_refs, a) = self.ingest(children[idx].clone(), chosen, depth).await?;
                acc.add(a);
                acc.flushes += 1;
                acc.max_flush_depth = acc.max_flush_depth.max(depth);
                children.splice(idx..idx + 1, new_refs);
            }
            Ok((children, buffer, acc))
        })
    }

    /// Push `incoming` messages into the subtree rooted at `child` (at `depth`
    /// below the root); apply at a leaf, recurse+buffer at an internal node;
    /// split on overflow. Returns the child ref(s) that now represent the subtree.
    fn ingest(
        &self,
        child: pb::ChildRef,
        incoming: Vec<pb::TaggedAction>,
        depth: u32,
    ) -> BoxFuture<'_, Result<IngestResult>> {
        Box::pin(async move {
            let mut acc = WriteAcc::default();
            if child.height == 0 {
                // Leaf: apply messages, then split if it overflows.
                let fragments = self.store.read_leaf(&child).await?;
                let mut map: BTreeMap<u64, Fragment> =
                    fragments.into_iter().map(|f| (f.id, f)).collect();
                node::apply_actions(&mut map, incoming)?;
                let new_frags: Vec<Fragment> = map.into_values().collect();

                // A fully-emptied leaf is dropped from its parent — keeping it would
                // create a phantom ChildRef with min_key=0 that corrupts the
                // sorted-by-min_key invariant `child_index_for` relies on.
                if new_frags.is_empty() {
                    return Ok((vec![], acc));
                }
                if node::leaf_logical_bytes(&new_frags) >= self.config.leaf_split_ceiling() {
                    let mut refs = Vec::new();
                    for piece in
                        node::split_leaf_fragments(new_frags, self.config.leaf_split_piece_bytes())
                    {
                        let w = self.store.write_leaf(&piece).await?;
                        acc.io_bytes += w.io_bytes;
                        refs.push(w.child_ref);
                    }
                    acc.splits += 1;
                    Ok((refs, acc))
                } else {
                    let w = self.store.write_leaf(&new_frags).await?;
                    acc.io_bytes += w.io_bytes;
                    Ok((vec![w.child_ref], acc))
                }
            } else {
                // Internal: buffer, recurse-flush, split if it overflows.
                let InternalNode {
                    children,
                    mut buffer,
                } = self.store.read_internal(&child).await?;
                buffer.extend(incoming);
                // This node is one level deeper than the parent that flushed to it.
                let (children, buffer, a) =
                    self.flush_internal(children, buffer, depth + 1).await?;
                acc.add(a);
                // Rebalance: coalesce any underflowing children before checking split.
                let (children, a) = self.merge_small_children(children).await?;
                acc.add(a);

                // An internal node whose children all vanished is dropped too (same
                // phantom-min_key=0 hazard as an empty leaf).
                if children.is_empty() {
                    return Ok((vec![], acc));
                }
                if node::internal_overflows(&children, &buffer, &self.config) {
                    let mut refs = Vec::new();
                    for (ch, buf) in node::split_internal(
                        children,
                        buffer,
                        self.config.split_piece_bytes(),
                        self.config.max_children_per_node,
                    ) {
                        let w = self.store.write_internal(ch, buf).await?;
                        acc.io_bytes += w.io_bytes;
                        refs.push(w.child_ref);
                    }
                    acc.splits += 1;
                    Ok((refs, acc))
                } else {
                    let w = self.store.write_internal(children, buffer).await?;
                    acc.io_bytes += w.io_bytes;
                    Ok((vec![w.child_ref], acc))
                }
            }
        })
    }

    /// Coalesce runs of adjacent children when one underflows (leaf ≤ 0.25 B;
    /// internal < max_children_per_node/4 children), bounded so the merged node stays valid
    /// (leaves ≤ 0.6 B; internal ≤ max_children_per_node children). Reads/writes the merged
    /// node(s). Leaves concat fragments; internal nodes concat children + buffers.
    async fn merge_small_children(
        &self,
        children: Vec<pb::ChildRef>,
    ) -> Result<(Vec<pb::ChildRef>, WriteAcc)> {
        let mut acc = WriteAcc::default();
        let mut out: Vec<pb::ChildRef> = Vec::with_capacity(children.len());
        let mut i = 0;
        while i < children.len() {
            if !node::is_underflow(&children[i], &self.config) {
                out.push(children[i].clone());
                i += 1;
                continue;
            }
            // Grow a coalesce group with adjacent siblings, bounded by node kind.
            let is_leaf = children[i].height == 0;
            let mut group = vec![children[i].clone()];
            let mut bytes = children[i].byte_size;
            let mut fan = children[i].num_children;
            let mut j = i + 1;
            while j < children.len() {
                let c = &children[j];
                let fits = if is_leaf {
                    bytes + c.byte_size <= self.config.leaf_coalesce_ceiling()
                } else {
                    fan + c.num_children <= self.config.max_children_per_node
                        && bytes + c.byte_size <= self.config.coalesce_ceiling()
                };
                if !fits {
                    break;
                }
                bytes += c.byte_size;
                fan += c.num_children;
                group.push(c.clone());
                j += 1;
            }
            if group.len() == 1 {
                out.push(group.pop().unwrap());
            } else {
                let (merged, a) = self.coalesce(group).await?;
                acc.add(a);
                acc.merges += 1;
                out.push(merged);
            }
            i = j;
        }
        Ok((out, acc))
    }

    /// Combine an adjacent group of same-height children into one node.
    async fn coalesce(&self, group: Vec<pb::ChildRef>) -> Result<(pb::ChildRef, WriteAcc)> {
        let mut acc = WriteAcc::default();
        if group[0].height == 0 {
            let mut fragments: Vec<Fragment> = Vec::new();
            for c in &group {
                fragments.extend(self.store.read_leaf(c).await?);
            }
            fragments.sort_by_key(|f| f.id);
            let w = self.store.write_leaf(&fragments).await?;
            acc.io_bytes += w.io_bytes;
            Ok((w.child_ref, acc))
        } else {
            let mut children: Vec<pb::ChildRef> = Vec::new();
            let mut buffer: Vec<pb::TaggedAction> = Vec::new();
            for c in &group {
                let node = self.store.read_internal(c).await?;
                children.extend(node.children);
                buffer.extend(node.buffer);
            }
            let w = self.store.write_internal(children, buffer).await?;
            acc.io_bytes += w.io_bytes;
            Ok((w.child_ref, acc))
        }
    }

    /// If the root has a single internal child, pull that child's children/buffer
    /// up into the root (height −1).
    async fn maybe_shrink_root(&mut self) -> Result<()> {
        while self.children.len() == 1 && self.children[0].height > 0 {
            let node = self.store.read_internal(&self.children[0]).await?;
            let mut buffer = node.buffer;
            buffer.extend(self.buffer.iter().cloned());
            if node::internal_overflows(&node.children, &buffer, &self.config) {
                break;
            }
            self.children = node.children;
            self.buffer = buffer;
        }
        Ok(())
    }

    /// Materialize the full fragment list: collect all leaf fragments + every
    /// buffered action in the tree, then overlay actions newest-wins.
    pub async fn materialize(&self) -> Result<Vec<Fragment>> {
        let mut map: BTreeMap<u64, Fragment> = BTreeMap::new();
        let mut actions: Vec<pb::TaggedAction> = self.buffer.clone();
        self.collect(&self.children, &mut map, &mut actions).await?;
        node::apply_actions(&mut map, actions)?;
        Ok(map.into_values().collect())
    }

    /// Resolve one fragment by loading only the root-to-leaf path.
    ///
    /// Buffered actions from every node on the path are applied with the same
    /// msn ordering as [`Self::materialize`].
    pub async fn resolve_fragment(&self, frag_id: u64) -> Result<Option<Fragment>> {
        let mut actions: Vec<pb::TaggedAction> = self
            .buffer
            .iter()
            .filter(|tagged| node::action_key(tagged) == frag_id)
            .cloned()
            .collect();
        let mut children = self.children.clone();
        let mut fragment = None;

        while !children.is_empty() {
            let child = children[node::child_index_for(&children, frag_id)].clone();
            if child.height == 0 {
                fragment = self
                    .store
                    .read_leaf(&child)
                    .await?
                    .into_iter()
                    .find(|candidate| candidate.id == frag_id);
                break;
            }

            let internal = self.store.read_internal(&child).await?;
            actions.extend(
                internal
                    .buffer
                    .into_iter()
                    .filter(|tagged| node::action_key(tagged) == frag_id),
            );
            children = internal.children;
        }

        let mut fragments = BTreeMap::new();
        if let Some(fragment) = fragment {
            fragments.insert(fragment.id, fragment);
        }
        node::apply_actions(&mut fragments, actions)?;
        Ok(fragments.remove(&frag_id))
    }

    /// Resolve an index fragment bitmap while loading only covering subtrees.
    ///
    /// Each covering internal node or leaf is loaded at most once, so a batch
    /// of nearby fragment ids avoids repeating the root-to-leaf traversal used
    /// by independent [`Self::resolve_fragment`] calls.
    pub async fn resolve_fragments(&self, fragment_ids: &RoaringBitmap) -> Result<Vec<Fragment>> {
        let mut fragments = BTreeMap::new();
        let mut actions = self
            .buffer
            .iter()
            .filter(|tagged| bitmap_contains(fragment_ids, node::action_key(tagged)))
            .cloned()
            .collect();
        self.collect_selected(
            self.children.clone(),
            0,
            None,
            fragment_ids,
            &mut fragments,
            &mut actions,
        )
        .await?;
        node::apply_actions(&mut fragments, actions)?;
        fragments.retain(|fragment_id, _| bitmap_contains(fragment_ids, *fragment_id));
        Ok(fragments.into_values().collect())
    }

    /// Stream fragments in id order while retaining at most one materialized
    /// leaf plus the buffered actions for not-yet-visited subtrees.
    pub fn iter_fragments(&self) -> impl Stream<Item = Result<Fragment>> + '_ {
        let mut stack = Vec::new();
        let mut ready = VecDeque::new();
        let mut initial_error = None;
        if self.children.is_empty() {
            let mut fragments = BTreeMap::new();
            match node::apply_actions(&mut fragments, self.buffer.clone()) {
                Ok(()) => ready.extend(fragments.into_values()),
                Err(error) => initial_error = Some(error),
            }
        } else {
            let action_buckets =
                node::partition_buffer_by_child(&self.children, self.buffer.clone());
            for (child, actions) in self.children.iter().cloned().zip(action_buckets).rev() {
                stack.push((child, actions));
            }
        }

        futures::stream::try_unfold(
            (stack, ready, initial_error),
            move |(mut stack, mut ready, initial_error)| async move {
                if let Some(error) = initial_error {
                    return Err(error);
                }
                loop {
                    if let Some(fragment) = ready.pop_front() {
                        return Ok(Some((fragment, (stack, ready, None))));
                    }
                    let Some((child, mut actions)) = stack.pop() else {
                        return Ok(None);
                    };
                    if child.height == 0 {
                        let fragments = self.store.read_leaf(&child).await?;
                        let mut fragments: BTreeMap<u64, Fragment> = fragments
                            .into_iter()
                            .map(|fragment| (fragment.id, fragment))
                            .collect();
                        node::apply_actions(&mut fragments, actions)?;
                        ready.extend(fragments.into_values());
                        continue;
                    }

                    let internal = self.store.read_internal(&child).await?;
                    actions.extend(internal.buffer);
                    if internal.children.is_empty() {
                        let mut fragments = BTreeMap::new();
                        node::apply_actions(&mut fragments, actions)?;
                        ready.extend(fragments.into_values());
                        continue;
                    }
                    let action_buckets =
                        node::partition_buffer_by_child(&internal.children, actions);
                    for (child, actions) in internal.children.into_iter().zip(action_buckets).rev()
                    {
                        stack.push((child, actions));
                    }
                }
            },
        )
    }

    /// Walk the tree and collect `(height, logical_bytes)` for every internal node
    /// (root included). Used to measure how full internal ε-buffers are — a node
    /// near `B` is holding a big buffer, a node near its ref-only size is "cold".
    pub async fn internal_node_sizes(&self) -> Result<Vec<(u32, u64)>> {
        let mut out = vec![(
            self.height(),
            node::internal_logical_bytes(&self.children, &self.buffer),
        )];
        self.collect_internal_sizes(self.children.clone(), &mut out)
            .await?;
        Ok(out)
    }

    fn collect_internal_sizes<'a>(
        &'a self,
        children: Vec<pb::ChildRef>,
        out: &'a mut Vec<(u32, u64)>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for c in &children {
                if c.height > 0 {
                    out.push((c.height, c.byte_size));
                    let node = self.store.read_internal(c).await?;
                    self.collect_internal_sizes(node.children, out).await?;
                }
            }
            Ok(())
        })
    }

    fn collect<'a>(
        &'a self,
        children: &'a [pb::ChildRef],
        frags: &'a mut BTreeMap<u64, Fragment>,
        actions: &'a mut Vec<pb::TaggedAction>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for child in children {
                if child.height == 0 {
                    for f in self.store.read_leaf(child).await? {
                        frags.insert(f.id, f);
                    }
                } else {
                    let node = self.store.read_internal(child).await?;
                    actions.extend(node.buffer);
                    self.collect_owned(node.children, frags, actions).await?;
                }
            }
            Ok(())
        })
    }

    fn collect_owned<'a>(
        &'a self,
        children: Vec<pb::ChildRef>,
        frags: &'a mut BTreeMap<u64, Fragment>,
        actions: &'a mut Vec<pb::TaggedAction>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for child in &children {
                if child.height == 0 {
                    for f in self.store.read_leaf(child).await? {
                        frags.insert(f.id, f);
                    }
                } else {
                    let node = self.store.read_internal(child).await?;
                    actions.extend(node.buffer);
                    self.collect_owned(node.children, frags, actions).await?;
                }
            }
            Ok(())
        })
    }

    fn collect_selected<'a>(
        &'a self,
        children: Vec<pb::ChildRef>,
        lower_bound: u64,
        upper_bound: Option<u64>,
        fragment_ids: &'a RoaringBitmap,
        fragments: &'a mut BTreeMap<u64, Fragment>,
        actions: &'a mut Vec<pb::TaggedAction>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for (index, child) in children.iter().enumerate() {
                let child_lower_bound = if index == 0 {
                    lower_bound
                } else {
                    child.min_key
                };
                let child_upper_bound = children
                    .get(index + 1)
                    .map(|next| next.min_key)
                    .or(upper_bound);
                if !bitmap_intersects_range(fragment_ids, child_lower_bound, child_upper_bound) {
                    continue;
                }
                if child.height == 0 {
                    fragments.extend(
                        self.store
                            .read_leaf(child)
                            .await?
                            .into_iter()
                            .filter(|fragment| bitmap_contains(fragment_ids, fragment.id))
                            .map(|fragment| (fragment.id, fragment)),
                    );
                } else {
                    let internal = self.store.read_internal(child).await?;
                    actions.extend(
                        internal.buffer.into_iter().filter(|tagged| {
                            bitmap_contains(fragment_ids, node::action_key(tagged))
                        }),
                    );
                    self.collect_selected(
                        internal.children,
                        child_lower_bound,
                        child_upper_bound,
                        fragment_ids,
                        fragments,
                        actions,
                    )
                    .await?;
                }
            }
            Ok(())
        })
    }

    /// Delete node and leaf objects unreachable from every published root.
    ///
    /// Published roots are retained as version history. This only removes COW
    /// intermediates and files left behind by failed pre-publication commits.
    ///
    /// This is an offline operation: the caller must ensure there are no
    /// concurrent writers. A writer creates new COW objects before publishing
    /// its root, so an online mark-and-sweep cannot distinguish those objects
    /// from abandoned commit output without a lease or grace-period protocol.
    pub async fn gc_unreferenced_offline(&self) -> Result<GcStats> {
        let versions = self.store.list_root_versions().await?;
        let mut reachable = HashSet::new();
        for version in &versions {
            let root = self.store.read_root(*version).await?;
            self.collect_reachable_paths(root.children, &mut reachable)
                .await?;
        }

        let mut stats = GcStats {
            roots_scanned: versions.len() as u64,
            ..Default::default()
        };
        for kind in ["node", "leaf"] {
            for path in self.store.list_node_paths(kind).await? {
                stats.objects_scanned += 1;
                if !reachable.contains(path.as_ref()) {
                    self.store.delete_path(&path).await?;
                    stats.objects_deleted += 1;
                }
            }
        }
        Ok(stats)
    }

    fn collect_reachable_paths<'a>(
        &'a self,
        children: Vec<pb::ChildRef>,
        reachable: &'a mut HashSet<String>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for child in children {
                if !reachable.insert(child.node_path.clone()) || child.height == 0 {
                    continue;
                }
                let internal = self.store.read_internal(&child).await?;
                self.collect_reachable_paths(internal.children, reachable)
                    .await?;
            }
            Ok(())
        })
    }

    /// Open the latest tree by listing committed root versions and loading the
    /// newest root object. A delta tip additionally loads its compacted base
    /// root and the delta chain between them; child nodes and leaves remain
    /// lazy either way.
    pub async fn open(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
    ) -> Result<Self> {
        let store = NodeStore::new(object_store, base, scheduler, cache);
        let version = store.read_latest_version().await?;
        let tip = store.read_root(version).await?;
        // The tip always carries the freshest sequencing and aggregates,
        // whether it is a compacted root or a delta root.
        let next_msn = tip.next_msn;
        let total_fragments = tip.total_fragments;
        let total_rows = tip.total_rows;
        let (root, buffer, base_root_version, delta_tail) = if tip.base_root_version == 0 {
            let buffer = tip.buffer.clone();
            (tip, buffer, version, 0)
        } else {
            let compacted = store.read_root(tip.base_root_version).await?;
            if compacted.base_root_version != 0 {
                return Err(Error::invalid_input(format!(
                    "delta root {version} points at base {} which is itself a delta of {}",
                    tip.base_root_version, compacted.base_root_version
                )));
            }
            let mut buffer = compacted.buffer.clone();
            for delta_version in compacted.version + 1..version {
                let delta = store.read_root(delta_version).await?;
                if delta.base_root_version != tip.base_root_version {
                    return Err(Error::invalid_input(format!(
                        "delta root {delta_version} extends base {} but tip {version} \
                         extends base {}",
                        delta.base_root_version, tip.base_root_version
                    )));
                }
                buffer.extend(delta.buffer);
            }
            buffer.extend(tip.buffer.clone());
            let delta_tail = u32::try_from(version - compacted.version).unwrap_or(u32::MAX);
            (compacted, buffer, tip.base_root_version, delta_tail)
        };
        let max_leaf_bytes = if root.max_leaf_bytes == 0 {
            root.max_node_bytes
        } else {
            root.max_leaf_bytes
        };
        Ok(Self {
            store,
            config: BeTreeConfig::new(root.max_node_bytes, root.max_children_per_node)
                .with_max_leaf_bytes(max_leaf_bytes)
                .with_root_delta_tail(root.max_root_delta_tail),
            version,
            children: root.children,
            buffer,
            next_msn,
            schema_pb: root.schema_pb,
            total_fragments,
            total_rows,
            base_root_version,
            delta_tail,
        })
    }

    /// Open the latest tree and fully materialize it for compatibility with the
    /// original benchmark API.
    pub async fn cold_open(
        object_store: Arc<ObjectStore>,
        base: Path,
        scheduler: Arc<ScanScheduler>,
        cache: Arc<LanceCache>,
    ) -> Result<Vec<Fragment>> {
        Self::open(object_store, base, scheduler, cache)
            .await?
            .materialize()
            .await
    }
}

fn bitmap_contains(fragment_ids: &RoaringBitmap, fragment_id: u64) -> bool {
    u32::try_from(fragment_id)
        .map(|fragment_id| fragment_ids.contains(fragment_id))
        .unwrap_or(false)
}

fn bitmap_intersects_range(
    fragment_ids: &RoaringBitmap,
    lower_bound: u64,
    upper_bound: Option<u64>,
) -> bool {
    if upper_bound.is_some_and(|upper_bound| upper_bound <= lower_bound) {
        return false;
    }
    let Ok(lower_bound) = u32::try_from(lower_bound) else {
        return false;
    };
    let upper_bound = upper_bound
        .and_then(|upper_bound| u32::try_from(upper_bound).ok())
        .map(|upper_bound| upper_bound.saturating_sub(1))
        .unwrap_or(u32::MAX);
    lower_bound <= upper_bound && fragment_ids.range_cardinality(lower_bound..=upper_bound) > 0
}

fn fragment_physical_rows(fragment: Option<&Fragment>) -> Result<i64> {
    let rows = fragment
        .and_then(|fragment| fragment.physical_rows)
        .unwrap_or(0);
    i64::try_from(rows).map_err(|_| {
        Error::invalid_input(format!(
            "fragment physical_rows does not fit aggregate delta: physical_rows={rows}"
        ))
    })
}

fn apply_aggregate_delta(base: u64, delta: i64, name: &str) -> Result<u64> {
    if delta >= 0 {
        base.checked_add(delta as u64).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} aggregate overflow: base={base}, delta={delta}"
            ))
        })
    } else {
        base.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} aggregate underflow: base={base}, delta={delta}"
            ))
        })
    }
}

fn sum_aggregate_deltas(deltas: impl IntoIterator<Item = i64>, name: &str) -> Result<i64> {
    deltas.into_iter().try_fold(0i64, |sum, delta| {
        sum.checked_add(delta).ok_or_else(|| {
            Error::invalid_input(format!(
                "Bε-tree {name} overflow while summing: sum={sum}, delta={delta}"
            ))
        })
    })
}
