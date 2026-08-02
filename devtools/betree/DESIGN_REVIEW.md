# Bε design review before posting

This pass challenged the prototype instead of taking the benchmark win at face
value. It found one selective-read correctness bug, two structural accounting
hazards, and one avoidable read on append. The fixes below are implemented and
covered by tests.

## Implemented

### Routing uses fences, not live maxima

`ChildRef.min_key` and `max_key` describe live materialized contents, but an
internal ε-buffer can contain an insert beyond those live bounds. The bitmap
resolver previously pruned on `[min_key, max_key]` and could skip a subtree that
held the requested buffered insert.

Selective traversal now derives half-open routing ranges from sibling
`min_key` separators and carries the parent's outer fences recursively.
`max_key` remains a summary, not a routing fence. The regression constructs
wide gaps, forces low and high inserts below the root without reaching leaves,
and checks that bitmap resolve returns every inserted id. It failed before the
fix and passes now.

### Internal size accounting is wire-accurate

Internal split decisions previously summed child payload sizes. A parent does
not contain those payloads. It contains encoded `ChildRef` messages and its
buffer. That error could split an internal node into one-child pieces merely
because its leaves were large.

Internal logical size now equals `InternalNode::encoded_len()`. Splitting packs
adjacent child-plus-routed-message units by that encoded size and fanout. An
internal child is an underflow candidate only when both its fanout and encoded
size are small, and coalescing checks both constraints. Root shrinking also
stops if pulling up the only child would overflow the root.

### Internal buffers and leaves have separate limits

`max_node_bytes` now controls internal nodes and inline ε-buffers.
`max_leaf_bytes` independently controls leaf split/merge. It is persisted in
the unstable prototype root and defaults to `max_node_bytes`, so the original
configuration is unchanged.

The separation lets writes use small buffers while readers use larger leaf
objects. The local probe found a useful knee at 64 KiB internal / 1 MiB leaf:
full materialization at ~100K fragments fell from 136 to 16 GETs, while
contiguous trickle still wrote 119.2× less than flat. That is a tuning result,
not a format constant. Remote-store latency and scanner/cache behavior still
need measurement.

### Fresh append supplies its aggregate delta

The generic action API resolves add/remove targets to maintain exact root
counts. Append already owns a stronger precondition: its fragment ids are
freshly allocated. The Dataset adapter now supplies the known `(1,
physical_rows)` delta with each append action, rejects duplicate ids within an
append batch, and skips a child read that existed only for bookkeeping.

A tracked object-store test asserts that append performs zero child reads and
that fragment/row aggregates remain exact.

## Measured follow-up

Median of three runs, local filesystem, fabricated metadata, fanout 16:

| Workload | Bε, 64 KiB internal / 1 MiB leaf | Flat |
|---|---:|---:|
| steady append, 5K commits | 32.3 KiB / 0.472 ms | 4,393.2 KiB / 7.672 ms |
| trickle contiguous, 500 commits | 36.7 KiB / 0.516 ms | 4,372.9 KiB / 7.218 ms |
| trickle scattered, 500 commits | 70.5 KiB / 0.787 ms | 4,372.9 KiB / 7.045 ms |
| one-shot, 50K fragments | 4,146.3 KiB / 78.410 ms | 7,943.0 KiB / 14.548 ms |

At ~100K fragments, Bε opened in 2 GETs / 0.973 ms, then materialized in 16
GETs / 32.536 ms. A point resolve averaged 1 GET / 1.568 ms. The larger leaf
reduces object count but raises uncached point latency. That is the main
tradeoff to state publicly.

See [`AB_BENCH.md`](./AB_BENCH.md) and the
[raw follow-up CSV](./bench-results/split64_leaf1024_local_20260726.csv).

## Still worth testing, not silently claiming

1. **Per-node immutable message runs.** Jack already proposed an external root
   delta chain. A stronger version lets any hot internal buffer spill immutable
   keyed runs, compacted by live-byte/obsolete-byte ratio. That could remove
   repeated internal protobuf rewrites without putting every commit back on a
   global chain. It needs a read-amplification and compaction benchmark before
   becoming part of the design.
2. **Preconditioned transaction actions.** Fresh append is the first example.
   Remove-if-present, replace-if-version, and compare-by-fragment-generation
   could make conflict rebase and aggregate maintenance cheap without trusting
   arbitrary callers. The preconditions need to be persisted or checked at
   commit publication.
3. **Column-oriented overlays above fragment leaves.** Repeated add-column
   metadata could be stored as `(fragment_id, field_id) -> file` runs and joined
   during scan planning. That may reduce leaf rewrites, but it changes the
   reader and compaction model enough that it should remain a separate
   experiment, not part of the current post.

The defensible public claim is narrower than “Bε is intrinsically faster”: this
implementation wins the measured fine-grained metadata workload, the current
tiered `Merge` path serializes table-wide state, and the remaining read/write
tradeoffs are visible.
