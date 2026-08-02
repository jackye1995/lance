# Bε integration report

The tiered-manifest experiment contributed its Dataset boundary, commit
publication lessons, and access-pattern coverage to the Bε prototype. The
sealed AMT format itself was not ported. The A/B result supports keeping
recursive, byte-balanced Bε nodes and putting a thin Dataset-shaped translation
layer above them.

## What was ported

### Atomic version publication

`latest_hint.json` is gone. A root is now published at the deterministic
`_bt/root/{version}.root` path with a create-only PUT. Two writers based on the
same version race for the same next root, and exactly one can publish it. An
existing root maps to `CommitConflict`. Open lists published root records,
selects the greatest version, and reads only that root.

A failed publication restores the writer session's version, root children,
buffer, message sequence, and aggregates. Its pre-publication COW files remain
unreferenced and are reclaimed by the new offline orphan-GC pass. The
concurrency test opens ten writers, commits them concurrently, asserts exactly
one version-2 publication, runs GC after every writer has stopped, and verifies
the winner remains readable.

### Dataset-shaped research boundary

`BeTreeDataset` is a narrow adapter in `lance-table`, selected by
`lance.manifest.layout=betree`. It proves the wiring without production feature
flags or a second on-disk format:

```text
append / add_columns / delete / row-delete metadata
                      │
                      ▼
              FragmentAction[]
                      │
                      ▼
                BeTree::commit
```

Append, add-column, deletion-file, whole-fragment delete, drop-file, and
replace-file operations commit targeted actions without materializing the
tree. Overwrite and restore are explicitly global and stream the current
fragment ids before issuing replacement actions. Lazy reopen, point resolve,
streaming enumeration, and IO-free root counts remain available through the
adapter.

Tests cover append, add columns, whole-fragment delete, row-delete metadata,
drop-column intent, cast/update intent as remove-plus-add files, overwrite,
restore, lazy reopen, and rejection of any layout value other than `betree`.

### Selective reads and lifecycle

`resolve_fragments(&RoaringBitmap)` prunes sibling-separator routing ranges
recursively, loads each covering subtree at most once, and filters buffered
actions to selected ids. It does not prune on a child's live `max_key`: a
buffered insert can lie beyond that summary. Tests cover ordinary multi-subtree
pruning and buffered inserts outside live child bounds.

`gc_unreferenced_offline` traces nodes and leaves reachable from every
published root, preserving version history while deleting failed-commit and
superseded COW intermediates that no root references. Its name and API contract
require writer quiescence. An online sweep could otherwise delete an active
writer's COW files before root publication.

### Common A/B harness

The `betree_ab` bench and the tiered companion harness emit one CSV schema for
flat, tiered, and Bε layouts. They use identical deterministic fragment metadata
and measure commit bytes/time, cold-open reads/time, point-resolve reads/time,
and full-materialization reads/time. See [`AB_BENCH.md`](./AB_BENCH.md) for
commands and results.

The final harness revision starts every timer from the same prebuilt workload
request, runs Bε through `BeTreeDataset`, uses Rust 1.97 and Cargo's `bench`
profile for both branches, disables both lazy metadata caches, separates tiered
tree and transaction bytes, and records three independent runs with per-commit
p50/p95. It also sweeps 64/128/256 KiB, contiguous/scattered trickle locality,
and a 5,000-append flush-forcing case.

### Post-review hardening

A second pass corrected internal-node sizing to use the parent's exact encoded
`ChildRef` plus buffer bytes rather than the referenced child payload sizes.
Internal split, underflow, coalesce, and root-shrink decisions now enforce both
encoded-byte and fanout constraints.

Internal-buffer and leaf limits are now independent. A three-run local probe at
64 KiB internal / 1 MiB leaf reduced ~100K-fragment materialization from 136 to
16 GETs while contiguous trickle remained 119.2× below flat write volume.
Fresh-ID append also supplies its exact aggregate delta from the Dataset
operation, eliminating a child read performed only to maintain counts. The
tracked-store test asserts zero child reads, exact counts, and duplicate-id
rejection. Full rationale and remaining experiments are in
[`DESIGN_REVIEW.md`](./DESIGN_REVIEW.md).

## What was left behind

- sealed immutable children as a format invariant
- protobuf fragment blobs as the leaf representation
- manifest deletion vectors as the primary delete mechanism
- production `tiered` versus `betree` feature flags
- full production `lance::Dataset` integration

The last item is intentional scope control. This change establishes and tests
the transaction/action boundary in `lance-table`. Production integration still
needs commit-handler selection, scanner/index call-site adoption, and
format-governance review.

## A/B summary

At N=50K with F=10 for 500 contiguous commits and a 128 KiB budget, the
three-run median Bε average was 62,470.986 tree bytes per commit versus
4,477,891.248 for flat. Tiered wrote 4,970,842.956 tree bytes plus
4,477,877.248 transaction bytes, or 9,448,720.204 total. Bε therefore wrote
71.7× less than flat, 79.6× less than tiered's tree alone, and 151.2× less than
tiered end to end.

The sensitivity sweep makes the boundary explicit. Across 64/128/256 KiB and
contiguous/scattered updates, Bε ranged from 34,300 to 146,433 bytes per
commit. Its worst point still wrote 30.6× less than flat and 34.4× less than
tiered-tree output, but the original `flat / 50` gate did not hold at every
budget/locality point.

Short append runs do not flush and are no longer treated as steady-state
evidence. In the post-review rerun of 5,000 N=50K appends, Bε flushed five
times and averaged 62,608 bytes / 0.492 ms, versus the prior matched tiered
result of 219,257 bytes / 8.400 ms and a rerun flat result of 4,498,647 bytes /
7.279 ms. The known append delta accounts for the Bε latency improvement.

On the read side, Bε cold open required two operations: root listing plus root
GET. Tiered was faster in wall time and materialized in fewer reads. With both
metadata caches disabled, both lazy layouts averaged exactly two reads per
point lookup. The post-review Bε rerun averaged 0.466 ms versus tiered's
original matched 3.118 ms. The preliminary 0.20-read tiered result was a
cache-policy artifact.

Keep Bε as the on-disk layout under study. The useful tiered work now lives at
the Dataset/action, publication, access-test, selective-read, and lifecycle
boundaries rather than as a competing sealed-tree format.
