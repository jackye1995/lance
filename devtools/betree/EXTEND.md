# Goal: Extend Jack's Bε-tree — fidelity + lazy loading + bench

**Base:** `jackye1995/lance` @ `jack/bepsilon-manifest-proto` (`36bcc8d95`)  
**Local branch:** `research/jack-betree-extend`  
**Worktree:** `/Users/viltrum/workspace/lance-betree-extend`  
**PR:** https://github.com/lance-format/lance/pull/7848  
**Discussion:** https://github.com/lance-format/lance/discussions/7499

**Hand this whole file to an agent as the goal.**

---

## Mission

Take Jack's recursive Bε-tree as the trunk. Make it correct enough to
round-trip real fragment metadata, add a lazy read path for open / point
resolve / stream, and benchmark lazy vs full materialize against the existing
flat / Bε write-amp harness.

No feature flags. No Dataset production wiring in this goal. Keep `_bt/`
prototype layout and flat-oracle tests.

---

## Stance

| Do | Do not |
|----|--------|
| Extend `rust/lance-table/src/betree/` | Rebuild sealed-AMT tiered-manifest |
| Add lazy APIs next to `materialize` | Make every open call full materialize |
| Keep write-amp benches green | Add reader feature-flag ceremony |
| Measure GET counts for lazy paths | Only report commit write bytes |

---

## Already good

- Recursive Bε: msn actions, flush, split/merge, COW (`tree.rs`, `node.rs`)
- Columnar Lance leaves, one row per data file (`store.rs`)
- Fine-grained backfill + at-scale benches (`benches/betree_backfill.rs`)
- Flat baseline via real `write_manifest_file_to_path` / `read_manifest`

**Known hole today:** `BeTree::cold_open` always calls `materialize()`. Leaves
drop DV / row-id / version meta on flush (`store.rs` documents this).

---

## Phase A — P0 leaf fidelity

**Why first:** lazy resolve is worthless if flushing a leaf deletes deletion
vectors / row ids.

### Requirements

1. Leaf Lance schema round-trips full `Fragment` extras needed for Lance:
   - `deletion_file`
   - `row_id_meta`
   - `created_at_version_meta` / `last_updated_at_version_meta`
   - Prefer nullable side columns or a compact per-fragment blob column. Keep
     per-`DataFile` columns as today.
2. Fragments with zero data files are representable via an explicit marker row
   or fragment-level row.
3. `AddDataFile` / `AddDeletionFile` / `RemoveDataFile` on a missing
   `frag_id` return `Error`, not a silent no-op.
4. Add `ClearDeletionFile` to `FragmentAction` + `apply_one` + `action.rs`
   builder.
5. Tests: write leaf with DV + row-id meta, read back equal, apply
   `ClearDeletionFile`, missing-frag action errors.

### Done when

```bash
cargo test -p lance-table betree
```

includes new fidelity tests. Existing split/merge flat-oracle tests still pass.

---

## Phase B — Lazy loading

### B1. Lazy open

Replace "cold open = materialize everything" with:

```text
BeTree::open(...) -> BeTree   // LIST roots + GET newest root; no child GETs
BeTree::materialize()         // keep for oracle / full enum
```

- `cold_open` may become a thin wrapper that `open` + `materialize` for
  backward-compat benches, or benches switch to `open` + explicit materialize.
- After `open`, `height()`, root buffer len, and child ref count are available
  with 0 child GETs.

### B2. Point resolve

```rust
impl BeTree {
    /// Load O(height) nodes; apply overlays along the path; return one fragment.
    pub async fn resolve_fragment(&self, frag_id: u64) -> Result<Option<Fragment>>;
}
```

- Route by sibling `ChildRef.min_key` separators. `max_key` is only a live-data
  summary: buffered inserts can sit beyond it until the next sibling separator.
- Collect `TaggedAction`s from root + each internal buffer on the path. Apply
  with leaf rows using msn newest-wins, matching full materialize for that id.
- GET bound: ≤ `height + 1` object reads for a hit. Root already in memory means
  ≤ height.
- Test: multi-level tree, resolve random ids == materialize()[id], assert GET
  count ≤ height + slack.

### B3. Streaming enumerate

```rust
impl BeTree {
    /// Yield fragments in id order without holding all N in RAM.
    pub async fn iter_fragments(&self) -> impl Stream<Item = Result<Fragment>>;
}
```

- Walk leaves in key order. Apply only actions targeting that leaf's id range,
  or apply path overlays per leaf.
- Peak RAM = O(one leaf + pending actions for that subtree), not O(N).

### B4. Aggregates on refs

Extend `ChildRef` in proto + writers:

| Field | Meaning |
|-------|---------|
| `total_rows` | Sum of live/physical rows in subtree. Physical is OK for v1. |
| `row_offset_start` | Prefix sum for row→frag routing. Optional if physical-only. |

Maintain on write_leaf / write_internal / flush/split/merge.

```rust
pub fn count_fragments(&self) -> u64;  // sum num_keys, no IO
pub fn count_rows(&self) -> u64;       // sum total_rows, no IO
```

### B5. Tests for lazy path

| Test | Assert |
|------|--------|
| `lazy_open_does_not_read_leaves` | After `open`, IO counter shows one root LIST + one root GET; 0 child GETs |
| `resolve_matches_materialize` | ≥40 random ids on height≥2 tree |
| `resolve_get_bound` | GETs ≤ height + 2 |
| `stream_equals_materialize` | Full stream == `materialize()` order/content |
| `counts_without_io` | `count_*` match materialize; no leaf GETs |

Instrument IO via existing object-store stats or a test wrapper.

---

## Phase C — Benchmark lazy vs eager

Extend `benches/betree_backfill.rs` or add `betree_lazy.rs`. Prefer local FS
first. S3 optional via existing `BASE_URI`.

### Scenarios

| ID | Setup | Measure |
|----|-------|---------|
| **L1** | Bootstrap N=100K or 1M if CI allows, height≥2 | `open` wall + GET count vs old `cold_open`/`materialize` |
| **L2** | Same tree | `resolve_fragment` × 100 random ids: mean GETs, mean ms |
| **L3** | Same tree | Full `iter_fragments` / stream: wall, peak RSS if easy, GET count |
| **L4** | After F=10 backfill sample | L1–L3 still hold; resolve still matches materialize |
| **W1** | Keep existing fine-grained backfill write-amp | Must not regress >10% vs baseline on this branch |

### Report

Write `devtools/betree/LAZY_BENCH.md` with:

- Commands + env vars
- Table: scenario × open_gets × open_ms × resolve_gets_avg × materialize_gets
- One-paragraph verdict: when lazy open/resolve wins and by how much

CSV optional under `devtools/betree/bench-results/`.

### Reproduce

```bash
cd /Users/viltrum/workspace/lance-betree-extend

# correctness
cargo test -p lance-table betree

# existing write-amp (local)
cargo bench -p lance-table --bench betree_backfill -- --quick   # if supported
# or env-driven modes already in the bench file

# lazy matrix after Phase C lands. Exact flags documented in LAZY_BENCH.md.
# e.g. LAZY_BENCH=1 cargo bench -p lance-table --bench betree_backfill
```

---

## Phase D — Optional, only if A–C done

- External L0 delta chain from Jack's <1 GiB extrapolation
- Thin Dataset spike for append / add_columns actions → `BeTree::commit`
- Orphan GC for old COW node files

---

## Out of scope

- Feature flags / old-reader fail-closed
- Replacing flat as Dataset default
- Sealed-AMT / `tiered-manifest` revival
- Python/Java bindings
- Full Iceberg-style stats plane

---

## Success criteria

- [x] Phase A fidelity tests green; DV/row-id survive leaf flush
- [x] `BeTree::open` does not load leaves
- [x] `resolve_fragment` correct vs materialize; GET-bounded
- [x] Streaming enum equals materialize; bounded RAM intent documented/tested
- [x] `count_fragments` / `count_rows` or documented physical equivalent without leaf IO
- [x] Existing betree split/merge / backfill oracle tests still pass
- [x] `LAZY_BENCH.md` filled with measured numbers, local FS minimum
- [x] Write-amp bench not regressing badly (W1)

Re-verified clean after `build.rs` proto watch fix. See `LAZY_BENCH.md`
re-verification section.

---

## Implementation notes

1. Work only in this worktree / branch.
2. Prefer extending `store.rs` leaf schema over inventing a second leaf format.
3. Keep `materialize()` as the correctness oracle.
4. Overlay application for resolve must use the same `apply_actions` / msn
   rules as materialize. Extract a shared helper if needed.
5. For GET counting in tests, wrap `ObjectStore` or use existing IO stats
   patterns. Do not fake counts.
6. Proto changes in `protos/betree.proto` regenerate via crate `build.rs`.
7. English comments only. No drive-by refactors outside `betree/`.

---

## One-shot agent prompt

```text
Execute the goal in:
  /Users/viltrum/workspace/lance-betree-extend/devtools/betree/EXTEND.md

Branch: research/jack-betree-extend (Jack's Bε-tree trunk).
No feature flags. No sealed-AMT work.

Order: Phase A leaf fidelity → Phase B lazy open / resolve_fragment /
streaming enum / ref aggregates → Phase C lazy vs eager benches →
devtools/betree/LAZY_BENCH.md.

Keep cargo test -p lance-table betree green and do not regress write-amp benches.
Prefer real ObjectStore GET counts over estimates.
```
