# Goal: External root delta chain (LSM complement on Bε)

**Trunk:** `/Users/viltrum/workspace/lance-betree-extend` @ `research/jack-betree-extend`  
**Depends on:** [TXN_PARITY.md](./TXN_PARITY.md) ideally first, so byte accounting already splits tree vs commit metadata  
**Prior:** [OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md) Q2, Jack’s notes on #7848 / #7499  
**Hand this whole file to an agent as the goal.**

---

## Mission

Implement Jack’s **external root delta chain** idea: stop rewriting the entire
inline root ε-buffer on every small commit when a cheaper append-only delta
object can carry the new actions until flush.

This is the LSM **complement**, not a substitute for keyed Bε routing.

**Question to answer with numbers:**

> Relative to txn-aware Bε without a root delta chain, how much do contiguous
> and scattered F=10 add-column / replace commits shrink in `total_bytes` and
> commit latency, and what do open / point-resolve GET counts become?

**Do not** replace the fragment-id tree with a global-only LSM.  
**Do** keep height-bounded resolve.

---

## Stance

| Do | Do not |
|----|--------|
| Append small root deltas for ordinary commits | Rewrite full root protobuf every time if avoidable |
| Compact / fold deltas into root or children on flush gates | Let delta chains grow unbounded without a policy |
| Keep point resolve ≤ height + small constant + delta-tail policy | Require readers to scan thousands of deltas |
| Measure against TXN_PARITY baselines | Claim victory from microbench without open/resolve |
| Fail if read amp explodes | Hide read amp behind write-only tables |

Working claim:

> An external root delta chain removes most per-commit root-buffer rewrite
> bytes while preserving keyed flush into the Bε tree and bounded resolve.

---

## Background

### Why this exists

From `OPEN_QUESTIONS_REPORT.md` Q2:

- Global manifest LSM alone is not a substitute: point resolve needs keyed
  structure or unbounded segment merge.
- Jack’s external root delta chain is the LSM-shaped piece worth stealing.
- It targets the per-commit root ε-buffer rewrite that still dominates many
  Bε commits even when leaves are untouched.

### Design constraints

1. **Commit path:** ordinary action commits append a delta object and advance
   a tip pointer / version with create-only CAS semantics consistent with
   current root publication tests.
2. **Fold path:** when ε / byte / count gates say so, fold outstanding root
   deltas into the tree via existing flush/split logic, publish a compacted
   root, and GC obsolete deltas under the same offline-GC contract spirit.
3. **Read path:** `open` loads compacted root + outstanding delta tail.
   `resolve_fragment` / `resolve_fragments` must apply relevant delta actions
   without full materialize.
4. **Byte accounting:** deltas count as commit metadata. Be explicit in CSV
   columns whether they land in `transaction_bytes`, `tree_bytes`, or a new
   `delta_bytes` field that rolls into `total_bytes`. Prefer a dedicated
   `delta_bytes` column if the harness is already being edited.

### Strawman layout (agent may adjust, document deviations)

```text
_bt/root/{version}.root          # compacted root (protobuf)
_bt/root_delta/{version}.delta   # append-only action batch for that commit
_bt/leaf/...                     # unchanged Lance leaves
_bt/node/...                     # unchanged internal nodes
```

Tip selection: highest version with create-only publish. Open reads compacted
root at or before tip, then the delta chain after that root.

If TXN_PARITY already writes `_bt/txn/...`, do not double-encode blindly.
Either:

- txn object *is* the root delta, or
- txn is a small envelope pointing at / digesting the delta,

and say which in the report. Prefer one object per commit when possible.

---

## Part 1 — Mechanism + tests

### Requirements

1. Commits that only dirty the root buffer append a delta instead of rewriting
   a large inline buffer root, until a fold is required.
2. Fold still uses keyed routing into children. No global fragment list.
3. Conflict / CAS semantics remain: two writers on same base version cannot
   both publish the next tip.
4. Offline GC understands delta objects.
5. Tests:
   - many small appends create deltas and defer large root rewrites
   - resolve matches materialize with outstanding deltas
   - bitmap resolve still sees buffered inserts outside live child max_key
   - fold clears delta tail and preserves counts
   - concurrent publish still single-winner

### Done when

```bash
cargo test -p lance-table betree
```

passes with new delta-chain coverage.

---

## Part 2 — Benchmarks

### Baseline

Use TXN_PARITY local numbers if present. Otherwise use
`update_ab_local_20260801.csv` and note missing txn/delta split.

### Required runs

N=50K, F=10, 128 KiB, median of 3, caches off:

| Scenario | Locality | Commits |
|----------|----------|--------:|
| AB-TRICKLE | contiguous, scattered | 500 |
| AB-REPLACE | contiguous, scattered | 500 |
| AB-MIXED | contiguous | 1000 |
| AB-OPEN / AB-RESOLVE after trickle bootstrap | — | read matrix |

Report: `tree_bytes`, `delta_bytes` or txn/delta accounting, `total_bytes`,
`commit_ms`, flushes/folds, open GETs/ms, resolve GETs/ms, max delta-tail
length observed.

### Interpretation bars

**Win:** contiguous trickle/replace `total_bytes` clearly below TXN_PARITY
Bε totals, without resolve GET regression beyond the Part 3 kill line.

**Acceptable trade:** slightly higher open cost for large delta tails if fold
policy bounds tail length and resolve stays within kill line.

**Loss:** write bytes barely change but resolve/open degrade badly, or fold
policy never runs and tails grow with commit count.

---

## Part 3 — Kill criteria

Hard stop / report KILL for root-delta as a default direction if:

1. After 500 contiguous F=10 trickles at N=50K, outstanding delta tail length
   exceeds **64** without a fold, or
2. Uncached point resolve average GETs exceed **4**, or
3. Contiguous trickle `total_bytes` is **worse** than TXN_PARITY Bε total by
   more than 20% with no compensating resolve/open win, or
4. Implementation requires scanning the full delta tail for every resolve with
   no fragment-id pruning and no bounded fold.

If killed: keep the code behind an explicit config flag if salvageable for
research, or document revert; do not silently leave an unbounded chain on.

---

## Part 4 — Report

Write **`devtools/betree/ROOT_DELTA_REPORT.md`** with:

1. On-disk layout and relationship to TXN_PARITY objects
2. Fold policy knobs
3. Before/after tables vs TXN_PARITY (or adapter baseline)
4. Read-amp behavior and max tail length
5. Verdict: adopt / adopt-behind-flag / reject for now
6. Paste-ready note for #7499 Q2 follow-up (“LSM complement measured”)

---

## Success criteria

- [ ] Root delta chain implemented with fold policy
- [ ] Tests for resolve correctness with outstanding deltas
- [ ] Bench CSVs checked in with date suffix
- [ ] Kill criteria explicitly evaluated
- [ ] `cargo test -p lance-table betree` green
- [ ] fmt + clippy clean on touched crates
- [ ] No Dataset default change
- [ ] No GitHub post unless human asks

---

## Order of work

```text
1. Confirm TXN_PARITY status. If txn accounting missing, implement the
   minimum needed so totals are honest, or clearly mark deltas in CSV.
2. Design delta path + fold policy against current write_root/commit
3. Implement + correctness tests
4. Bench Part 2
5. Kill criteria
6. ROOT_DELTA_REPORT.md
```

---

## Out of scope

- Full leveled LSM replacing Bε leaves
- Per-node immutable message runs across all internal nodes (optional stretch
  only after root chain works; do not start here)
- Production format freeze
- Flat ZSTD
- Sealed children

---

## One-shot agent prompt

```text
Execute /Users/viltrum/workspace/lance-betree-extend/devtools/betree/ROOT_DELTA.md

Implement Jack's external root delta chain as an LSM complement on the Bε
tree: append-only root deltas for small commits, keyed fold/flush into the
tree, bounded resolve with outstanding deltas.

Prefer running after TXN_PARITY.md so byte accounting is honest.
Measure trickle/replace/mixed vs the txn-parity baseline.
Kill if delta tails grow past 64 without fold, resolve needs >4 GETs, or
total_bytes regress >20% without a read win.

Produce ROOT_DELTA_REPORT.md. Do not post to GitHub.
```
