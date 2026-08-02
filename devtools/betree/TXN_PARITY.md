# Goal: Transaction record parity + CommitBuilder-shaped opt-in

**Trunk:** `/Users/viltrum/workspace/lance-betree-extend` @ `research/jack-betree-extend`  
**Prior:** [OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md), [AB_BENCH.md](./AB_BENCH.md), [INTEGRATE_REPORT.md](./INTEGRATE_REPORT.md)  
**Jack PR:** [#7848](https://github.com/lance-format/lance/pull/7848)  
**Discussion:** [#7499](https://github.com/lance-format/lance/discussions/7499)

**Hand this whole file to an agent as the goal.**  
**Pair with:** [ROOT_DELTA.md](./ROOT_DELTA.md) after this goal’s success criteria pass, or in parallel only if a second agent will not fight over the same files.

---

## Mission

Close the biggest validity risk in every Bε write number we have published:

> The research adapter has no production-shaped transaction record. Tiered’s
> full-list `Merge` txn erased its delta advantage. Until Bε reports
> `tree_bytes + txn_bytes`, the fine-grained win is storage-layout only.

Build the smallest real-ish commit path that:

1. Writes a **minimal Bε transaction record** beside each commit.
2. Accounts **tree bytes and txn bytes separately**, then sums them.
3. Exposes a **CommitBuilder-shaped opt-in** behind the existing
   `lance.manifest.layout=betree` key. Flat stays default. No production
   default change.
4. Reruns add-column trickle, replace trickle, and mixed stream through that
   path and reports whether the update-window byte property survives.

**Do not** land Bε as a default.  
**Do** falsify or confirm the OPEN_QUESTIONS team ask with evidence.

---

## Stance

| Do | Do not |
|----|--------|
| Make txn bytes visible in the harness CSV | Hide txn cost inside tree_bytes |
| Keep actions as the mutation language | Reintroduce sealed AMT / full fragment Merge lists |
| Prefer extending `BeTreeDataset` + `betree_ab` | Boil the ocean through every Dataset call site |
| Fail loud if txn serialization becomes table-scale | “Fix” a kill by widening budgets silently |
| Local FS first; S3 if credentials and time remain | Block the whole goal on S3 |

Working claim to test:

> A Bε commit can carry a transaction record whose size tracks the commit’s
> actions, not the live fragment count, while tree writes stay near the
> update window.

---

## Background

### Why this exists

From `OPEN_QUESTIONS_REPORT.md`:

- Add-column / replace / mixed stayed ~61–75 KiB/commit on the adapter vs
  ~4.2–4.4 MiB flat.
- Tiered lost because txn + tree both went table-scale.
- Recommended team ask: ~5 days for txn parity + CommitBuilder-shaped opt-in,
  then remeasure.

### What already exists

- `BeTree::commit(actions)` with create-only root publish + conflict
- `BeTreeDataset` research adapter (`lance.manifest.layout=betree`)
- `betree_ab` harness with `AB-TRICKLE`, `AB-REPLACE`, `AB-MIXED`, flat baseline
- CSV columns already conceptually split tree vs transaction for tiered

### What “transaction record” means here

Minimum viable research txn, not a full clone of production `Transaction`:

- One object per successful commit, create-only, versioned path under `_bt/`
  or beside roots
- Contents sufficient to audit the commit: version, operation kind, and the
  `FragmentAction[]` (or an equivalent compact encoding of those actions)
- Size must be counted in `transaction_bytes`
- Readers / open path must not require replaying the entire txn history if
  the tree root already reflects applied state. Txn is for parity, conflict
  rebase research, and honest byte accounting. Do not make every open merge
  all txns unless ROOT_DELTA explicitly takes that on.

If production `lance` transaction protos can be reused without dragging the
goal into a months-long integration, prefer reuse. Otherwise a betree-local
proto/JSON is acceptable for the research path, with a clear “not final
format” note in the report.

---

## Part 1 — Minimal txn record on `BeTree::commit`

### Requirements

1. Every successful `BeTree::commit` persists a txn object for that version.
2. Failed / conflicted commits do not leave a published txn as tip.
3. `CommitStats` (or equivalent) reports:
   - `tree_bytes` (roots/nodes/leaves written this commit)
   - `transaction_bytes` (txn object bytes)
   - `total_bytes = tree_bytes + transaction_bytes`
4. Tests:
   - append / add-column / replace commits create txn objects
   - conflict path does not advance txn tip
   - txn bytes for F=10 add-column are far below flat manifest size at N=50K
   - open still works without materializing history

### Done when

```bash
cargo test -p lance-table betree
```

includes txn accounting tests and existing betree tests stay green.

---

## Part 2 — Harness byte parity

### Requirements

1. `betree_ab` Bε rows populate `transaction_bytes` from the new txn objects.
2. `total_bytes` is what “survives update-window” judgments use.
3. Flat continues to report txn=0 or N/A consistently with prior CSVs.
4. Rerun and check in dated CSVs under `devtools/betree/bench-results/`.

### Required scenarios (local, median of 3)

N=50K, F=10, 128 KiB budget, contiguous + scattered:

| Scenario | Commits |
|----------|--------:|
| AB-TRICKLE add-column | 500 |
| AB-REPLACE | 500 |
| AB-MIXED | 1000 |

Compare against research-adapter baselines in
`OPEN_QUESTIONS_REPORT.md` / `update_ab_local_20260801.csv`.

### Kill criteria (hard stop)

Stop structural optimism and write a failure-leaning report if **any** of:

1. Contiguous AB-TRICKLE or AB-REPLACE `total_bytes` at N=50K F=10 exceeds
   **10×** the prior adapter `tree_bytes` baseline (~61–62 KiB → kill above
   ~610–620 KiB), or
2. Uncached point resolve after those streams needs **more than 4 GETs** at
   the chosen leaf size, or
3. The txn object itself encodes a full live fragment list / table-scale blob
   (design failure even if somehow small once).

If killed: do not invent a new tree. Document the failure and what the txn
contained that blew up.

---

## Part 3 — CommitBuilder-shaped opt-in

### Goal

Prove the boundary production will need, without changing defaults.

### Requirements

1. A path in `lance` or a clearly named research module that looks like
   CommitBuilder usage:
   - select layout via `lance.manifest.layout=betree`
   - translate append / add_columns-style / replace-file intents into
     `FragmentAction[]`
   - commit through the txn-aware Bε path
2. Flat / unset layout behavior unchanged.
3. Reject unknown layout values with a clear error.
4. Tests for opt-in success and default-flat non-interference.

Acceptable scopes, in preference order:

A. Wire through real `lance::dataset` commit pieces if clean and small.  
B. If A is a tangle, ship `BeTreeCommitBuilder` in `lance-table` that mirrors
   the CommitBuilder shape and call it from `betree_ab` + unit tests, with an
   explicit “next step: Dataset integration” section in the report.

Do not pretend B equals production Dataset wiring. Name it honestly.

---

## Part 4 — Optional S3 smoke (time-permitting)

If AWS creds / `BASE_URI` are available:

- Rerun contiguous AB-TRICKLE + AB-REPLACE once on S3 Standard or Express
- Report mean commit ms and total_bytes only; do not expand matrix

If unavailable, note “local only” and move on. Do not block the report.

---

## Deliverables

1. **`devtools/betree/TXN_PARITY_REPORT.md`**
   - What txn record was implemented and where it lives
   - Byte tables: tree / txn / total for trickle, replace, mixed
   - Kill-criteria verdict: PASS / KILL / MIXED
   - CommitBuilder-shaped surface: what landed vs still missing
   - Exact paste-ready status blurb for the human’s team ask
2. Dated CSVs under `devtools/betree/bench-results/`
3. Code + tests on this branch only

---

## Success criteria

- [ ] Txn object written on successful commits; counted separately
- [ ] `betree_ab` reports tree + txn + total for Bε
- [ ] Contiguous trickle/replace/mixed rerun checked in
- [ ] Kill criteria evaluated explicitly
- [ ] CommitBuilder-shaped opt-in exists (A or honest B)
- [ ] `cargo test -p lance-table betree` passes
- [ ] `cargo fmt` + clippy clean on touched crates
- [ ] No default layout change
- [ ] No GitHub post unless human asks

---

## Order of work

```text
1. Read OPEN_QUESTIONS_REPORT.md team ask + kill criteria
2. Design minimal txn object + CommitStats fields
3. Implement Part 1 + tests
4. Wire harness Part 2 + local reruns
5. Evaluate kill criteria before Part 3 polish
6. Part 3 CommitBuilder-shaped opt-in
7. Optional S3 smoke
8. TXN_PARITY_REPORT.md
9. If PASS: proceed to ROOT_DELTA.md
```

---

## Out of scope

- Changing Dataset default manifest layout
- Full scanner/index adoption
- Sealed-AMT revival
- Per-node immutable runs (see ROOT_DELTA / later)
- ZSTD for flat (separate track)
- Posting on #7499 / #7848

---

## One-shot agent prompt

```text
Execute /Users/viltrum/workspace/lance-betree-extend/devtools/betree/TXN_PARITY.md

Close the research-adapter honesty gap: add a minimal Bε transaction record
with tree_bytes + transaction_bytes accounting, CommitBuilder-shaped opt-in
behind lance.manifest.layout=betree (no default change), and rerun
AB-TRICKLE / AB-REPLACE / AB-MIXED.

Kill if total_bytes exceed 10× prior adapter baselines on contiguous F=10 at
N=50K, or resolve needs >4 GETs, or txn encodes a full fragment list.

Produce TXN_PARITY_REPORT.md. Do not post to GitHub.
If PASS, continue with ROOT_DELTA.md.
```
