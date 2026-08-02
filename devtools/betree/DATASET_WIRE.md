# Goal: Wire Bε through the real Dataset writer

**Trunk:** `/Users/viltrum/workspace/lance-betree-extend` @ `research/jack-betree-extend`  
**Prior:** [TXN_PARITY_REPORT.md](./TXN_PARITY_REPORT.md), [ROOT_DELTA_REPORT.md](./ROOT_DELTA_REPORT.md), [OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md)  
**Jack PR:** [#7848](https://github.com/lance-format/lance/pull/7848)  
**Discussion:** [#7499](https://github.com/lance-format/lance/discussions/7499)

**Hand this whole file to an agent as the goal.**

---

## Mission

Stop measuring a research adapter and put Bε on the **real writer**.

Every public number so far goes through `BeTreeDataset` and/or
`lance_table::betree::commit_builder::CommitBuilder`. That answered storage
layout and action-sized txn questions. It did **not** answer:

> When a user commits through `lance::Dataset` / `lance::dataset::Write` /
> `CommitBuilder`, do fine-grained add-column and replace commits still stay
> near the update window, or does the production transaction / manifest path
> reintroduce table-scale work the way tiered `Merge` did?

This goal exists because the previous goals were allowed to fake the writer.
This one is not.

**Do not** change the Dataset default layout.  
**Do** make `lance.manifest.layout=betree` select a real commit path and
prove it with the A/B harness.

---

## Hard bans (read twice)

If the agent violates these, the goal is a failure even if benches look good.

1. **Forbidden as the measured commit path**
   - `BeTreeDataset::…` commit helpers used by `betree_ab` as the Bε contender
   - `lance_table::betree::commit_builder::CommitBuilder` as the Bε contender
   - Any new “research writer” that is not `lance::dataset` / `lance::io::commit`

2. **Required as the measured commit path**
   - Real `lance::Dataset` (or the same `CommitBuilder` / write path production
     code uses) with config `lance.manifest.layout=betree`
   - Real production transaction objects under the normal transactions dir /
     commit machinery, unless the report proves an explicit compatible
     substitute that `CommitBuilder` itself writes

3. **Allowed research code**
   - `BeTree` storage engine underneath
   - Root delta chain / txn helpers **called from** the real commit path
   - Flat baseline may still use the existing flat writer
   - Unit tests may still use `BeTreeDataset` for engine-only checks

4. **No escape hatches**
   - Do not “finish” by documenting that Dataset wiring is future work
   - Do not widen kill lines to absorb a full-list `Merge`
   - Do not claim PASS if only create/open work and mutations still go through
     the research adapter

---

## Stance

| Do | Do not |
|----|--------|
| Wire the smallest real commit surface that proves the invariant | Port every Dataset API in one night |
| Translate real `Operation` / transaction intents into `FragmentAction[]` without embedding the full fragment list | Silently fall back to `Operation::Merge` with a full fragment list for the measured scenarios |
| Count tree + txn (+ delta) bytes on the real path | Report adapter-only totals and call them Dataset |
| Flat remains default when layout unset | Change defaults or remove flat |
| Local FS required; S3 optional | Block forever on missing AWS creds |

Working claim to falsify:

> On the real Dataset commit path with `lance.manifest.layout=betree`, 
> fine-grained add-column and replace commits keep
> `tree_bytes + transaction_bytes (+ delta_bytes)` near the update window at
> N=50K, F=10.

---

## Background the agent must internalize

### Why previous goals stopped short

`TXN_PARITY` explicitly allowed “honest scope B”: a research CommitBuilder in
`lance-table`. That closed the “txn encodes the fragment list” scare on the
adapter. It left the social and technical hole Will / reviewers will hit:

research path ≠ production writer.

Tiered already showed the failure mode: tree can look fine while the
production txn serializes table-scale state.

### What already exists to reuse

- `BeTree` commit, root δ chain (`max_root_delta_tail`), action txn objects
- `lance.manifest.layout` key constants
- `betree_ab` scenarios: trickle, replace, mixed
- Production `lance::dataset::write::commit::CommitBuilder` and
  `lance::io::commit` manifest publication
- Flat writer path as baseline

### Minimum Dataset surface for this goal

Must work end-to-end with layout=`betree`:

| User-facing op | Why required |
|----------------|--------------|
| Create / write initial table | Bootstrap tree instead of flat-only manifest |
| Append fragments | W1 |
| Add columns / add data files to existing fragments | W2 trickle |
| Replace / drop+add data files on a fragment subset | W4 replace |
| Open latest version lazily enough to resolve a fragment | read check after streams |

Nice-to-have if clean: delete fragment, overwrite. Not required to PASS.

Out of scope for PASS: Python/Java, index rebuild, compaction planner rewrites,
full scanner redesign. But if those call sites force full materialize on every
commit in the measured path, document it as a kill risk.

---

## Part 1 — Production commit integration

### Requirements

1. Reading `lance.manifest.layout=betree` from dataset config / write params
   selects the Bε commit backend inside the real `lance` crate commit path.
2. Unset / `flat` keeps today’s behavior.
3. Unknown layout values error clearly.
4. Commits go through `CommitBuilder` (or the identical internal function
   `CommitBuilder::execute` calls). No parallel private publish API for the
   bench.
5. Transaction persistence uses the production transaction mechanism when
   possible. If Bε needs extra `_bt/` objects, they are in addition to, not
   instead of, an honest txn byte count the harness can read.
6. Fine-grained ops must **not** expand into a full-fragment `Merge` payload
   for the measured scenarios. If today’s API only offers Merge, add or use an
   operation shape that carries the touched actions / fragment subset, and say
   so in the report. Hiding a full list inside Merge is an automatic KILL.

### Tests (in `lance` and/or `lance-table`, but exercising Dataset)

- create with layout=betree, reopen, resolve one fragment
- append commit through Dataset/CommitBuilder
- add-column-style commit touching F≪N fragments
- replace-file-style commit touching F≪N fragments
- default layout still flat
- two writers conflict behavior still sane (at least one wins, no corrupt tip)

---

## Part 2 — Harness must call the real path

### Requirements

1. Change `betree_ab` (or replace the Bε contender) so the Bε layout commits
   via `lance::Dataset` / production `CommitBuilder`.
2. CSV still reports `tree_bytes`, `transaction_bytes`, `delta_bytes` if any,
   `total_bytes`, commit ms, open/resolve GETs after the stream.
3. Flat contender stays the real flat writer.
4. Comment at the top of the bench file states the Bε path is Dataset-backed.
   If someone reintroduces `BeTreeDataset` as the contender, that is a bug.

### Required local runs (median of 3)

N=50K, F=10, 128 KiB, root δ enabled at the research default (32) unless the
real path cannot honor it yet (then say why and run both):

| Scenario | Locality | Commits |
|----------|----------|--------:|
| AB-TRICKLE add-column | contiguous, scattered | 500 |
| AB-REPLACE | contiguous, scattered | 500 |
| AB-MIXED | contiguous | 1000 |

Compare against:

- research adapter: `update_ab_local_20260801.csv`
- txn parity: `txn_parity_local_20260802.csv`
- root delta: `root_delta_local_20260802.csv`

---

## Part 3 — Kill criteria

Hard KILL (stop, write failure report, do not dress it up):

1. Contiguous AB-TRICKLE or AB-REPLACE `total_bytes` at N=50K F=10 exceed
   **10×** the root-delta research baseline (~4 KiB → kill above ~40 KiB),
   **or** if root δ is unavailable on the real path, exceed **10×** the
   txn-parity baseline (~63 KiB → kill above ~630 KiB).
2. The production transaction object for those commits encodes a full live
   fragment list / table-scale manifest blob.
3. Uncached point resolve after the streams needs **more than 4 GETs**.
4. The measured Bε path is still the research adapter (ban violation).

MIXED is allowed only if some scenarios pass and a specific op still forces
Merge-with-full-list. Name the op. Do not call the goal PASS.

PASS requires trickle + replace contiguous clear of the kill lines on the
real Dataset path.

---

## Part 4 — Report

Write **`devtools/betree/DATASET_WIRE_REPORT.md`** with:

1. Exact files / functions where Dataset commit branches on layout=betree
2. How `Operation` maps to `FragmentAction[]` without full-list Merge for the
   measured ops
3. Byte tables vs adapter / txn / root-δ baselines
4. Kill-criteria verdict: PASS / KILL / MIXED
5. What is still not wired (scanner, indexes, Python, etc.)
6. Paste-ready blurb the human can put under Will’s thread or send internally
   that does **not** claim the design is landed

Also check in dated CSVs under `devtools/betree/bench-results/`.

Optional S3 smoke if creds exist. Do not block the report on S3.

---

## Success criteria

- [ ] Real `lance::Dataset` / `CommitBuilder` path commits with layout=betree
- [ ] `betree_ab` Bε contender uses that path only
- [ ] Trickle + replace (+ mixed) local CSVs checked in
- [ ] Kill criteria evaluated explicitly
- [ ] Default layout still flat
- [ ] `cargo test -p lance-table betree` still green for engine tests
- [ ] Relevant `lance` tests for the new wiring green
- [ ] fmt + clippy clean on touched crates
- [ ] No GitHub post unless human asks
- [ ] No “we used the research builder because Dataset was hard” PASS

---

## Order of work

```text
1. Trace CommitBuilder::execute / write_manifest_file for flat today
2. Find the smallest branch point for layout=betree
3. Map append / add-columns / replace intents → FragmentAction[] 
   without full-list Merge
4. Dataset create/open/commit tests
5. Point betree_ab Bε contender at Dataset path
6. Run kill-criteria benches
7. DATASET_WIRE_REPORT.md
```

If step 3 is impossible without a new `Operation` variant, add the minimal
variant / transaction payload needed for research opt-in and document it.
Do not fake it with Merge(full fragments).

---

## Out of scope

- Making betree the default
- Posting on #7499 / #7848
- Sealed-AMT revival
- Full index/scanner migration
- Flat ZSTD (separate)
- Claiming production readiness

---

## One-shot agent prompt

```text
Execute /Users/viltrum/workspace/lance-betree-extend/devtools/betree/DATASET_WIRE.md

Wire Bε through the REAL lance::Dataset / CommitBuilder path behind
lance.manifest.layout=betree. Flat stays default.

Hard ban: betree_ab must NOT measure BeTreeDataset or the research
lance_table betree CommitBuilder as the Bε contender.

Rerun AB-TRICKLE / AB-REPLACE / AB-MIXED on that path.
Kill if totals exceed 10× the root-delta baseline (~4 KiB) when δ is
available, or 10× txn-parity (~63 KiB) otherwise, or if the production txn
encodes a full fragment list, or if resolve needs >4 GETs.

Produce DATASET_WIRE_REPORT.md. Do not post to GitHub.
Do not PASS if you only improved the research adapter.
```
