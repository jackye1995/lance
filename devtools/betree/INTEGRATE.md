# Goal: Prove Bε vs tiered, then integrate mine into his

**Trunk:** Jack's Bε-tree (`research/jack-betree-extend` @ worktree
`/Users/viltrum/workspace/lance-betree-extend`)  
**Yours:** `tiered-manifest` in `/Users/viltrum/workspace/lance`  
**Already ported onto trunk:** leaf fidelity + lazy open / `resolve_fragment` /
stream / counts. See [EXTEND.md](./EXTEND.md), [LAZY_BENCH.md](./LAZY_BENCH.md).

**Hand this file to an agent as the next goal.**

---

## Mission

1. Run a fair head-to-head matrix: same N, same ops, same machine, local FS
   first.
2. Integrate what was valuable from tiered-manifest into the Bε trunk. Not the
   other way around.
3. Produce a short verdict doc: keep Bε as architecture, list exactly what was
   ported from tiered.

---

## Stance

| Decision | Why |
|----------|-----|
| Architecture under study = Jack's Bε | Byte-balanced nodes + flush absorb fragments × columns. Sealed AMT does not. |
| Integration direction = mine → his | Port Dataset wiring, access-pattern tests, commit CAS patterns into `betree/` |
| Do not revive sealed children as a competing format | Messages-on-AMT were a stepping stone |
| No feature-flag ceremony until Dataset spike is real | Research path / config opt-in string is enough |

---

## Part 1 — Fair A/B

### Contenders

| Label | Code | How to drive |
|-------|------|----------------|
| **flat** | Real `write_manifest_file_to_path` / `read_manifest` | Already in `betree` bench as `FlatBaseline` |
| **tiered** | `tiered-manifest` branch Dataset + `tiered_manifest_bench` | Checkout/worktree of `tiered-manifest`; layout=`tiered`, small `buffer_cap` so it seals |
| **betree** | This worktree `BeTree::commit` / `open` / `resolve_fragment` | `benches/betree_backfill.rs` + lazy mode |

### Same harness rules

- Same N, same paths. Prefer ~50-char realistic names when possible.
- Same F, fragments touched per backfill commit.
- Measure metadata only where possible, or separate meta PUT bytes from
  data-file write time.
- Report: `commit_bytes`, `commit_ms`, `open_gets`, `open_ms`,
  `resolve_gets_avg`, `materialize_gets`
- Local FS first. Optional S3 later with `BASE_URI`.

### Matrix

| ID | Op | N | Detail | Why |
|----|-----|---|--------|-----|
| **AB-APPEND** | Append 1 frag × 100 | 50K, 1M | After sealed / bootstrapped | Your known win. Check betree doesn't lose badly. |
| **AB-TRICKLE** | Backfill F=10 × sample | 50K or 100K; sample ≥500 commits | Headline | Jack's claimed win vs flat. Must include tiered. |
| **AB-ONESHOT** | One `AddDataFile`×N, or real `add_columns` if Dataset | 50K | One commit touches all | Your Merge/message shape |
| **AB-OPEN** | Cold open | after AB-APPEND @ 100K+ | GET count + ms | Lazy story |
| **AB-RESOLVE** | 100 point resolves | same tree | GET avg | Path cost |

### Deliverable

`devtools/betree/AB_BENCH.md` with one table:

```text
scenario | flat | tiered | betree | winner
```

Pass bar for "his is faster on trickle": betree `commit_bytes` ≤ 0.5× tiered on
AB-TRICKLE, and ≤ flat/50.

If tiered wins AB-APPEND open/root by a lot, note it. Do not change
architecture for that alone.

### How to run

```bash
# Betree (this worktree)
cd /Users/viltrum/workspace/lance-betree-extend
# extend betree_backfill or add ab_matrix mode that emits AB_* rows

# Tiered (separate worktree from lance @ tiered-manifest)
cd /Users/viltrum/workspace/lance
git checkout tiered-manifest
# run tiered_manifest_bench with matching N / F / rounds; export CSV
```

Prefer one CSV schema both emit so AB_BENCH.md is mechanical.

---

## Part 2 — Integrate mine into his

### Already done on Bε trunk

| From yours | Status on betree |
|------------|------------------|
| Overlay / message idea | His `FragmentAction` + your `ClearDeletionFile` |
| Lazy open | `BeTree::open` |
| Point resolve | `BeTree::resolve_fragment` |
| Streaming enum | `BeTree::iter_fragments` |
| IO-free counts | `count_fragments` / `count_rows` |
| Leaf metadata fidelity | DV / row-id / empty frag round-trip |

### Port next

| # | From tiered-manifest | Into betree | Notes |
|---|----------------------|-------------|-------|
| 1 | Commit handler / version CAS | Replace JSON `latest_hint` with Lance-style atomic publish | Needed before Dataset |
| 2 | `materialized_manifest` discipline | Writers never drop subtrees. Commit base = open + resolve/stream as needed. | Don't full-materialize every commit |
| 3 | Dataset spike | `add_columns` / append / delete → `BeTree::commit(actions)` | Research layout config string |
| 4 | Access-pattern tests | Port overwrite/restore/drop/cast/update intent onto betree actions | Cast may still be Remove+Add files |
| 5 | Index bitmap prune | `children_for_fragment_ids`-style on ChildRefs → load only covering leaves | Scanner-side, after resolve exists |
| 6 | Orphan GC | GC unreferenced `_bt/leaf\|node\|root` after version advance | COW creates garbage |

### Explicitly leave behind

- Sealed-immutable child as invariant
- Manifest DV as primary delete mechanism. Use `RemoveFragment` /
  `AddDeletionFile` actions.
- Protobuf fragment-blob children as the leaf format
- Competing tiered vs betree feature flags in production

### Integration picture

```text
┌─────────────────────────────────────────────────────────┐
│ Dataset / Transaction (from YOURS — thin wiring)        │
│   Operation → FragmentAction[] → BeTree::commit         │
│   open → BeTree::open (lazy)                            │
│   take/scan → resolve_fragment / iter_fragments         │
└──────────────────────────┬──────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────┐
│ Bε trunk (from JACK + EXTEND)                           │
│   root ε-buffer · flush · split/merge · Lance leaves    │
└─────────────────────────────────────────────────────────┘
```

---

## Part 3 — Success criteria

### A/B

- [ ] AB-TRICKLE, AB-APPEND, AB-ONESHOT, AB-OPEN, AB-RESOLVE filled for flat +
      tiered + betree
- [ ] `AB_BENCH.md` names a winner per scenario with numbers
- [ ] Verdict sentence: architecture stays Bε regardless of AB-APPEND optics

### Integrate

- [ ] Dataset spike: create tiered-like table that commits through `BeTree` for
      append + add_columns + delete
- [ ] Lazy open on that Dataset path. No full materialize on open.
- [ ] Tests: resolve matches materialize. Trickle backfill metadata bytes logged.
- [ ] Short `INTEGRATE_REPORT.md`: what ported, what dropped, AB summary

### Out of scope

- Merging to `main` / Jack's PR as production default
- Python/Java
- Full Iceberg stats plane
- Continuing AMT as alternate on-disk format

---

## Recommended order of work

```text
1. Add AB harness mode on betree side + notes to run tiered_manifest_bench with same N/F
2. Fill AB_BENCH.md. Confirm trickle winner. Decide nothing new architecturally.
3. Dataset spike on betree trunk (commit/open/resolve)
4. INTEGRATE_REPORT.md + optional reply blurb for #7499
```

---

## One-shot agent prompt

```text
Execute /Users/viltrum/workspace/lance-betree-extend/devtools/betree/INTEGRATE.md

Part 1: fair A/B flat vs tiered-manifest vs betree. AB-TRICKLE is the headline.
Part 2: integrate Dataset wiring from tiered into Bε trunk. Do NOT bring sealed AMT.
Produce AB_BENCH.md and INTEGRATE_REPORT.md.

Worktrees:
  betree:  /Users/viltrum/workspace/lance-betree-extend  (branch research/jack-betree-extend)
  tiered:  /Users/viltrum/workspace/lance @ tiered-manifest

No feature-flag ceremony. Prefer local FS measurements with GET/byte counts.
```
