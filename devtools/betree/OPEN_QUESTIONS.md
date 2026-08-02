# Goal: Answer Will’s open questions and decide what still blocks landing

**Context:** Discussion [#7499](https://github.com/lance-format/lance/discussions/7499)  
**Will’s comment:** `wjones127` on 2026-07-28 in [#7499](https://github.com/lance-format/lance/discussions/7499) (append-only? LSM? ZSTD?)  
**Jack’s draft PR:** [#7848](https://github.com/lance-format/lance/pull/7848)  
**Local trunk:** `/Users/viltrum/workspace/lance-betree-extend` @ `research/jack-betree-extend`  
**Prior evidence:** [AB_BENCH.md](./AB_BENCH.md), [DESIGN_REVIEW.md](./DESIGN_REVIEW.md), [INTEGRATE_REPORT.md](./INTEGRATE_REPORT.md)

**Hand this whole file to an agent as the goal.**

---

## Mission

Figure out, with evidence, whether keyed write-buffered manifests (Jack’s Bε
prototype + our extensions) are worth continuing — and what is still missing
before anyone should talk about landing.

Will asked three things. Treat them as the agenda, not as attacks:

1. **Updates / schema evolution:** is this only useful for append-only, or also
   for regular add-column and data replacement?
2. **LSM alternative:** would an LSM-style manifest delta log get similar
   append wins and better update / schema-evolution behavior?
3. **Compression:** should manifests use ZSTD (or similar)? How does that
   interact with layout choice?

Also answer the meta-question the human cannot yet defend in a room:

4. **Landing readiness:** what is complete, what is prototype-only, and what
   kill criteria should gate any production spike?

**Do not** decide “Bε is the final architecture.”  
**Do** produce a short report that a quiet engineer can paste to the team:
what we know, what we don’t, what experiment closes each gap.

---

## Stance

| Do | Do not |
|----|--------|
| Falsify or support Will’s concerns with measurements or tight design analysis | Claim the design is ready to land |
| Compare mechanisms on the same workloads | Debate aesthetics of Bε vs LSM without a workload |
| Prefer real commit-path evidence over research-adapter-only | Pretend local adapter latency = production Dataset latency |
| Keep flat as the default baseline | Revive sealed-AMT as a competing format to ship |
| Steal useful ideas from LSM / compression into the plan | Require a full LSM rewrite before answering (1) |

Working hypothesis to test, not to preach:

> Under fine-grained metadata commits, write cost should track the touched
> fragment window, not table size. Keyed write buffers are one mechanism.
> Global LSM deltas and compression are still open.

---

## Background the agent must internalize

### Already measured (do not redo from scratch)

- Matched local flat / tiered / Bε harness: trickle F=10 stays near
  update-window scale for Bε; flat and current tiered `Merge` path do
  table-scale work. See `AB_BENCH.md`.
- One-shot whole-table touch: Bε can win bytes, flat wins wall-clock.
- Read tradeoff: matched 128/128 has high materialize GET count; 64 KiB
  buffer / 1 MiB leaf cuts materialize GETs and raises uncached point latency.
- Hardening already landed on the research branch: routing fences, wire-accurate
  internal sizing, separate leaf limit, fresh-append aggregate deltas.

### Will’s questions (verbatim summary)

- Useful only for append-only? What about regular add-column + data replacement?
- Considered LSM-tree style? Roughly equivalent on append, maybe better on
  updates / schema evolution?
- Considered ZSTD on uncompressed protobuf manifests?

### Partial public answers already on the thread

- Xuanwo posted compression / Lance-file size and decode numbers on the same
  discussion. Incorporate them; do not ignore them. Re-verify only if needed
  for a fair comparison in this repo.

### Known completeness gaps

- Bε research adapter still lacks a production transaction record comparable
  to Dataset `Merge` / txn files.
- Object-store latency matrix for our extended branch is thinner than Jack’s
  AWS numbers on the draft PR.
- No head-to-head “manifest LSM” implementation in-tree yet — may need a
  minimal strawman or a design-cost analysis if a full build is too large.
- Landing story (compat, opt-in, scanners, indexes) is not written as a
  go/no-go checklist.

---

## Part 1 — Workload taxonomy (half day)

Define 5–7 concrete metadata workloads. For each: who initiates it in Lance
today, which fragments/files change, whether schema changes, and whether a
naive implementation touches O(window) or O(table).

Required rows:

| ID | Workload | Example |
|----|----------|---------|
| W1 | Pure append | new fragments only |
| W2 | Fine-grained add-column / backfill | F≪N fragments gain a data file |
| W3 | Coarse / one-shot add-column | all N fragments gain a data file in one commit |
| W4 | Selective data replacement | replace files on a fragment subset |
| W5 | Whole-table overwrite / restore | global rewrite |
| W6 | Mixed update stream | alternating append + F=10 backfill + occasional replace |
| W7 | Schema evolution burst | repeated add-column rounds over days |

Deliverable section in the report: which workloads Bε is hypothesized to win,
lose, or be unclear — **before** new benches.

---

## Part 2 — Answer Will Q1 with evidence

### Goal

Show whether Bε’s advantage survives non-append workloads on the best harness
we can run in this worktree.

### Minimum experiments

Reuse / extend `betree_ab` or `BeTreeDataset` so these are first-class
scenarios, not hand-wavy:

1. **Add-column trickle:** N=50K (or largest reliable local N), F=10, ≥500
   commits, each commit adds one data file to F existing fragments.
2. **Replace-file trickle:** same shape, but replace an existing data file /
   drop+add on F fragments.
3. **One-shot add-column:** one commit touches all N.
4. **Mixed stream:** 70% append, 25% F=10 add-column, 5% selective replace,
   ≥1,000 commits after bootstrap.

Report per scenario: `tree_bytes/commit`, `commit_ms`, flushes, and for reads
after the stream: open GETs/ms, resolve GETs/ms.

Controls from `AB_BENCH.md` where possible: caches off, median of 3, shared
budget, fabricated metadata, Rust 1.97 bench profile.

### Interpretation bar

- If add-column trickle and replace trickle stay near update-window scale vs
  flat, Q1 answer is: **not append-only**.
- If they collapse to table-scale on the real-ish path, document why (action
  shape, txn boundary, leaf rewrite) and say so plainly.
- One-shot / overwrite may lose latency; that is an adverse result to keep
  visible, not hide.

---

## Part 3 — Answer Will Q2 (LSM) without boiling the ocean

### Goal

Decide whether “just use an LSM” is a substitute, a complement, or a rename.

### Required analysis (design, must be specific)

Write a mechanism comparison table:

| Concern | Flat | Bε (keyed buffers) | Manifest LSM (global delta levels) |
|---------|------|--------------------|-------------------------------------|
| Pure append commit write | | | |
| F≪N add-column | | | |
| Point resolve / fragment lookup | | | |
| Full materialize / scan planning | | | |
| Compaction debt | | | |
| Concurrent commit / CAS | | | |
| Fit with Lance txn model | | | |

Define “manifest LSM strawman” explicitly, even if unimplemented:

- append-only action log segments
- periodic compaction into leveled snapshots or sorted runs
- reader merges until compaction catches up

### Optional code spike (only if Part 2 is done and time remains)

A minimal in-worktree strawman is allowed if it can answer one discriminating
question cheaply, for example:

> For F=10 add-column × 500 commits at N=50K, does a naive global delta log
> keep commit writes small **and** keep point resolve ≤ a small constant
> without building a fragment-id index?

If the strawman needs a fragment-id index to stay fast, say that — that index
is converging back toward a keyed tree.

### Interpretation bar

- **Substitute:** LSM alone matches Bε on W2/W4 and stays simple on reads.
- **Complement:** LSM deltas help root/commit publication, but keyed structure
  still needed for F≪N updates.
- **Rename:** Bε already is the keyed write-optimized structure; LSM language
  without keys reintroduces merge amp.

Jack’s external root delta chain and per-node immutable message runs count as
LSM-shaped complements. Analyze them in those terms.

---

## Part 4 — Answer Will Q3 (compression)

### Goal

Separate “smaller objects” from “smaller rewrite amplification.”

### Work

1. Summarize Xuanwo’s thread numbers and Jack’s columnar-leaf compression
   behavior.
2. If easy in this repo, measure flat protobuf size vs protobuf+zstd (and
   current Bε leaf/node sizes) on the same fabricated N=50K or N=100K
   metadata. One table is enough.
3. State clearly:
   - compression helps absolute size / maybe S3 thresholds
   - compression does **not** by itself stop flat from rewriting the full
     blob every commit
   - recommendation: independent cheap win for flat; not a reason to stop
     structural work

---

## Part 5 — Landing gap checklist

Produce a go/no-go inventory. Each row: status (`done` / `prototype` /
`missing`), evidence link, blocker for production opt-in.

Minimum rows:

- leaf fidelity (DV, row-id, empty fragments)
- lazy open / resolve / stream
- concurrent create-only root publish + conflict
- orphan GC contract
- Dataset / CommitBuilder production wiring
- transaction record parity with today’s commits
- scanner + index call-site adoption
- old-reader fail-closed / opt-in config
- object-store evidence for trickle + resolve
- adverse-case policy (one-shot, scattered, materialize fanout)
- migration / maintenance path back toward flat or dual-read

End with a recommended **team ask** that does **not** require declaring the
design complete. Example shape:

> Approve N days to close Will Q1 on a real-ish commit path. Flat stays
> default. Kill if fine-grained add-column/replace lose the update-window
> byte property on that path.

---

## Deliverables

1. **`devtools/betree/OPEN_QUESTIONS_REPORT.md`**
   - Executive answer to Will Q1/Q2/Q3 in ≤1 page
   - Workload taxonomy
   - New measurements + commands to reproduce
   - LSM comparison table + verdict (substitute / complement / rename)
   - Compression table + verdict
   - Landing gap checklist
   - Exact recommended team ask + kill criteria
   - Draft reply blurb for Will (optional appendix)

2. **CSV / bench artifacts** under `devtools/betree/bench-results/` if new
   runs are produced. Name with date suffix.

3. **Code only as needed** to make Part 2 (and optional Part 3 spike)
   measurable. Prefer extending `betree_ab` / `BeTreeDataset` over new
   frameworks. No production default change. No sealed-AMT revival.

---

## Success criteria

- [ ] Will Q1 answered with at least add-column trickle + replace trickle +
      one-shot numbers vs flat (tiered optional if costly)
- [ ] Will Q2 answered with an explicit substitute/complement/rename verdict
      and a mechanism table; strawman code optional
- [ ] Will Q3 answered with compression vs rewrite-amp separation; Xuanwo’s
      data incorporated
- [ ] Landing checklist states clearly: not ready to land, and why
- [ ] Report includes a bounded team ask a quiet person can send unchanged
- [ ] `cargo test -p lance-table betree` still passes if code changed
- [ ] No claim that Bε is intrinsically N× faster; stick to workload +
      implementation findings

---

## Order of work

```text
1. Read AB_BENCH.md, DESIGN_REVIEW.md, INTEGRATE_REPORT.md, discussion #7499
2. Part 1 workload taxonomy
3. Part 2 extend harness + run update/schema scenarios
4. Part 4 compression (quick; can parallelize with Part 2 writeup)
5. Part 3 LSM analysis (+ optional strawman only if discriminating)
6. Part 5 landing gaps + team ask
7. OPEN_QUESTIONS_REPORT.md
```

---

## Out of scope

- Merging to `main` or changing Dataset defaults
- Posting on GitHub unless the human asks
- Full production LSM implementation
- Python / Java bindings
- Reviving sealed children as the ship vehicle
- Perfecting leaf-size tuning as if it were the format decision

---

## One-shot agent prompt

```text
Execute /Users/viltrum/workspace/lance-betree-extend/devtools/betree/OPEN_QUESTIONS.md

Will (wjones127) asked on discussion #7499:
1) append-only vs add-column/replace
2) LSM instead of Bε
3) ZSTD/compression on manifests

Also answer: is this complete enough to land? If not, what bounded ask closes
the next gap?

Produce OPEN_QUESTIONS_REPORT.md with evidence, kill criteria, and a paste-ready
team ask. Do not declare Bε the final architecture. Do not post to GitHub.

Worktree: /Users/viltrum/workspace/lance-betree-extend
Branch: research/jack-betree-extend
```
