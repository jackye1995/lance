# Answers to Will's open questions on keyed write-buffered manifests

**Context.** Will asked three questions on discussion
[#7499](https://github.com/lance-format/lance/discussions/7499) about the Bε
manifest work in Jack's draft PR
[#7848](https://github.com/lance-format/lance/pull/7848) and this branch's
extensions. This report answers them with new measurements from
`research/jack-betree-extend`, plus a landing-gap inventory and a bounded team
ask. Nothing here claims the design is ready to land.

All new numbers come from the matched local harness described in
[AB_BENCH.md](./AB_BENCH.md). Controls were unchanged. Fabricated metadata
only, local filesystem, Rust 1.97 with Cargo's bench profile, fanout 16,
128 KiB shared node budget, metadata caches off, median of three independent
runs. Raw rows are checked in under
[bench-results/](./bench-results/) with a 20260801 date suffix.

---

## Executive answers

**Q1, append-only or not.** Not append-only. On this branch's research
adapter, fine-grained add-column trickle wrote 61.0 KiB per commit against
flat's 4,372.9 KiB, replace-file trickle wrote 62.1 KiB against 4,183.2 KiB,
and a mixed stream of 70% append, 25% add-column, 5% replace wrote 75.2 KiB
against 4,306.6 KiB. All three stayed at update-window scale while flat did
table-scale work on every commit. The adverse cases stay adverse. A one-shot
add-column that touches all 50K fragments favors flat wall clock, roughly
14 ms versus 78 to 168 ms depending on Bε leaf sizing, and scattered updates
narrow the byte advantage to 47× on add-column and 41× on replace, from 72×
and 67× contiguous. The one caveat that matters is that the adapter has no
production transaction record yet, so these are storage-layout numbers, not
end-to-end Dataset commit numbers.

**Q2, LSM instead.** Complement, partly rename, not a substitute. A global
manifest delta log makes commits cheap but leaves point resolve unbounded
between compactions unless it maintains a fragment-id index, and a leveled
LSM keyed by fragment id is the same design family as a Bε tree. Jack's
proposed external root delta chain is the LSM-shaped piece worth taking. It
would remove the per-commit root-buffer rewrite that currently dominates Bε
commit bytes. Staging updates in a global log does not make add-column or
replacement cheaper than keyed staging does, because both record the same
actions. The full mechanism table is below.

**Q3, compression.** Worth doing for flat regardless, and orthogonal to the
structural question. Xuanwo measured zstd level 1 at 31% of raw protobuf on a
1M-fragment manifest. Our local probe at N=50K agrees, zstd level 1 reached
26% of raw at both one and two data files per fragment. Compression shrinks
the object but flat still rewrites the whole object every commit, so
per-commit write volume stays table-scale, roughly 1.1 MiB
compressed versus Bε's 62 KiB on the same workload. Bε leaves are Lance files
and already get columnar compression. Recommendation is to pursue zstd for
flat manifests as an independent cheap win and keep it out of the
Bε-versus-flat decision.

**Q4, landing readiness.** Not ready to land, and nobody should claim
otherwise. The prototype is a hardened research adapter in `lance-table` with
tests for leaf fidelity, lazy reads, concurrent publication, and offline GC.
It lacks a production transaction record, Dataset and CommitBuilder wiring,
scanner and index adoption, an opt-in config with old-reader fail-closed
behavior, and object-store evidence on this branch. The bounded ask at the
end of this report closes the highest-risk gap first.

---

## Part 1, workload taxonomy

Hypotheses were written before the new runs. W2 and W3 map to existing
scenarios. W4 and W6 are the new ones this report adds.

| ID | Workload | Who initiates it today | Touched scope | Schema change | Naive flat cost | Hypothesis before measuring |
|----|----------|------------------------|---------------|---------------|-----------------|------------------------------|
| W1 | Pure append | streaming ingest, `Dataset::write` append mode | new fragments only | no | O(table) rewrite | Bε wins |
| W2 | Fine-grained add-column backfill | embedding and feature backfill jobs, `add_columns` over F≪N fragments per commit | F fragments gain a data file | once per column round | O(table) per commit | Bε wins |
| W3 | Coarse one-shot add-column | ALTER-style add column materialized in one commit | all N fragments gain a data file | yes | O(table) once | flat wins latency, Bε may win bytes |
| W4 | Selective data replacement | compaction of a fragment subset, update rewrites, file-level cast | F fragments swap a data file | no | O(table) per commit | Bε wins, untested before this report |
| W5 | Whole-table overwrite or restore | overwrite mode, restore to version | every fragment | maybe | O(table), inherent | flat wins, global is global |
| W6 | Mixed update stream | production feeds, append-heavy with periodic backfill and compaction | varies per commit | occasionally | O(table) per commit | Bε wins, untested before this report |
| W7 | Schema evolution burst | repeated add-column rounds over days | N fragments per round, spread over many commits | repeatedly | O(table) per commit | Bε should win, leaf growth and flush debt are the risk |

W7 has partial evidence rather than a dedicated bench. The equivalence test
in `betree/mod.rs` replays three full backfill rounds over a multi-level tree
and asserts state equality with flat, and the trickle scenarios measure one
round's steady cost. A multi-day burst bench remains future work.

---

## Part 2, Will Q1 measurements

New scenarios `AB-REPLACE` and `AB-MIXED` were added to the
`betree_ab` harness beside the existing `AB-TRICKLE` add-column scenario.
Replace drops the fragment's current base file and attaches a fresh one
through the same remove-plus-add actions the adapter exposes for cast and
update intent. Mixed interleaves 70% appends, 25% F=10 add-column commits,
and 5% F=10 replace commits over 1,000 commits. Every mutation scenario now
also measures reads after the stream, a lazy reopen plus 100 uncached point
resolves.

N=50K, F=10, 128 KiB budget, contiguous locality, median of three runs.

| Scenario | Layout | KiB/commit | ms/commit | Flushes | Open GETs / ms | Resolve GETs / ms |
|----------|--------|-----------:|----------:|--------:|---------------:|------------------:|
| Add-column trickle, 500 commits | flat | 4,372.9 | 6.66 | 0 | 3 / 13.9 | 0 / 0.00 |
| | Bε | 61.0 | 0.52 | 7 | 2 / 4.6 | 2.00 / 0.39 |
| Replace trickle, 500 commits | flat | 4,183.2 | 6.45 | 0 | 3 / 12.8 | 0 / 0.00 |
| | Bε | 62.1 | 0.53 | 12 | 2 / 5.1 | 2.00 / 0.35 |
| Mixed stream, 1,000 commits | flat | 4,306.6 | 6.78 | 0 | 3 / 14.9 | 0 / 0.00 |
| | Bε | 75.2 | 0.53 | 9 | 2 / 8.2 | 2.00 / 0.39 |

Scattered locality, same shape.

| Scenario | Layout | KiB/commit | ms/commit | Flushes |
|----------|--------|-----------:|----------:|--------:|
| Add-column trickle | flat | 4,372.9 | 9.77 | 0 |
| | Bε | 92.4 | 0.83 | 7 |
| Replace trickle | flat | 4,183.2 | 9.68 | 0 |
| | Bε | 100.9 | 0.96 | 34 |
| Mixed stream | flat | 4,306.6 | 10.15 | 0 |
| | Bε | 87.9 | 0.83 | 5 |

Scattered replacement is the closest case for Bε at this budget, 41× less
write than flat with 34 flushes across 500 commits, because remove-plus-add
pairs land on 10 unrelated subtrees per commit and push buffers over the
flush gate sooner. It still writes 41× less than flat's table-scale
rewrite.

One-shot add-column numbers carry over from [AB_BENCH.md](./AB_BENCH.md).
Flat finished the 50K-fragment one-shot in 14.2 ms at 7,943.0 KiB. Bε wrote
fewer bytes, 4,539.5 KiB, but took 167.8 ms at the matched 128 KiB
configuration and 78.4 ms at the 64/1024 split configuration.

**Q1 interpretation.** Add-column trickle, replace trickle, and the mixed
stream all stayed near update-window scale on this branch. The answer to
"only useful for append-only" is no on this harness. Two qualifications
stand. First, the harness commits through the research adapter, which has no
production transaction record. If a production commit serializes a
table-scale transaction file the way today's tiered `Merge` path does, the
advantage collapses, which is exactly what the tiered measurement showed.
Second, one-shot and whole-table operations remain flat's territory and any
landing plan needs an explicit policy for them.

Reproduce with

```bash
NUM_FRAGMENTS=50000 \
AB_SCENARIOS=trickle,replace,mixed \
AB_TRICKLE_COMMITS=500 \
AB_MIXED_COMMITS=1000 \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=128 \
AB_LOCALITY=contiguous \
AB_CSV=devtools/betree/bench-results/update_ab_local_20260801.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab
```

and the same command with `AB_LOCALITY=scattered`.

---

## Part 3, Will Q2, the LSM alternative

### Strawman definition

A manifest LSM here means the following, none of it implemented.

1. Every commit PUTs one action-log segment, a small object listing that
   commit's fragment actions, under a versioned create-only name.
2. The latest segment name doubles as the commit point, so CAS works the
   same way root publication works today.
3. Readers apply segments newest-first over the last compacted snapshot.
4. Background compaction folds segments into sorted runs keyed by fragment
   id and eventually into a fresh full snapshot.

### Mechanism comparison

| Concern | Flat | Bε keyed buffers | Manifest LSM global delta levels |
|---------|------|------------------|----------------------------------|
| Pure append commit write | O(table) rewrite, 4.4 MiB measured | O(ε) root rewrite, 32 to 62 KiB measured | O(commit) segment append, smallest possible |
| F≪N add-column | O(table), 4.4 MiB measured | O(window), 62.5 KiB measured | O(commit) segment append |
| Point resolve | eager full read, then in-memory | height-bounded GETs, 2 GETs / 0.4 ms measured | snapshot plus every unmerged segment, GETs grow with commit count until compaction runs |
| Full materialize, scan planning | 1 GET | leaves plus buffered actions, 16 to 136 GETs by leaf size | snapshot plus segments, reader performs a global key merge |
| Compaction debt | none | paid inside commits, visible in flush counts and commit p95 | deferred to background, read amplification grows while it lags |
| Concurrent commit CAS | create-only manifest PUT | create-only root PUT, ten-writer test in-tree | create-only segment PUT, rebase is segment replay, easiest of the three |
| Fit with Lance txn model | manifest is the state | actions mirror transaction ops, txn record parity still missing | segments are transaction records, closest fit |

### Verdict

Substitute, only if reads can tolerate merging an unbounded segment tail or
the log grows a fragment-id index. The moment the strawman adds leveled
sorted runs keyed by fragment id to bound point reads, it is a keyed
write-optimized tree, which is the Bε family with different vocabulary.

Complement, yes, and worth stealing. An external root delta chain, which
Jack already proposed on the PR, is the LSM idea applied to root
publication. It replaces the per-commit ε-buffer rewrite with a single
appended delta, which his own numbers project at roughly 8,000× less than
flat for F=10 backfill. Per-node immutable message runs are the same idea
applied per key range. Both keep the keyed routing structure and therefore
keep bounded point reads.

On Will's specific hypothesis that LSM handles updates and schema evolution
better, the measured answer is that keyed buffers already keep those
workloads at update-window scale. Both designs ship the same actions. The
difference is where they wait and how much the reader pays later.

---

## Part 4, Will Q3, compression

Xuanwo's thread numbers on a 1M-fragment manifest. Zstd level 1 reached 31%
of raw protobuf, LZ4 42%, and a Lance-file encoding 46% with a 6× decode
speedup, 297 ms to 49 ms.

Local probe on this branch at N=50K, one fabricated table, flat manifest raw
versus zstd, next to Bε total tree output for the same content.

| Table state | Layout and codec | Bytes | Share of raw | Encode ms | Decode ms |
|-------------|------------------|------:|-------------:|----------:|----------:|
| bootstrap, 1 file per fragment | flat raw | 4,283,603 | 100% | | |
| | flat zstd level 1 | 1,091,543 | 25.5% | 7.30 | 3.09 |
| | flat zstd level 3 | 1,142,640 | 26.7% | 10.75 | 3.51 |
| 2 files per fragment | flat raw | 8,133,603 | 100% | | |
| | flat zstd level 1 | 2,119,226 | 26.1% | 14.41 | 5.80 |
| | flat zstd level 3 | 2,212,029 | 27.2% | 22.94 | 6.99 |
| 2 files per fragment | Bε total tree, Lance columnar leaves | 5,012,323 | 61.6% | | |

Level 3 came out larger than level 1 on this data, so level 1 is the right
default for manifests. Encode cost at N=50K was 14 ms on the 7.8 MiB
manifest, about two flat commits' worth of latency, for a 74% PUT byte
reduction. The Bε row is total on-disk tree output for the same fragments.
Flat raw is 1.6× larger at this scale, the same direction as Jack's 2.4× at
one billion data files, and zstd on flat beats both on pure size.

### Verdict

Compression solves object size, not rewrite amplification. Flat with zstd
still writes the full compressed manifest on every commit, table-scale work
at roughly 1.1 MiB per commit on the trickle workload where Bε
writes 62 KiB. It also cuts S3 request cost and may matter for latency
thresholds, so it is an independent cheap win for the flat default and
should proceed regardless of any structural decision. It is not a reason to
stop structural work, and it is not specific to flat either, since Bε
internal protobuf nodes could adopt the same codec while its leaves already
compress columnar.

Reproduce with

```bash
NUM_FRAGMENTS=50000 \
AB_SCENARIOS=compress \
AB_NODE_SIZE_KIB=128 \
AB_COMPRESS_CSV=devtools/betree/bench-results/compress_local_20260801.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab
```

---

## Part 5, landing gap inventory

Status values are done, prototype, or missing. Done means proven at the
research boundary, not production-ready.

| Item | Status | Evidence | Blocker for production opt-in |
|------|--------|----------|-------------------------------|
| Leaf fidelity, deletion files, row-id meta, empty fragments | prototype | `store.rs` leaf round-trip test | needs production-schema review, none known missing |
| Lazy open, point resolve, streaming enumeration | prototype | tracked-store tests, `AB-OPEN` and `AB-RESOLVE` rows | scanner call sites must actually use them |
| Concurrent create-only root publish with conflict | prototype | ten-writer publication test | conflict resolution beyond first-writer-wins is undesigned |
| Orphan GC contract | prototype | `gc_unreferenced_offline` plus test | offline-only, requires writer quiescence, no online story |
| Dataset and CommitBuilder production wiring | missing | intentional scope cut, see [INTEGRATE_REPORT.md](./INTEGRATE_REPORT.md) | the whole item |
| Transaction record parity with today's commits | missing | Bε bytes exclude any txn file, tiered's txn file erased its delta advantage | biggest validity risk for every number in this report |
| Scanner and index call-site adoption | missing | none | full-materialize call sites would erase lazy-read wins |
| Old-reader fail-closed and opt-in config | missing | adapter rejects non-betree layout values, nothing in production config | format governance decision |
| Object-store latency evidence for this branch | missing | Jack's AWS numbers cover his prototype on #7848, this branch is local-only | trickle and resolve need S3 runs on this branch |
| Adverse-case policy, one-shot, scattered, materialize fanout | prototype | measured and documented here and in [AB_BENCH.md](./AB_BENCH.md) | needs a decision, for example route whole-table ops through a flat-style rebuild |
| Migration and maintenance path back to flat or dual-read | missing | none on this branch | opt-out story required before any opt-in ships |

---

## Recommended team ask

> Approve about five working days to close Will Q1 on a real-ish commit
> path. Scope, in order. One, a minimal Bε transaction record with byte
> parity accounting so tree bytes plus txn bytes is the reported number.
> Two, a CommitBuilder-shaped opt-in path in `lance` behind the existing
> layout key, no default change. Three, rerun add-column trickle, replace
> trickle, and the mixed stream through that path locally and against S3
> with the existing harness. Flat stays the default throughout.
>
> Kill criteria. Stop the structural track and fall back to flat plus zstd
> if fine-grained add-column or replace on that path loses the
> update-window byte property, concretely if per-commit tree plus
> transaction bytes at N=50K, F=10 exceed ten times the research-adapter
> result, or if uncached point resolve needs more than four GETs at the
> chosen leaf size, or if backfill conflict rebase cannot be expressed as
> action replay.

Compression is a separate, smaller ask. Prototype zstd for flat manifests
behind a writer feature flag and measure read-side cost on open. That work
is justified by Xuanwo's numbers alone and does not wait on anything above.

---

## Appendix, draft reply blurb for Will

> Good questions, we measured rather than argued. On append-only, no. We
> added replace-file and mixed-stream scenarios to the matched harness. At
> N=50K, F=10 over 500 commits, add-column trickle writes 61.0 KiB per
> commit and replace trickle 62.1 KiB versus flat's 4.4 MiB and 4.2 MiB. A
> mixed stream of 70% append, 25% add-column, 5% replace over 1,000 commits
> writes 75.2 KiB per commit versus 4.3 MiB. Whole-table one-shots still
> favor flat wall clock and we treat that as a policy question, not a
> footnote. Caveat, these run through the research adapter, which has no
> production transaction record yet, so we are proposing to close exactly
> that gap next before anyone talks about landing.
>
> On LSM, our read is complement rather than substitute. A global delta log
> keeps commits small but point resolve then pays every unmerged segment,
> and once you add fragment-id-keyed sorted runs to fix that you have
> rebuilt a keyed write-buffered tree. The LSM idea we do want is the
> external root delta chain Jack proposed, which removes the per-commit
> root-buffer rewrite, and possibly per-node immutable message runs later.
>
> On zstd, yes, independently of everything else. Xuanwo's numbers plus our
> local probe agree it cuts the flat manifest to roughly a third. It does
> not change rewrite amplification, flat with zstd still writes a
> table-scale object every commit, so we see it as a cheap win for the flat
> default rather than an alternative to structural work.

---

## Artifacts

- [bench-results/update_ab_local_20260801.csv](./bench-results/update_ab_local_20260801.csv), 36 rows, trickle rerun plus the new replace and mixed scenarios, both localities
- [bench-results/compress_local_20260801.csv](./bench-results/compress_local_20260801.csv), compression probe
- Harness changes on this branch, `AB-REPLACE` and `AB-MIXED` scenarios, post-stream read measurement in every mutation scenario, `FlatBaseline::commit_replace_data_files`, `make_replacement_data_file`, and a replace round in the flat-equivalence test
