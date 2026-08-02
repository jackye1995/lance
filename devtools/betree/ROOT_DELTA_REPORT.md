# ROOT_DELTA report, external root delta chain on the Bε manifest

**Goal file** [ROOT_DELTA.md](./ROOT_DELTA.md), executed 2026-08-02 on
`research/jack-betree-extend`, immediately after
[TXN_PARITY_REPORT.md](./TXN_PARITY_REPORT.md) passed. Baseline for every
before/after below is the txn-parity run
[txn_parity_local_20260802.csv](./bench-results/txn_parity_local_20260802.csv).

## Verdict

**Adopt behind the research flag.** Every kill criterion passed. With a
tail cap of 32, contiguous F=10 add-column trickle fell from 63,330 total
bytes per commit on the txn-parity baseline to 3,969, a 16.0× reduction
and roughly 1,128× under flat. Replace fell 15.5×, the mixed stream
22.7×, and scattered trickle and replace fell 17.4× and 11.8×. Commit
latency halved to about 0.43 ms because an ordinary commit is now a
single small PUT. Point resolve stayed at 2 GETs. The cost is cold open
with a long outstanding tail, 26 to 27 GETs instead of 2 in the worst
measured state, which the fold cap bounds and which parallel delta
fetches could largely hide. Adopt means keep `max_root_delta_tail` as an
opt-in knob on the research branch and carry it into any production
spike, not change any default.

## On-disk layout and relationship to TXN_PARITY objects

Delta roots live in the same versioned namespace as compacted roots,
`_bt/root/{version}.root`, not a separate `_bt/root_delta/` directory. That
deviation from the goal's strawman is deliberate. Publication stays a single
create-only PUT against one namespace, so two writers racing from the same
base version collide on the same path no matter which of them folds and
which appends a delta. A split namespace would let a folding writer and a
delta-appending writer both claim the same version.

A delta root sets `base_root_version` to the compacted root its chain
extends, carries no children, and its buffer holds only the actions
committed at that version, msn-tagged with their aggregate deltas. It also
carries the operation label, so **the delta root is that commit's
transaction record**. Ordinary delta commits write exactly one object and
skip the separate `_bt/txn/` envelope. Fold commits write the compacted
root plus the small txn envelope, because a compacted root's buffer mixes
residual actions from many commits and cannot serve as a per-commit audit
record.

## Fold policy

A commit appends a delta only while all of the following hold, and folds
into a compacted root otherwise.

1. The chain is enabled, `max_root_delta_tail > 0` in `BeTreeConfig`,
   persisted in the root so reopened sessions keep the policy.
2. The outstanding tail is below `max_root_delta_tail`. Benchmarks below
   use 32, half the 64 kill line.
3. The root buffer, including this commit's actions, is below the flush
   gate that would make the tree pipeline touch children.

Gate 3 is the safety property. The flush, split, merge, and root-shrink
pipeline runs only on fold commits, because a delta reader reconstructs
state as compacted root plus appended actions and must never observe
restructured children or a reordered buffer. Two in-tree hazards forced
this shape. `flush_internal` can repartition the buffer without writing
anything, and root shrink can replace the root's children with zero write
IO. Deferring the whole pipeline to the fold makes delta commits pure,
one PUT and no reads.

## Before/after tables

N=50K, F=10, 128 KiB budget, tail cap 32, median of three runs, caches
off, local filesystem. Baseline is txn-parity Bε, whose total is tree plus
txn. Delta-mode total is tree plus txn plus delta bytes. Flat unchanged,
shown for scale.

Contiguous locality.

| Scenario | Baseline total B | Delta total B | Reduction | Baseline ms | Delta ms | Folds | Max tail |
|----------|-----------------:|--------------:|----------:|------------:|---------:|------:|---------:|
| Add-column trickle, 500 commits | 63,330 | 3,969 | 16.0× | 0.82 | 0.42 | 16 | 32 |
| Replace trickle, 500 commits | 65,159 | 4,199 | 15.5× | 0.81 | 0.43 | 18 | 32 |
| Mixed stream, 1,000 commits | 77,345 | 3,405 | 22.7× | 0.85 | 0.43 | 31 | 32 |

Scattered locality.

| Scenario | Baseline total B | Delta total B | Reduction | Folds | Max tail |
|----------|-----------------:|--------------:|----------:|------:|---------:|
| Add-column trickle | 95,438 | 5,489 | 17.4× | 19 | 32 |
| Replace trickle | 104,953 | 8,876 | 11.8× | 17 | 32 |

## Read amplification

Open cost now depends on the outstanding tail. After the trickle,
replace, and mixed streams, which ended with tails of 24 to 25 deltas
outstanding, a cold open performed 26 to 27 GETs in 5 to 9 ms locally,
versus 2 GETs on the baseline. Each delta is roughly 1 to 1.6 KiB, so
the bytes are trivial and the cost is round trips. The current reader
fetches the chain sequentially. On an object store the right
implementation issues the range of versioned delta GETs concurrently,
since the chain bounds are known after reading the tip, which collapses
the wall-clock cost to about one extra round trip. The read matrix after
100 appends ended one commit past a fold, open was 3 GETs / 1.5 ms,
materialize 68 GETs / 23 ms at N=50K, and point resolve 2 GETs /
0.33 ms. Resolve is tail-independent because the session applies the
reconstructed buffer in memory.

## Kill criteria evaluation

1. **Tail must not exceed 64 without a fold.** Folds fired at the configured cap of 32 or earlier when the ε gate
   triggered, 16 to 31 folds per stream, and the maximum observed tail
   was exactly 32. Pass.
2. **Uncached point resolve at most 4 GETs.** 2.0 GETs in every scenario. Pass.
3. **Total bytes must not regress more than 20% without a read win.** Totals improved 11.8× to 22.7×, no regression anywhere. Pass.
4. **Resolve must not scan the full tail without pruning or a bounded
   fold.** The session holds the reconstructed buffer in memory and
   filters it by fragment id exactly as the pre-delta buffer overlay did,
   and the fold bound caps reconstruction at open. Pass by construction.

## Paste-ready note for the #7499 Q2 follow-up

> We measured the LSM complement on the Bε tree, Jack's external root
> delta chain. Ordinary commits publish one small delta object in the same
> create-only versioned namespace as roots, the delta doubles as the
> commit's transaction record, and the tree's flush pipeline runs only on
> fold commits, every 32 commits or when the ε gate fires. At N=50K, F=10
> contiguous add-column trickle, per-commit metadata went from 63,330
> bytes on the txn-parity path to 3,969 bytes, roughly 1,100× under flat, with
> point resolve still 2 GETs and open reading the compacted root plus the
> outstanding tail. This supports the complement-not-substitute read on
> the LSM question, the keyed tree stays, and the delta chain removes the
> per-commit root rewrite.

## Artifacts

- [bench-results/root_delta_local_20260802.csv](./bench-results/root_delta_local_20260802.csv)
- New tests, delta deferral and fold, reopen with outstanding deltas
  including GC survival, and single-winner publication on the shared
  versioned path
- Knobs, `BeTreeConfig::with_root_delta_tail`, harness `AB_ROOT_DELTA_TAIL`,
  new CSV columns `delta_bytes_avg`, `folds`, `max_delta_tail`
