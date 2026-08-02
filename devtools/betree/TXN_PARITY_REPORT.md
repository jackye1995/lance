# TXN_PARITY report, transaction record parity for the Bε manifest

**Goal file** [TXN_PARITY.md](./TXN_PARITY.md), executed 2026-08-02 on
`research/jack-betree-extend`. Prior baseline
[OPEN_QUESTIONS_REPORT.md](./OPEN_QUESTIONS_REPORT.md) and
[update_ab_local_20260801.csv](./bench-results/update_ab_local_20260801.csv).

## Verdict

**PASS.** Every kill criterion cleared with wide margin. Contiguous
add-column trickle totals 61.8 KiB per commit against a 610 KiB kill line,
replace totals 63.6 KiB against 620 KiB, point resolve stays at 2 GETs
against a 4-GET line, and the transaction record encodes only the commit's
actions, byte-identical across a 10× table-size difference. The
update-window byte property survives a per-commit transaction record.

## What was implemented

**Transaction record.** Every successful `BeTree::commit` now publishes
`_bt/txn/{version}.txn`, a `BeTreeTransaction` protobuf holding version,
base version, an operation label, and the commit's `FragmentAction` list.
It is written only after the version's root publication wins its
create-only race, so a conflicted commit never leaves a transaction record
at the tip. The ten-writer concurrency test now asserts that. This is a
research parity artifact, not a final format. A production integration
would reconcile it with `lance.format.pb.Transaction`.

**Byte accounting.** `CommitStats` now separates `tree_write_bytes` from
`transaction_bytes` and exposes `total_bytes()`. The `betree_ab` harness
populates the previously flat-only `transaction_bytes_avg` CSV column for
Bε rows, and `total_bytes_avg` is the number every update-window judgment
below uses.

**CommitBuilder-shaped opt-in, honest scope B.** A new
`betree::commit_builder` module provides `ManifestLayout::from_config`,
which treats an unset `lance.manifest.layout` as flat, accepts `flat` and
`betree`, and rejects anything else with a clear error, plus an
`Operation` intent enum and a `CommitBuilder` whose `execute` translates
intents into actions and commits through the transaction-aware path. The
benchmark now commits through this surface, so the measured path is the
opt-in path. This is not `lance::dataset::CommitBuilder` integration. The
next step remains wiring `ManifestLayout` and these intents into the
production builder and `Dataset` call sites.

**Tests.** Four new tests beside the 26 existing ones. Transaction records
per operation kind with version chaining, transaction size invariance to
table size, open reading only the latest root with no transaction history,
and layout selection plus intent execution through the builder. The
conflict test gained transaction-tip assertions.

## Byte parity tables

N=50K, F=10, 128 KiB budget, median of three runs, caches off, local
filesystem. Bytes are per commit. Baseline columns come from
[update_ab_local_20260801.csv](./bench-results/update_ab_local_20260801.csv),
which had no transaction record.

Contiguous locality.

| Scenario | Bε tree B | Bε txn B | Bε total B | Baseline tree B | Kill line | Flat total B | ms (was) |
|----------|----------:|---------:|-----------:|----------------:|----------:|-------------:|---------:|
| Add-column trickle, 500 commits | 62,475 | 856 | 63,330 | 62,475 | 624,750 | 4,477,891 | 0.82 (0.52) |
| Replace trickle, 500 commits | 63,584 | 1,575 | 65,159 | 63,585 | 635,850 | 4,283,604 | 0.81 (0.53) |
| Mixed stream, 1,000 commits | 76,980 | 364 | 77,345 | 76,980 | 769,800 | 4,409,964 | 0.85 (0.53) |

Scattered locality.

| Scenario | Bε tree B | Bε txn B | Bε total B | Baseline tree B | Flat total B |
|----------|----------:|---------:|-----------:|----------------:|-------------:|
| Add-column trickle | 94,575 | 863 | 95,438 | 94,575 | 4,477,891 |
| Replace trickle | 103,364 | 1,589 | 104,953 | 103,363 | 4,283,604 |
| Mixed stream | 89,998 | 367 | 90,365 | 89,998 | 4,409,964 |

Post-stream reads stayed at 2 open GETs and 2.0 resolve GETs per point
lookup for Bε in every scenario, unchanged from the baseline and inside
the 4-GET kill line.

## Kill criteria evaluation

1. **Total bytes within 10× of adapter baseline.** Contiguous trickle totals 63,330 bytes, 1.01× the 62,475-byte baseline and about a tenth of the 624,750-byte kill line. Replace totals 65,159 bytes, 1.02× its baseline. Mixed totals 77,345 bytes, 1.005× its baseline. Pass.
2. **Point resolve at most 4 GETs.** 2.0 GETs in every scenario. Pass.
3. **Transaction encodes actions only, never the fragment list.** The
   record holds the commit's `FragmentAction`s and two version integers.
   The `transaction_bytes_track_actions_not_table_size` test pins
   byte-identical records at N=400 and N=4,000. Pass by construction and
   by test.

## Costs worth stating

The transaction record is one extra PUT per commit. On the local
filesystem that raised Bε commit latency from roughly 0.5 ms to roughly
0.81 to 0.85 ms in these scenarios. On an object store this is one more small
request per commit, either serial latency or a parallel PUT. The
[ROOT_DELTA](./ROOT_DELTA.md) follow-up folds the per-commit action record
and the commit point into one object, which is the right shape if this
cost matters.

S3 numbers were not collected. No AWS credentials were available in this
environment, so this run is local-only per the goal's fallback.

## Paste-ready status blurb

> Update on the transaction-record gap flagged in the open-questions
> report. The Bε research path now writes a per-commit transaction record,
> actions plus version chain only, published after the create-only root
> CAS so conflicts cannot leave a stale txn tip. The harness now reports
> tree, txn, and total bytes separately, and the totals still hold the
> update-window property. Contiguous F=10 add-column trickle at N=50K is
> 61.8 KiB total per commit versus flat's 4.4 MiB, replace is 63.6 KiB
> versus 4.2 MiB, and a 70/25/5 mixed stream is 75.5 KiB versus 4.4 MiB,
> all roughly 57 to 71× under flat with the txn record included. Point
> resolve stays at 2 GETs. The commit surface is a CommitBuilder-shaped
> research builder behind lance.manifest.layout=betree, flat unchanged
> and unknown layouts rejected. Still missing, production Dataset wiring
> and S3 runs on this branch.

## Artifacts

- [bench-results/txn_parity_local_20260802.csv](./bench-results/txn_parity_local_20260802.csv), 36 rows, both localities
- `betree::commit_builder` module, `BeTreeTransaction` proto message, txn store methods, threaded `TxnOperation` labels
- Reproduce with the OPEN_QUESTIONS commands plus the same env vars against `AB_CSV=devtools/betree/bench-results/txn_parity_local_20260802.csv`
