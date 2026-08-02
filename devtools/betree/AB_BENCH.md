# Flat vs tiered vs Bε A/B benchmark

Measured locally on 2026-07-26 on an Apple M5 Pro with 64 GiB RAM and macOS
25.5.0. The Bε worktree was based on `b069ecc228`. The tiered worktree was
detached at `61066f586`. All runs used Rust 1.97.0, Cargo's `bench` profile, a
temporary local filesystem store, and fabricated metadata only. No data-file
payloads were written.

This report replaces the preliminary single-run matrix. The corrected harness
aligns timing boundaries, disables metadata caches for both lazy layouts,
separates tiered tree and transaction bytes, repeats independent setups three
times, tests contiguous and scattered updates, sweeps the byte budget, and
forces append flushes.

## What is and is not comparable

Every mutation starts from the same prebuilt workload request: one fragment or
a vector of `(fragment_id, data_file)` pairs. The timer starts before each
contender translates that request into its native operation.

The matrix uses fanout 16 throughout. `AB_NODE_SIZE_KIB` is the shared Bε node
budget and tiered message-byte budget. Tiered's fragment-count buffer cap stays
at its harness default of `N / 10`.

The original matrix used the same byte limit for Bε internal nodes and leaves.
The follow-up design probe below sets `AB_LEAF_SIZE_KIB` independently. That is
a Bε tuning experiment, not a replacement for the shared-budget A/B.

- flat calls the real full-manifest writer
- Bε goes through `BeTreeDataset` and translates the request into actions
- tiered goes through the real `Dataset`/`CommitBuilder` transaction path

Byte columns:

- `tree_bytes` is flat manifest output, Bε root/node/leaf output, or tiered
  root/child output
- `transaction_bytes` is the separate tiered transaction file
- `total_bytes` is their sum

Bε does not yet have a production transaction record, so tiered total bytes are
an end-to-end comparison while tiered tree bytes are the closer storage-layout
comparison. Commit latency compares the current paths end to end. It is not an
isolated encoder microbenchmark.

Each table below reports the median of three independent runs. Within each run,
commit time is averaged across all commits. The raw CSV also contains per-run
commit p50 and p95.

After the design review, flat and Bε were rerun with the same 128 KiB uniform
configuration. Their cells below use that rerun. Tiered cells retain the
original matched-run values because neither the tiered code nor harness
changed. Headline byte ratios were stable. Expected differences are lower Bε
append latency after known aggregate deltas and lower one-shot latency after
correcting internal-node size accounting.

## Main matrix

Mutation cells report average metadata KiB per commit / average milliseconds.
For tiered, bytes are shown as `tree + transaction = total`. Read cells report
read operations / milliseconds. Materialization operations are additional to
open. Both lazy layouts use disabled metadata caches.

| Scenario | flat | tiered | betree | Winner |
|---|---:|---:|---:|---|
| AB-APPEND, N=50K, 100 commits, pre-flush | 4,187.4 KiB / 7.13 ms | 9.9 + 0.1 = 10.1 KiB / 8.59 ms | 5.5 KiB / 0.37 ms | Bε, startup window |
| AB-APPEND-STEADY, N=50K, 5,000 commits | 4,393.2 KiB / 7.28 ms | 214.0 + 0.1 = 214.1 KiB / 8.40 ms | 61.1 KiB / 0.49 ms | Bε after 5 flushes |
| AB-APPEND, N=1M, 100 commits, pre-flush | 83,972.6 KiB / 125.83 ms | 89.8 + 0.1 = 89.9 KiB / 240.00 ms | 5.9 KiB / 0.35 ms | Bε, startup window |
| AB-TRICKLE, N=50K, F=10, 500 contiguous commits | 4,372.9 KiB / 6.93 ms | 4,854.3 + 4,372.9 = 9,227.3 KiB / 69.07 ms | 61.0 KiB / 0.50 ms | Bε |
| AB-TRICKLE, N=50K, F=10, 500 scattered commits | 4,372.9 KiB / 8.67 ms | 4,891.8 + 4,372.9 = 9,264.8 KiB / 68.36 ms | 92.4 KiB / 0.69 ms | Bε |
| AB-ONESHOT, N=50K, F=50K | 7,943.0 KiB / 14.21 ms | 15,892.1 + 7,943.0 = 23,835.1 KiB / 121.87 ms | 4,539.5 KiB / 167.79 ms | Bε bytes; flat time |
| AB-OPEN, N=100K after 100 appends | 3 / 35.83 ms, eager | 3 / 0.28 ms; +20 / 41.84 ms | 2 / 1.25 ms; +136 / 61.82 ms | tiered open time; tiered materialize |
| AB-RESOLVE, N=100K, cache disabled | 0 after eager open | 2.00 / 3.12 ms | 2.00 / 0.47 ms | Bε among lazy layouts |

The preliminary 0.20-read tiered resolve result was a cache-policy artifact.
With caches disabled on both lazy layouts, each performs two reads. Tiered
still has the fastest root open and needs fewer objects to materialize. Bε
resolves the selected fragment faster at this leaf sizing.

Short append runs are labeled pre-flush. The 5,000-commit run is the
steady-state evidence: Bε flushed five times per rerun and still wrote 71.9×
less than flat and 3.50× less than tiered. Supplying append's known aggregate
delta reduced Bε average latency from 0.97 ms to 0.49 ms.

## Trickle sensitivity

The sensitivity sweep crosses byte budget with update locality. Contiguous
commits touch a tight fragment-id window. Scattered commits spread ids across
the keyspace. Each row is the median of three 500-commit runs. Tiered-tree
excludes its 4,477,877 byte average transaction file. Tiered-total includes it.

| Budget | Locality | Bε bytes/commit | Flat / Bε | Tiered tree / Bε | Tiered total / Bε | Bε flushes |
|---:|---|---:|---:|---:|---:|---:|
| 64 KiB | contiguous | 34,300 | 130.6× | 144.3× | 274.8× | 16 |
| 64 KiB | scattered | 56,764 | 78.9× | 88.2× | 167.1× | 28 |
| 128 KiB | contiguous | 62,471 | 71.7× | 79.6× | 151.2× | 7 |
| 128 KiB | scattered | 94,571 | 47.3× | 53.0× | 100.3× | 7 |
| 256 KiB | contiguous | 115,138 | 38.9× | 43.6× | 82.5× | 2 |
| 256 KiB | scattered | 146,433 | 30.6× | 34.4× | 65.0× | 2 |

The original `flat / 50` bar depends on budget and locality. It passes at 64
KiB and at 128 KiB contiguous, narrowly misses at 128 KiB scattered, and misses
at 256 KiB. The worst measured point still writes 30.6× less than flat and
34.4× less than tiered's tree alone.

At 128 KiB contiguous, about 4.48 MB is the full-list `Merge` transaction, and
another 4.97 MB is root/child output. The current transaction path erases the
delta advantage. Sealed-child and message compaction also cost more than the Bε
path in this harness.

## Post-review design probe

Internal ε-buffer sizing and leaf sizing had been coupled even though they
serve different access patterns. The prototype now stores separate limits. With
64 KiB internal nodes, 1 MiB leaves, fanout 16, and the same local metadata-only
harness, the median of three independent runs was:

| Scenario | Bε result | Flat result | Interpretation |
|---|---:|---:|---|
| 5,000 steady appends | 32.3 KiB / 0.472 ms | 4,393.2 KiB / 7.672 ms | 136.1× less write |
| 500 contiguous trickles | 36.7 KiB / 0.516 ms | 4,372.9 KiB / 7.218 ms | 119.2× less write |
| 500 scattered trickles | 70.5 KiB / 0.787 ms | 4,372.9 KiB / 7.045 ms | 62.0× less write |
| one-shot 50K | 4,146.3 KiB / 78.410 ms | 7,943.0 KiB / 14.548 ms | Bε bytes; flat time |
| open at ~100K | 2 GETs / 0.973 ms | 3 GETs / 30.132 ms | flat is eager |
| then materialize | +16 GETs / 32.536 ms | included above | down from +136 GETs |
| resolve 100 ids | 1.00 GET / 1.568 ms | 0 after eager open | larger leaf trades point latency for fewer GETs |

Append uses an exact delta from the fresh-id transaction operation, so
maintaining root aggregates no longer reads the target leaf. A tracked-store
test asserts zero append child reads. That matters with 1 MiB leaves: before
the fast path, the same split configuration averaged about 2.5 ms per steady
append because aggregate maintenance resolved each new id.

The split configuration improves materialization from the uniform 128 KiB
baseline's 136 GETs / 47.87 ms to 16 GETs / 32.54 ms. Point lookup becomes
slower, 1.57 ms versus 0.36 ms, because one larger leaf is fetched. Treat 1 MiB
as a measured local-filesystem knee, not a universal default. Scattered trickle
also writes more than the uniform 64 KiB configuration, 72,222 versus 56,764
bytes per commit. Object-store latency, scanner batching, and cache hit rates
should choose the production leaf limit.

The 36 raw rows are checked in at
[`bench-results/split64_leaf1024_local_20260726.csv`](./bench-results/split64_leaf1024_local_20260726.csv).

Reproduce the probe:

```bash
NUM_FRAGMENTS=50000 \
AB_SCENARIOS=append_steady,trickle,oneshot \
AB_STEADY_APPEND_COMMITS=5000 \
AB_TRICKLE_COMMITS=500 \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=64 \
AB_LEAF_SIZE_KIB=1024 \
AB_LOCALITY=contiguous \
AB_CSV=/tmp/betree_split.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab

NUM_FRAGMENTS=50000 \
AB_SCENARIOS=trickle \
AB_TRICKLE_COMMITS=500 \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=64 \
AB_LEAF_SIZE_KIB=1024 \
AB_LOCALITY=scattered \
AB_CSV=/tmp/betree_split_scattered.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab

NUM_FRAGMENTS=100000 \
AB_SCENARIOS=read \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=64 \
AB_LEAF_SIZE_KIB=1024 \
AB_CSV=/tmp/betree_split_read.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab
```

## Verdict

Under this workload and these implementations, Bε write cost stayed near the
update window while flat and the current tiered `Merge` path did table-scale
work. That is a measured prototype result, not a claim that Bε is intrinsically
151× faster.

Findings that held in the matrix:

- Bε tree writes beat flat and tiered-tree output at every tested
  budget/locality point
- the current tiered Dataset path also serializes a table-wide transaction on
  every `Merge`
- Bε's append advantage shrinks after forced flushes but remains real
- tiered retains the fastest root-open wall time at the matched uniform
  configuration. The split-leaf probe reverses its materialization object-count
  advantage locally, with a point-latency tradeoff
- one-shot whole-table work favors flat latency

## Reproduce

From the Bε worktree, run the sensitivity matrix:

```bash
for budget in 64 128 256; do
  for locality in contiguous scattered; do
    NUM_FRAGMENTS=50000 \
    AB_SCENARIOS=trickle \
    AB_TRICKLE_COMMITS=500 \
    AB_REPEATS=3 \
    AB_NODE_SIZE_KIB="$budget" \
    AB_LOCALITY="$locality" \
    AB_CSV=/tmp/betree_ab.csv \
    cargo +1.97.0 bench -p lance-table --bench betree_ab
  done
done
```

Run the remaining Bε matrix:

```bash
NUM_FRAGMENTS=50000 \
AB_SCENARIOS=append,append_steady,oneshot \
AB_APPEND_COMMITS=100 \
AB_STEADY_APPEND_COMMITS=5000 \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=128 \
AB_CSV=/tmp/betree_ab.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab

NUM_FRAGMENTS=100000 \
AB_SCENARIOS=read \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=128 \
AB_CSV=/tmp/betree_ab.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab

NUM_FRAGMENTS=1000000 \
AB_SCENARIOS=append \
AB_APPEND_COMMITS=100 \
AB_REPEATS=3 \
AB_NODE_SIZE_KIB=128 \
AB_CSV=/tmp/betree_ab.csv \
cargo +1.97.0 bench -p lance-table --bench betree_ab
```

Create an isolated tiered worktree without changing another checkout:

```bash
TIERED_PARENT="$(mktemp -d /tmp/lance-tiered-ab.XXXXXX)"
TIERED_WORKTREE="$TIERED_PARENT/worktree"
git worktree add --detach "$TIERED_WORKTREE" 61066f586
git -C "$TIERED_WORKTREE" apply \
  "$PWD/devtools/betree/tiered_ab_public_resolve.patch"
cp "$PWD/devtools/betree/tiered_ab_bench.rs" \
  "$TIERED_WORKTREE/rust/lance/examples/tiered_ab_bench.rs"
```

Run the same commands from the tiered worktree, replacing the final command
with:

```bash
CARGO_TARGET_DIR=/tmp/lance-tiered-target \
cargo +1.97.0 run --profile bench -p lance --example tiered_ab_bench
```

The exact 108 emitted data rows are checked in at
[`bench-results/ab_local_20260726.csv`](./bench-results/ab_local_20260726.csv).
That file is the original fully matched run. The 48 post-review flat/Bε rows
are in
[`bench-results/post_review_128_local_20260726.csv`](./bench-results/post_review_128_local_20260726.csv).
