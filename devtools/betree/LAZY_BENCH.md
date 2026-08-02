# Bε-tree lazy read benchmark

Measured locally on 2026-07-25 using an Apple M5 Pro with 64 GiB RAM, macOS
25.5.0, and Rust 1.97.0. `cargo bench` used the repository's bench profile
with thin LTO and 16 codegen units. The default benchmark URI was a temporary
local filesystem directory.

## Reproduce

From the repository root:

```bash
# L1-L4 lazy read matrix. The defaults in lazy mode are the values shown here.
LAZY_BENCH=true \
NUM_FRAGMENTS=100000 \
NODE_SIZE_MB=0.25 \
FANOUT=16 \
LAZY_RESOLVES=100 \
LAZY_BACKFILL_COMMITS=10 \
cargo bench -p lance-table --bench betree_backfill

# W1 write-amplification sample used for the comparison below.
NUM_FRAGMENTS=5000 \
FRAGMENTS_PER_COMMIT=10 \
NODE_SIZE_MB=0.25 \
FANOUT=16 \
BETREE_COMMITS=500 \
FLAT_SAMPLE_COMMITS=5 \
cargo bench -p lance-table --bench betree_backfill
```

Set `BASE_URI` to select another local path or an S3 URI. For S3, also set
`AWS_REGION`. The existing `S3_EXPRESS` and AWS credential environment
variables remain supported.

The lazy benchmark emits a human-readable table and `LAZY_RESULT|...` rows for
machine parsing. It bootstraps 100,000 fragments, asserts tree height is at
least two, resolves 100 deterministic fragment ids, verifies every resolve and
the stream against full materialization, then repeats after ten F=10 backfill
commits.

## Results

Bootstrap wrote 4.54 MiB into 64 leaves at height 2 in 194.17 ms.

| Scenario | Open GETs | Open ms | Resolve GETs avg | Resolve ms avg | Materialize GETs | Materialize ms | Stream GETs | Stream ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| L1-L3 bootstrap | 2 | 0.424 | 2.00 | 0.813 | 68 | 56.671 | 68 | 41.768 |
| L4 after ten F=10 commits | 2 | 0.176 | 2.00 | 0.534 | 68 | 43.285 | 68 | 38.117 |

`open` includes one root-directory listing and the newest root-object GET.
Materialization and stream counts exclude those two operations. Each point
resolve at height 2 used one internal-node GET and one leaf GET. The stream
implementation itself keeps one decoded leaf plus pending subtree actions. This
benchmark collects the stream solely to compare it with the materialized
oracle, so process peak RSS was not reported.

Lazy open used 34 times fewer GETs than materializing the tree, 2 versus 68,
and returned about 134 times sooner in the initial run. A point resolve also
used 34 times fewer GETs and was about 70 times faster than full
materialization. The same bounds and equality checks held after backfill.
Streaming performs a full traversal, as expected, while avoiding the
materializer's all-fragment map.

## W1 write amplification

The W1 command was run once against unmodified `HEAD` (`725e9e467`) in a
temporary detached worktree and once against this implementation. Both runs
performed 500 F=10 commits and triggered two leaf flushes.

| Revision | Per-commit Bε write | Flushes | Full-backfill estimate |
|---|---:|---:|---:|
| Unmodified `HEAD` | 0.145 MiB | 2 | 0.07 GiB |
| Lazy/fidelity implementation | 0.146 MiB | 2 | 0.07 GiB |

At the benchmark's displayed precision, per-commit write increased by about
0.7%, well below the 10% regression limit. The flat baseline in both runs was
0.408 MiB per commit.

## Re-verification (2026-07-25 evening)

Clean rebuild after fixing `lance-table/build.rs` to `rerun-if-changed` each
proto file. The symlink `protos/` had missed content updates and left release
builds on a stale generated module. Debug tests had been green while
`cargo bench` failed to compile.

| Check | Result |
|-------|--------|
| `cargo test -p lance-table betree` (clean) | 11/11 pass |
| `cargo test -p lance-table --lib` | 138/138 pass |
| `cargo clippy -p lance-table --tests --benches -- -D warnings` | clean |
| `cargo fmt -p lance-table -- --check` | clean |
| Lazy L1–L4 @ N=100K, B=0.25 MiB | open **2** GET / resolve **2.00** / materialize **68** GET (matches table above) |
| W1 @ 500×F=10 commits | **0.146 MiB**/commit, 2 flushes; flat 0.408 MiB; no write-amp regression |

Out of scope for this goal: Dataset wiring, S3 re-run, L0 delta chain.
