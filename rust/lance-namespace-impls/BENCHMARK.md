# `__manifest` catalog benchmark

Measures manifest-only directory catalog startup, reads, and copy-on-write mutations as
the manifest scales. The current catalog loads the complete `__manifest` into an in-memory
snapshot, queries that snapshot, and reloads it after a successful update.

The catalog commits every mutation by rewriting the whole `__manifest` (copy-on-write)
and atomically writing a new manifest version. This benchmark characterizes:

- **Startup** — open the namespace and load the entire manifest snapshot.
- **Read** — list namespaces, list tables, or describe a table from an already loaded
  snapshot.
- **Continuous commit** — a single process commits `N` times into a manifest already
  holding `rows` entries (per-commit latency + throughput).
- **Concurrent commit** — `C` processes commit continuously for a fixed duration against
  a manifest of `rows` entries (steady, contended TPS).

## Binary: `examples/manifest_bench.rs`

```text
manifest_bench seed-large --root <uri> --count <rows> \
    [--storage-option aws_region=us-east-1]
manifest_bench run --root <uri> --operation startup \
    --concurrency 1 --operations 1 --initial-entries <rows>
manifest_bench run --root <uri> --operation warm-read-describe-table \
    --concurrency 1 --operations 1000 --warmup 10 --initial-entries <rows>
manifest_bench run --root <uri> --operation write-create-namespace \
    --concurrency 1 --operations 100 --initial-entries <rows>
manifest_bench run --root <uri> --operation write-create-namespace \
    --concurrency 50 --duration-secs 30 --initial-entries <rows>
```

- `seed-large` bootstraps all `count` rows in one direct Lance dataset write. It does not
  call catalog create operations per entry and does not perform an index-building rewrite.
- `run` spawns `--concurrency` worker subprocesses. With `--operations` it runs a fixed
  operation budget; with `--duration-secs` each worker commits until the deadline. It
  prints one JSON `BenchResult` per concurrency level with throughput and p50/p90/p99
  latency.
- `startup`, `warm-read-list-namespaces`, `warm-read-list-tables`, and
  `warm-read-describe-table` cover the in-memory load and query paths.
- The committed operation defaults to `write-create-namespace`, the cheapest pure
  `__manifest` mutation. `write-create-table` and `write-declare-table` are also available.

S3 requires the default `dir-aws` feature (on by default) and AWS credentials in the
environment; pass `--storage-option aws_region=<region>`.

## Legacy sweep panel: `benches/manifest_commit_sweep.sh`

The legacy sweep script runs sizes × {inline index, no index} × {continuous,
concurrent×C}. It remains useful with a build from before the in-memory implementation.
Use isolated S3 prefixes and the same one-shot seed for every variant so each run starts
from the same catalog contents.

```bash
cargo build --release --example manifest_bench -p lance-namespace-impls
S3_BASE=s3://<bucket>/manifest-cow-bench/$(date -u +%Y%m%dT%H%M%SZ) \
  rust/lance-namespace-impls/benches/manifest_commit_sweep.sh
```

The default legacy panel can be overridden with `SIZES`, `CONCURRENCY`,
`INLINE_VARIANTS`, `CONT_OPS`, and `CONC_DURATION_SECS`. Results land in `$OUT_DIR`.

## Representative results

EC2 `c7i.12xlarge`, S3 `us-east-1`, upstream commit `577091e5` versus the in-memory
implementation at `ff72b913`. Startup is the median of five fresh processes; other values
are operation p50. Every catalog was bootstrapped in one direct dataset write before one
preparation mutation established its steady-state representation.

| rows | variant | startup | startup RSS | warm exact read | list tables | serial writes/s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1K | indexed | 97.7 ms | 44.5 MiB | 35.0 ms | 61.7 ms | 3.339 |
| 1K | no index | 100.2 ms | 43.1 MiB | 60.9 ms | 58.6 ms | 4.563 |
| 1K | in memory | 136.3 ms | 75.3 MiB | 9.1 ms | 9.4 ms | 5.677 |
| 100K | indexed | 96.4 ms | 44.6 MiB | 39.5 ms | 138.7 ms | 1.731 |
| 100K | no index | 98.9 ms | 43.2 MiB | 65.5 ms | 101.6 ms | 2.167 |
| 100K | in memory | 231.5 ms | 106.0 MiB | 9.5 ms | 19.1 ms | 2.578 |
| 1M | indexed | 104.3 ms | 44.3 MiB | 92.8 ms | 461.0 ms | 0.565 |
| 1M | no index | 98.2 ms | 43.3 MiB | 84.1 ms | 463.8 ms | 0.781 |
| 1M | in memory | 603.3 ms | 386.7 MiB | 11.0 ms | 125.9 ms | 0.684 |

At 1M rows, the in-memory snapshot is 8.4× faster than the indexed implementation for an
exact lookup, 3.7× faster for a full table listing, and 21% faster for serial writes. Its
fresh startup costs about 0.5 seconds and 342 MiB more RSS. The legacy no-index writer is
12% faster for serial writes at 1M, while its read latency remains close to the indexed
implementation. With ten contending writers, in-memory and no-index were effectively tied
(0.731 versus 0.737 ops/s) and both were about 43–45% faster than indexed writes.
