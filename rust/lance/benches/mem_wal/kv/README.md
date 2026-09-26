# MemWAL key-value comparison

This benchmark compares Lance MemWAL with RocksDB and SlateDB. It gives every engine the same deterministic keys, values, query order, hit ratio, batch size, and thread count. Each engine runs in a separate process so memory measurements do not overlap.

The Lance benchmark stays in the main workspace. The RocksDB and SlateDB adapters live in the isolated `reference` crate. This keeps RocksDB native build requirements and SlateDB dependencies out of normal Lance builds.

## Metrics

The JSON output separates API acceptance from WAL durability. `write_accepted_rows_per_s` ends when the engine accepts all write batches. `wal_durable_rows_per_s` includes the final wait for every accepted batch to become durable. `durability_drain_s` is the time between the last accepted batch and the durable frontier.

The `flushed` storage mode also measures `sstable_flush_s`. Lance seals the active MemTable and waits for its flush fence. SlateDB requests a MemTable flush and waits for it to finish. Reads then use the flushed tier.

RocksDB keeps its historical asynchronous WAL behavior by default. Its WAL result is null because a returned write has not requested an `fsync`. Pass `--rocksdb-sync` directly to the reference binary when a synchronous durability result is needed.

## Local run

Run the default 100,000, 500,000, and 1,000,000 row sweep with:

```sh
rust/lance/benches/mem_wal/kv/run_kv_compare.sh
```

The runner builds both benchmark binaries. The first run also builds RocksDB and SlateDB in the isolated reference target directory.

## Amazon S3 run

Create a private bucket in the same AWS region as the benchmark host. Then run Lance and SlateDB with fresh prefixes:

```sh
BASE_URI=s3://BUCKET/PREFIX \
ENGINES="lance slatedb" \
STORAGES="active flushed" \
rust/lance/benches/mem_wal/kv/run_kv_compare.sh RUN_ID
```

An S3 run defaults to one full read prewarm round and three measured repetitions. The prewarm queries are not included in the read metrics. Each engine, storage mode, size, and repetition gets a distinct object prefix.

The runner does not delete S3 objects. This preserves failed runs and raw inputs for diagnosis. Remove the dedicated experiment bucket only after the result bundle has been downloaded and reviewed.

## Main controls

- `SIZES` sets row counts.
- `VALUE_SIZE` sets value bytes.
- `QUERIES` sets timed point lookups.
- `MISS_RATIO` sets the missing-key fraction.
- `THREADS` sets concurrent read workers.
- `BATCH_ROWS` sets rows per write batch.
- `KEY_TYPES` selects `int`, `uuid`, or both.
- `ENGINES` selects `lance`, `rocksdb`, or `slatedb`.
- `STORAGES` selects `active`, `flushed`, or both.
- `WARMUP_ROUNDS` sets discarded read passes.
- `REPETITIONS` sets measured repetitions.
- `BASE_URI` sets a local path or S3 prefix.
- `RESULT_DIR` sets the raw result directory.
