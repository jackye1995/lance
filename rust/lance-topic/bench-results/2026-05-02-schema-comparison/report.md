# Lance Topic Schema Comparison Benchmark

Run date: 2026-05-02

## Setup

- Host: EC2 `c7i.8xlarge` (32 vCPUs, 64 GB RAM) in `us-east-1`
- Storage: S3 in the same region
- Repeats: 3 per case (values below are arithmetic mean)
- Rust toolchain: 1.84.0

**Default schema**: `id: Utf8` (PK) + `payload: lance.json` (256-byte body)

**Custom schema**: `event_id: Utf8` (PK) + `user_id: Int64` + `score: Float64`
+ `embedding: FixedSizeList(Float32, 1024)` — each row carries a 4 KB vector

## Write Throughput

### Default Schema (id + payload)

| Case | Partitions | Producers | Rows/s | WAL MiB/s |
| --- | --- | --- | --- | --- |
| single_p1 | 1 | 1 | 89,859 | 28.7 |
| single_p4 | 4 | 1 | 99,372 | 31.8 |
| scale_p4_prod2 | 4 | 2 | 168,768 | 54.0 |
| scale_p4_prod4 | 4 | 4 | 305,883 | 97.7 |
| scale_p4_prod8 | 4 | 8 | 391,302 | 124.5 |
| batch_100 | 1 | 1 | 2,602 | 0.9 |
| batch_1000 | 1 | 1 | 23,896 | 7.6 |
| batch_5000 | 1 | 1 | 80,405 | 25.7 |

### Custom Schema (event_id + user_id + score + 1024-dim embedding)

| Case | Partitions | Producers | Rows/s | WAL MiB/s |
| --- | --- | --- | --- | --- |
| single_p1 | 1 | 1 | 14,651 | 59.8 |
| single_p4 | 4 | 1 | 34,222 | 139.7 |
| scale_p4_prod2 | 4 | 2 | 55,503 | 226.6 |
| scale_p4_prod4 | 4 | 4 | 85,945 | 350.8 |
| scale_p4_prod8 | 4 | 8 | 99,861 | 353.2 |
| batch_100 | 1 | 1 | 2,308 | 9.5 |
| batch_1000 | 1 | 1 | 10,580 | 43.2 |
| batch_5000 | 1 | 1 | 16,887 | 68.9 |

### Analysis

With the custom schema, each row is ~4.3 KB (dominated by the 1024×4 = 4096
bytes of embedding data) versus ~335 bytes for the default schema. This is a
**12.8x larger per-row payload**. The throughput difference reflects this:

- **Default single producer**: ~90k rows/s at 28.7 MiB/s WAL bandwidth
- **Custom single producer**: ~15k rows/s at 59.8 MiB/s WAL bandwidth
- The custom schema moves **2x more WAL data per second** despite fewer rows,
  showing the bottleneck is S3 PUT bandwidth, not per-row overhead.

Producer scaling works for both schemas:

| Producers | Default Rows/s | Custom Rows/s | Default Speedup | Custom Speedup |
| --- | --- | --- | --- | --- |
| 1 | 89,859 | 14,651 | 1.0x | 1.0x |
| 2 | 168,768 | 55,503 | 1.9x | 3.8x |
| 4 | 305,883 | 85,945 | 3.4x | 5.9x |
| 8 | 391,302 | 99,861 | 4.4x | 6.8x |

The custom schema benefits *more* from horizontal scaling because each
producer writes larger WAL entries that better amortize S3 PUT latency.
With a single producer, the single_p4 case (4 concurrent shard writes) gives
2.3x speedup over single_p1 for custom schema — the per-partition WAL
entries are smaller and PUT faster.

### Batch Size (Single Producer, 1 Partition)

| Batch Size | Default Rows/s | Custom Rows/s | Default per-PUT | Custom per-PUT |
| --- | --- | --- | --- | --- |
| 100 | 2,602 | 2,308 | ~35 KB | ~430 KB |
| 1000 | 23,896 | 10,580 | ~336 KB | ~4.3 MB |
| 5000 | 80,405 | 16,887 | ~1.7 MB | ~21 MB |

At batch_size=5000, the custom schema writes ~21 MB per PUT. The default
schema writes ~1.7 MB per PUT. The custom schema's lower rows/s but higher
WAL MiB/s (68.9 vs 25.7) shows it's pushing more data but is bandwidth-bound
rather than latency-bound.

## Read Throughput

| Case | Producers | Default Rows/s | Custom Rows/s |
| --- | --- | --- | --- |
| 200k_poll32 | 1 | 83,359 | 20,385 |
| 200k_poll32 | 4 | 88,340 | 19,933 |
| 500k_poll32 | 1 | 57,529 | 20,766 |

### Analysis

- **Default schema reads**: ~83k rows/s for 200k rows, dropping to ~58k for
  500k rows (more S3 GETs).
- **Custom schema reads**: ~20k rows/s, roughly constant regardless of row
  count or producer count. The bottleneck is S3 GET bandwidth — each entry
  is ~21 MB and takes longer to download.
- Custom schema read throughput at ~20k rows/s × 4.3 KB/row ≈ **84 MiB/s**
  of S3 GET bandwidth, similar to the default schema's ~83k rows/s ×
  335 bytes/row ≈ **27 MiB/s**. The custom schema is saturating more S3
  bandwidth per shard.

## Summary

| Metric | Default Schema | Custom Schema (1024-dim) |
| --- | --- | --- |
| Per-row size | ~335 bytes | ~4.3 KB |
| Single-producer write | 90k rows/s | 15k rows/s |
| 8-producer write | 391k rows/s | 100k rows/s |
| WAL write bandwidth (8 prod) | 124 MiB/s | 353 MiB/s |
| Single-shard read | 83k rows/s | 20k rows/s |
| Read bandwidth | 27 MiB/s | 84 MiB/s |

The custom schema (with 1024-dim vectors) writes and reads fewer rows per
second but moves significantly more data. The system scales correctly with
both schema types. The bottleneck shifts from S3 PUT/GET latency (default
schema) to S3 PUT/GET bandwidth (custom schema with large payloads).
