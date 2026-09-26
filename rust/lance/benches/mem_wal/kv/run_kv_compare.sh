#!/usr/bin/env bash
# Run Lance, RocksDB, and SlateDB with identical deterministic KV inputs.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

RUN_ID="${1:-kv-compare-$(date -u +%Y%m%dT%H%M%SZ)}"
SIZES="${SIZES:-100000 500000 1000000}"
VALUE_SIZE="${VALUE_SIZE:-100}"
QUERIES="${QUERIES:-5000}"
MISS_RATIO="${MISS_RATIO:-0.5}"
THREADS="${THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)}"
BATCH_ROWS="${BATCH_ROWS:-1000}"
KEY_TYPES="${KEY_TYPES:-int}"
ENGINES="${ENGINES:-lance rocksdb slatedb}"
STORAGES="${STORAGES:-active}"
CONFIG_TIMEOUT="${CONFIG_TIMEOUT:-3600}"
WORK="${WORK:-${TMPDIR:-/tmp}/kv_compare/$RUN_ID}"
BASE_URI="${BASE_URI:-$WORK/data}"
RESULT_DIR="${RESULT_DIR:-$REPO_ROOT/target/kv-compare-results/$RUN_ID}"
SOURCE_REVISION="$(git rev-parse HEAD)"
BENCH_HOST_TYPE="${BENCH_HOST_TYPE:-unknown}"

if [[ "$BASE_URI" == s3://* ]]; then
    WARMUP_ROUNDS="${WARMUP_ROUNDS:-1}"
    PREWARM_REPETITIONS="${PREWARM_REPETITIONS:-1}"
    REPETITIONS="${REPETITIONS:-3}"
else
    WARMUP_ROUNDS="${WARMUP_ROUNDS:-0}"
    PREWARM_REPETITIONS="${PREWARM_REPETITIONS:-0}"
    REPETITIONS="${REPETITIONS:-1}"
fi

mkdir -p "$WORK" "$RESULT_DIR"

BENCH=mem_wal_kv_point_lookup
echo "Building Lance KV benchmark"
rm -f "$REPO_ROOT"/target/release/deps/${BENCH}-*
cargo bench -p lance --bench "$BENCH" --no-run || {
    echo "ERROR: Lance benchmark build failed" >&2
    exit 1
}
LANCE_BIN="$(find "$REPO_ROOT/target/release/deps" -maxdepth 1 -type f -perm -111 \
    -name "${BENCH}-*" ! -name '*.d' | head -1)"

REFERENCE_MANIFEST="$SCRIPT_DIR/reference/Cargo.toml"
REFERENCE_TARGET="$REPO_ROOT/target/kv-reference"
echo "Building RocksDB and SlateDB KV benchmark"
CARGO_TARGET_DIR="$REFERENCE_TARGET" cargo build \
    --release --manifest-path "$REFERENCE_MANIFEST" || {
    echo "ERROR: reference benchmark build failed" >&2
    exit 1
}
REFERENCE_BIN="$REFERENCE_TARGET/release/mem-wal-kv-reference"

echo "run id: $RUN_ID"
echo "base uri: $BASE_URI"
echo "sizes: $SIZES"
echo "engines: $ENGINES"
echo "storages: $STORAGES"
echo "discarded prewarm repetitions: $PREWARM_REPETITIONS"
echo "repetitions: $REPETITIONS"
echo "read prewarm rounds: $WARMUP_ROUNDS"
echo "source revision: $SOURCE_REVISION"
echo "host type: $BENCH_HOST_TYPE"

TIMEOUT_BIN="$(command -v timeout || command -v gtimeout || true)"

run_engine() {
    local engine="$1" storage="$2" key_type="$3" rows="$4" tag="$5" repetition="$6" phase="$7"
    local case_name="${engine}_${storage}_${key_type}_${tag}"
    local name output log uri
    local -a command

    if [[ "$phase" == prewarm ]]; then
        name="${case_name}_prewarm${repetition}"
        output="$WORK/prewarm-results/$RUN_ID/${name}.json"
        log="$WORK/prewarm-results/$RUN_ID/${name}.log"
        uri="${BASE_URI%/}/${RUN_ID}/prewarm/${name}"
    else
        name="${case_name}_r${repetition}"
        output="$RESULT_DIR/${name}.json"
        log="$RESULT_DIR/${name}.log"
        uri="${BASE_URI%/}/${RUN_ID}/measured/${name}"
    fi
    mkdir -p "$(dirname "$output")"

    if [[ -f "$output" ]]; then
        echo ">>> $name already completed"
        return 0
    fi
    if [[ "$engine" == rocksdb && "$uri" == s3://* ]]; then
        echo ">>> $name skipped because RocksDB has no S3 object-store backend"
        return 0
    fi

    if [[ "$engine" == lance ]]; then
        command=("$LANCE_BIN" --bench --engine lance --lance-read-mode api)
    else
        command=("$REFERENCE_BIN" --engine "$engine")
    fi
    command+=(
        --storage "$storage"
        --key-type "$key_type"
        --rows "$rows"
        --value-size "$VALUE_SIZE"
        --queries "$QUERIES"
        --miss-ratio "$MISS_RATIO"
        --threads "$THREADS"
        --batch-rows "$BATCH_ROWS"
        --warmup-rounds "$WARMUP_ROUNDS"
        --uri "$uri"
        --output "$output"
    )

    echo ">>> $name"
    if [[ "$uri" != s3://* ]]; then
        rm -rf "$uri"
    fi
    if [[ -n "$TIMEOUT_BIN" ]]; then
        BENCH_SOURCE_REVISION="$SOURCE_REVISION" \
            BENCH_HOST_TYPE="$BENCH_HOST_TYPE" \
            BENCH_CAMPAIGN_ID="$RUN_ID" \
            BENCH_PHASE="$phase" \
            "$TIMEOUT_BIN" "$CONFIG_TIMEOUT" "${command[@]}" >"$log" 2>&1
    else
        BENCH_SOURCE_REVISION="$SOURCE_REVISION" \
            BENCH_HOST_TYPE="$BENCH_HOST_TYPE" \
            BENCH_CAMPAIGN_ID="$RUN_ID" \
            BENCH_PHASE="$phase" \
            "${command[@]}" >"$log" 2>&1
    fi
    local result=$?
    if [[ "$uri" != s3://* ]]; then
        rm -rf "$uri"
    fi
    if [[ "$result" -eq 124 ]]; then
        echo "    timed out after ${CONFIG_TIMEOUT}s, see $log"
        return 1
    fi
    if [[ "$result" -ne 0 ]]; then
        echo "    failed with status $result, see $log"
        return 1
    fi
    echo "    ok"
}

failures=0
for rows in $SIZES; do
    case "$rows" in
        1000000) tag=1m ;;
        500000) tag=500k ;;
        100000) tag=100k ;;
        *) tag="$rows" ;;
    esac
    for storage in $STORAGES; do
        for key_type in $KEY_TYPES; do
            for prewarm in $(seq 1 "$PREWARM_REPETITIONS"); do
                for engine in $ENGINES; do
                    run_engine "$engine" "$storage" "$key_type" "$rows" "$tag" "$prewarm" prewarm \
                        || failures=$((failures + 1))
                done
            done
            for repetition in $(seq 1 "$REPETITIONS"); do
                for engine in $ENGINES; do
                    run_engine "$engine" "$storage" "$key_type" "$rows" "$tag" "$repetition" measured \
                        || failures=$((failures + 1))
                done
            done
        done
    done
done

python3 - "$RESULT_DIR" <<'PY'
import glob
import json
import os
import statistics
import sys

directory = sys.argv[1]
groups = {}
metrics = (
    "write_accepted_rows_per_s",
    "wal_durable_rows_per_s",
    "sstable_flush_s",
    "read_p50_us",
    "read_p99_us",
    "read_qps_1t",
    "read_qps_nt",
    "peak_rss_mb",
)
for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
    with open(path, encoding="utf-8") as source:
        document = json.load(source)
    for result in document.get("results", []):
        key = (
            result["rows"],
            result.get("storage", "active"),
            result.get("key_type", "int"),
            result["engine"],
        )
        groups.setdefault(key, []).append(result)

print("rows storage key engine runs accepted/s durable/s flush_s p50_us p99_us qps_1t qps_nt rss_mb")
for key in sorted(groups):
    runs = groups[key]
    values = []
    for metric in metrics:
        samples = []
        for run in runs:
            value = run.get(metric)
            if value is not None:
                samples.append(float(value))
        values.append("-" if not samples else f"{statistics.median(samples):.3f}")
    print(*key, len(runs), *values)
PY

echo "results: $RESULT_DIR"
if [[ "$failures" -ne 0 ]]; then
    echo "$failures benchmark cases failed" >&2
    exit 1
fi
