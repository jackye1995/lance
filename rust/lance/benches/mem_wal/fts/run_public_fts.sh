#!/usr/bin/env bash
# Run the direct on-disk FTS comparison on one prepared public dataset.
#
# Required environment:
#   DATASET_DIR     corpus.txt, docids.txt, queries.tsv, and optional qrels.tsv
#   LUCENE_CP       Lucene core and analysis-common jars
#   PUBLIC_FTS_BIN  public_fts_bench executable (or set both runner variables)
#
# Optional environment:
#   WORK_DIR        local persistent work area (default: /tmp/public-fts/<run-id>)
#   RESULTS_DIR     result directory (default: <repo>/target/public-fts-results/<run-id>)
#   LANCE_RUNNER    Lance runner executable (defaults to PUBLIC_FTS_BIN)
#   TANTIVY_RUNNER  Tantivy runner executable (defaults to PUBLIC_FTS_BIN)
#   JAVA_BIN        Java executable (default: java)
#   JAVAC_BIN       Java compiler (default: javac)
#   LUCENE_SOURCE   PublicLuceneFtsBench.java path (default: beside this script)
#   THREADS         concurrent query workers (default: online CPUs)
#   K               timed-query top-k (default: 10)
#   QUALITY_K       quality ranking depth (default: 1000)
#   BATCH_ROWS      Rust runner input batch size (default: 8192)
#
# Each engine builds a separate index, performs one complete discarded query
# round, and records three measured rounds.  Existing index directories cause
# an error: this script never deletes a corpus or an index.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"
RUN_ID="${RUN_ID:-public-fts-$(date -u +%Y%m%dT%H%M%SZ)}"

: "${DATASET_DIR:?set DATASET_DIR to a prepared public dataset directory}"
: "${LUCENE_CP:?set LUCENE_CP to Lucene core and analysis-common jars}"

PUBLIC_FTS_BIN="${PUBLIC_FTS_BIN:-}"
LANCE_RUNNER="${LANCE_RUNNER:-$PUBLIC_FTS_BIN}"
TANTIVY_RUNNER="${TANTIVY_RUNNER:-$PUBLIC_FTS_BIN}"
: "${LANCE_RUNNER:?set PUBLIC_FTS_BIN or LANCE_RUNNER}"
: "${TANTIVY_RUNNER:?set PUBLIC_FTS_BIN or TANTIVY_RUNNER}"

WORK_DIR="${WORK_DIR:-${TMPDIR:-/tmp}/public-fts/$RUN_ID}"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/target/public-fts-results/$RUN_ID}"
JAVA_BIN="${JAVA_BIN:-java}"
JAVAC_BIN="${JAVAC_BIN:-javac}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
TIME_BIN="${TIME_BIN:-/usr/bin/time}"
LUCENE_SOURCE="${LUCENE_SOURCE:-$SCRIPT_DIR/PublicLuceneFtsBench.java}"
THREADS="${THREADS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)}"
K="${K:-10}"
QUALITY_K="${QUALITY_K:-1000}"
BATCH_ROWS="${BATCH_ROWS:-8192}"

CORPUS="$DATASET_DIR/corpus.txt"
DOCIDS="$DATASET_DIR/docids.txt"
QUERIES="$DATASET_DIR/queries.tsv"
QRELS="$DATASET_DIR/qrels.tsv"
DATASET_MANIFEST="$DATASET_DIR/manifest.json"
EVALUATOR="$SCRIPT_DIR/evaluate_public_fts.py"

for required in "$CORPUS" "$DOCIDS" "$QUERIES" "$EVALUATOR" "$LUCENE_SOURCE"; do
    if [[ ! -f "$required" ]]; then
        echo "ERROR: required file not found: $required" >&2
        exit 1
    fi
done
for executable in "$LANCE_RUNNER" "$TANTIVY_RUNNER" "$TIME_BIN"; do
    if [[ ! -x "$executable" ]]; then
        echo "ERROR: executable not found: $executable" >&2
        exit 1
    fi
done
for command_name in "$JAVA_BIN" "$JAVAC_BIN" "$PYTHON_BIN"; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "ERROR: command not found: $command_name" >&2
        exit 1
    fi
done
if [[ ! "$THREADS" =~ ^[1-9][0-9]*$ || ! "$K" =~ ^[1-9][0-9]*$ \
      || ! "$QUALITY_K" =~ ^[1-9][0-9]*$ || ! "$BATCH_ROWS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: THREADS, K, QUALITY_K, and BATCH_ROWS must be positive integers" >&2
    exit 1
fi
if (( QUALITY_K < 100 )); then
    echo "ERROR: QUALITY_K must be at least 100 for Recall@100 and MAP@100" >&2
    exit 1
fi

mkdir -p "$WORK_DIR" "$RESULTS_DIR"
INDEX_ROOT="$WORK_DIR/indexes"
LUCENE_CLASS_DIR="$WORK_DIR/lucene-classes"
for fresh_path in "$INDEX_ROOT/lance" "$INDEX_ROOT/tantivy" "$INDEX_ROOT/lucene" \
                  "$LUCENE_CLASS_DIR"; do
    if [[ -e "$fresh_path" ]]; then
        echo "ERROR: refusing to reuse or delete existing path: $fresh_path" >&2
        exit 1
    fi
done
mkdir -p "$INDEX_ROOT/lance" "$INDEX_ROOT/tantivy" "$INDEX_ROOT/lucene" \
    "$LUCENE_CLASS_DIR"

validate_inputs() {
    "$PYTHON_BIN" - "$CORPUS" "$DOCIDS" "$QUERIES" "$QRELS" <<'PY'
import sys
from pathlib import Path

corpus_path, docids_path, queries_path, qrels_path = map(Path, sys.argv[1:])
with corpus_path.open(encoding="utf-8") as source:
    corpus_count = sum(1 for _ in source)
with docids_path.open(encoding="utf-8") as source:
    docids = [line.rstrip("\r\n") for line in source]
if corpus_count == 0 or corpus_count != len(docids):
    raise SystemExit(
        f"corpus/docids row mismatch: corpus={corpus_count}, docids={len(docids)}"
    )
if any(not docid for docid in docids) or len(set(docids)) != len(docids):
    raise SystemExit("docids.txt contains an empty or duplicate document ID")

qids = []
with queries_path.open(encoding="utf-8") as source:
    for line_number, raw in enumerate(source, 1):
        fields = raw.rstrip("\r\n").split("\t")
        if len(fields) != 2 or not fields[0] or not fields[1]:
            raise SystemExit(
                f"{queries_path}:{line_number}: expected qid<TAB>query"
            )
        qids.append(fields[0])
if not qids or len(set(qids)) != len(qids):
    raise SystemExit("queries.tsv has no queries or contains duplicate query IDs")

if qrels_path.is_file() and qrels_path.stat().st_size:
    qrel_qids = set()
    with qrels_path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            fields = raw.rstrip("\r\n").split("\t")
            if len(fields) != 3:
                raise SystemExit(
                    f"{qrels_path}:{line_number}: expected qid<TAB>docid<TAB>relevance"
                )
            qrel_qids.add(fields[0])
    if qrel_qids != set(qids):
        raise SystemExit("queries.tsv and qrels.tsv query ID sets differ")
PY
}

validate_output() {
    local result_json="$1"
    local topk_file="$2"
    "$PYTHON_BIN" - "$result_json" "$topk_file" "$QUERIES" "$DOCIDS" \
        "$QUALITY_K" <<'PY'
import json
import sys
from pathlib import Path

result_path, topk_path, queries_path, docids_path = map(Path, sys.argv[1:5])
quality_k = int(sys.argv[5])
if not result_path.is_file() or not result_path.stat().st_size:
    raise SystemExit(f"missing or empty result JSON: {result_path}")
with result_path.open(encoding="utf-8") as source:
    result = json.load(source)
if not isinstance(result, dict):
    raise SystemExit(f"result is not a JSON object: {result_path}")
if not topk_path.is_file() or not topk_path.stat().st_size:
    raise SystemExit(f"missing or empty top-k file: {topk_path}")

with queries_path.open(encoding="utf-8") as source:
    expected = [line.rstrip("\r\n").split("\t", 1)[0] for line in source]
with docids_path.open(encoding="utf-8") as source:
    document_count = sum(1 for _ in source)

if result.get("documents") != document_count:
    raise SystemExit(
        f"result document count mismatch: {result.get('documents')} != {document_count}"
    )
if result.get("query_count") != len(expected):
    raise SystemExit(
        f"result query count mismatch: {result.get('query_count')} != {len(expected)}"
    )
if result.get("warmup_rounds") != 1 or result.get("measured_runs") != 3:
    raise SystemExit("result does not record one warmup and three measured runs")
if not isinstance(result.get("runs"), list) or len(result["runs"]) != 3:
    raise SystemExit("result does not contain three measured run records")

seen_qids = []
with topk_path.open(encoding="utf-8") as source:
    for line_number, raw in enumerate(source, 1):
        line = raw.rstrip("\r\n")
        if "\t" not in line:
            raise SystemExit(f"{topk_path}:{line_number}: missing tab")
        qid, row_ids_text = line.split("\t", 1)
        if not qid or qid in seen_qids:
            raise SystemExit(f"{topk_path}:{line_number}: empty or duplicate query ID")
        seen_qids.append(qid)
        row_ids = []
        for token in row_ids_text.split():
            try:
                row_id = int(token)
            except ValueError as error:
                raise SystemExit(
                    f"{topk_path}:{line_number}: invalid row ID {token!r}"
                ) from error
            if row_id < 0 or row_id >= document_count:
                raise SystemExit(
                    f"{topk_path}:{line_number}: row ID {row_id} out of range"
                )
            row_ids.append(row_id)
        if len(row_ids) > quality_k or len(set(row_ids)) != len(row_ids):
            raise SystemExit(
                f"{topk_path}:{line_number}: too many or duplicate row IDs"
            )
if seen_qids != expected:
    raise SystemExit("top-k query IDs/order do not match queries.tsv")
PY
}

run_timed() {
    local engine="$1"
    shift
    local stdout_log="$RESULTS_DIR/${engine}.stdout.log"
    local stderr_log="$RESULTS_DIR/${engine}.stderr.log"
    local time_log="$RESULTS_DIR/${engine}.time.txt"
    local command_log="$RESULTS_DIR/${engine}.command.txt"
    if [[ -e "$stdout_log" || -e "$stderr_log" || -e "$time_log" \
          || -e "$command_log" ]]; then
        echo "ERROR: refusing to overwrite existing results for $engine" >&2
        exit 1
    fi
    {
        printf '%q ' "$@"
        printf '\n'
    } > "$command_log"
    echo "=== $engine ==="
    "$TIME_BIN" -v -o "$time_log" "$@" > "$stdout_log" 2> "$stderr_log"
}

run_rust_engine() {
    local engine="$1"
    local runner="$2"
    local result_json="$RESULTS_DIR/${engine}.json"
    local topk_file="$RESULTS_DIR/${engine}.topk.tsv"
    if [[ -e "$result_json" || -e "$topk_file" ]]; then
        echo "ERROR: refusing to overwrite existing outputs for $engine" >&2
        exit 1
    fi
    run_timed "$engine" "$runner" \
        --engine "$engine" \
        --corpus "$CORPUS" \
        --queries "$QUERIES" \
        --index-dir "$INDEX_ROOT/$engine" \
        --output "$result_json" \
        --topk-file "$topk_file" \
        --k "$K" \
        --quality-k "$QUALITY_K" \
        --threads "$THREADS" \
        --warmup-rounds 1 \
        --measured-runs 3 \
        --batch-rows "$BATCH_ROWS"
    validate_output "$result_json" "$topk_file"
}

run_lucene() {
    local result_json="$RESULTS_DIR/lucene.json"
    local topk_file="$RESULTS_DIR/lucene.topk.tsv"
    if [[ -e "$result_json" || -e "$topk_file" ]]; then
        echo "ERROR: refusing to overwrite existing outputs for lucene" >&2
        exit 1
    fi
    "$JAVAC_BIN" -cp "$LUCENE_CP" -d "$LUCENE_CLASS_DIR" "$LUCENE_SOURCE"
    run_timed lucene "$JAVA_BIN" -cp "$LUCENE_CP:$LUCENE_CLASS_DIR" \
        PublicLuceneFtsBench \
        --corpus "$CORPUS" \
        --queries "$QUERIES" \
        --index-dir "$INDEX_ROOT/lucene" \
        --output "$result_json" \
        --topk-file "$topk_file" \
        --k "$K" \
        --quality-k "$QUALITY_K" \
        --threads "$THREADS" \
        --warmup-rounds 1 \
        --measured-runs 3
    validate_output "$result_json" "$topk_file"
}

evaluate_quality() {
    if [[ ! -s "$QRELS" ]]; then
        return
    fi
    local dataset_name
    dataset_name="$(basename "$DATASET_DIR")"
    for engine in lance tantivy lucene; do
        if [[ -e "$RESULTS_DIR/${engine}.quality.json" ]]; then
            echo "ERROR: refusing to overwrite existing quality output for $engine" >&2
            exit 1
        fi
        "$PYTHON_BIN" "$EVALUATOR" \
            --docids "$DOCIDS" \
            --qrels "$QRELS" \
            --topk "$RESULTS_DIR/${engine}.topk.tsv" \
            --engine "$engine" \
            --dataset "$dataset_name" \
            > "$RESULTS_DIR/${engine}.quality.json"
    done
}

write_manifest() {
    local status="$1"
    "$PYTHON_BIN" - "$status" "$REPO_ROOT" "$DATASET_DIR" "$WORK_DIR" \
        "$RESULTS_DIR" "$LANCE_RUNNER" "$TANTIVY_RUNNER" "$LUCENE_CP" \
        "$JAVA_BIN" "$JAVAC_BIN" "$TIME_BIN" "$THREADS" "$K" "$QUALITY_K" \
        "$BATCH_ROWS" "$DATASET_MANIFEST" <<'PY'
import hashlib
import json
import os
import platform
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    status,
    repo_root_text,
    dataset_dir_text,
    work_dir_text,
    results_dir_text,
    lance_runner_text,
    tantivy_runner_text,
    lucene_cp,
    java_bin,
    javac_bin,
    time_bin_text,
    threads,
    k,
    quality_k,
    batch_rows,
    dataset_manifest_text,
) = sys.argv[1:]
repo_root = Path(repo_root_text)
dataset_dir = Path(dataset_dir_text)
work_dir = Path(work_dir_text)
results_dir = Path(results_dir_text)
manifest_path = results_dir / "manifest.json"

def digest(path):
    path = Path(path)
    if not path.is_file():
        return None
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            value.update(chunk)
    return {"bytes": path.stat().st_size, "sha256": value.hexdigest()}

def version(command):
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=15,
        )
        return result.stdout.strip().splitlines()[0] if result.stdout.strip() else None
    except (OSError, subprocess.TimeoutExpired):
        return None

def git(*arguments):
    result = subprocess.run(
        ["git", "-C", str(repo_root), *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None

def memory_bytes():
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None

input_files = {}
for name in ("corpus.txt", "docids.txt", "queries.tsv", "qrels.tsv", "manifest.json"):
    details = digest(dataset_dir / name)
    if details is not None:
        input_files[name] = details

result_files = {}
if results_dir.is_dir():
    for path in sorted(results_dir.iterdir()):
        if path.is_file() and path != manifest_path:
            result_files[path.name] = digest(path)

classpath_files = {}
for entry in lucene_cp.split(os.pathsep):
    details = digest(entry)
    if details is not None:
        classpath_files[str(Path(entry))] = details

runner_files = {}
for name, path in (("lance", lance_runner_text), ("tantivy", tantivy_runner_text)):
    details = digest(path)
    if details is not None:
        runner_files[name] = {"path": str(Path(path)), **details}

dataset_metadata = None
dataset_manifest = Path(dataset_manifest_text)
if dataset_manifest.is_file():
    try:
        dataset_metadata = json.loads(dataset_manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        dataset_metadata = None

manifest = {
    "status": status,
    "written_at_utc": datetime.now(timezone.utc).isoformat(),
    "git": {
        "commit": git("rev-parse", "HEAD"),
        "dirty": bool(git("status", "--porcelain")),
    },
    "host": {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpus": os.cpu_count(),
        "memory_bytes": memory_bytes(),
    },
    "configuration": {
        "dataset_dir": str(dataset_dir),
        "work_dir": str(work_dir),
        "results_dir": str(results_dir),
        "threads": int(threads),
        "k": int(k),
        "quality_k": int(quality_k),
        "batch_rows": int(batch_rows),
        "warmup_rounds": 1,
        "measured_runs": 3,
        "engine_order": ["lance", "tantivy", "lucene"],
    },
    "tool_versions": {
        "python": platform.python_version(),
        "java": version([java_bin, "-version"]),
        "javac": version([javac_bin, "-version"]),
        "rustc": version(["rustc", "--version"]),
        "cargo": version(["cargo", "--version"]),
        "time": version([time_bin_text, "--version"]),
    },
    "runner_files": runner_files,
    "lucene_classpath_files": classpath_files,
    "input_files": input_files,
    "result_files": result_files,
    "dataset_manifest": dataset_metadata,
}
manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY
}

finalize() {
    local return_code=$?
    trap - EXIT
    local status=complete
    if (( return_code != 0 )); then
        status=failed
    fi
    write_manifest "$status" || true
    exit "$return_code"
}
trap finalize EXIT

validate_inputs
run_rust_engine lance "$LANCE_RUNNER"
run_rust_engine tantivy "$TANTIVY_RUNNER"
run_lucene
evaluate_quality

echo "results: $RESULTS_DIR"
echo "indexes retained: $INDEX_ROOT"
