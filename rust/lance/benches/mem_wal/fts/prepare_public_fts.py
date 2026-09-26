#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors
"""Prepare reproducible public corpora for the MemWAL FTS benchmarks."""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import io
import json
import os
import re
import stat
import sys
import tempfile
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable


BEIR_BASE_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets"
FINEWEB_EDU_URI = "hf://datasets/lance-format/fineweb-edu/data/train.lance"
FINEWEB_EDU_REVISION = "8ffa2f205192ef1f50da9735e92ec50693cb485d"
FINEWEB_EDU_VERSION = 66
FINEWEB_QUERY_COUNT = 1_000
MANIFEST_VERSION = 1
TOKENIZER_NAME = "python-unicode-word-lower-v1"
TOKENIZER_DEFINITION = r're.findall(r"\w+", text.lower(), flags=re.UNICODE)'
TOKEN_RE = re.compile(r"\w+", flags=re.UNICODE)


@dataclass(frozen=True)
class BeirSpec:
    md5: str
    split: str
    corpus_count: int
    query_count: int
    qrel_count: int

    @property
    def expected_counts(self) -> dict[str, int]:
        return {
            "corpus": self.corpus_count,
            "queries": self.query_count,
            "qrels": self.qrel_count,
        }


BEIR_SPECS = {
    "msmarco": BeirSpec(
        md5="444067daf65d982533ea17ebd59501e4",
        split="dev",
        corpus_count=8_841_823,
        query_count=6_980,
        qrel_count=7_437,
    ),
    "nq": BeirSpec(
        md5="d4d3d2e48787a744b6f6e691ff534307",
        split="test",
        corpus_count=2_681_468,
        query_count=3_452,
        qrel_count=4_201,
    ),
    "hotpotqa": BeirSpec(
        md5="f412724f78b0d91183a0e86805e16114",
        split="test",
        corpus_count=5_233_329,
        query_count=7_405,
        qrel_count=14_810,
    ),
    "dbpedia-entity": BeirSpec(
        md5="c2a39eb420a3164af735795df012ac2c",
        split="test",
        corpus_count=4_635_922,
        query_count=400,
        qrel_count=43_515,
    ),
}


class DigestWriter:
    def __init__(self, path: Path):
        self.path = path
        self._file: BinaryIO | None = None
        self._sha256 = hashlib.sha256()
        self.size = 0

    def __enter__(self) -> DigestWriter:
        self._file = self.path.open("wb")
        return self

    def write_line(self, *fields: str) -> None:
        if self._file is None:
            raise RuntimeError("writer is not open")
        line = "\t".join(fields).encode("utf-8") + b"\n"
        self._file.write(line)
        self._sha256.update(line)
        self.size += len(line)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self._file is not None:
            self._file.close()

    @property
    def metadata(self) -> dict[str, Any]:
        return {"bytes": self.size, "sha256": self._sha256.hexdigest()}


def canonicalize_text(value: str) -> str:
    return " ".join(re.findall(r"\w+", value.lower(), flags=re.UNICODE))


def tokenizer_manifest() -> dict[str, str]:
    return {"definition": TOKENIZER_DEFINITION, "name": TOKENIZER_NAME}


def checked_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    if "\t" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{label} contains a TSV delimiter or line break")
    return value


def md5_digest() -> Any:
    try:
        return hashlib.md5(usedforsecurity=False)
    except TypeError:
        return hashlib.md5()  # noqa: S324 -- verifies the publisher's legacy MD5


def digest_file(path: Path, algorithm: str) -> str:
    digest = md5_digest() if algorithm == "md5" else hashlib.new(algorithm)
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def reusable_manifest(
    output_dir: Path, request: dict[str, Any], output_names: Iterable[str]
) -> dict[str, Any] | None:
    manifest_path = output_dir / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if (
        manifest.get("manifest_version") != MANIFEST_VERSION
        or manifest.get("status") != "complete"
        or manifest.get("request") != request
    ):
        return None
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != set(output_names):
        return None
    for name in output_names:
        path = output_dir / name
        recorded = files.get(name)
        if not path.is_file() or not isinstance(recorded, dict):
            return None
        if path.stat().st_size != recorded.get("bytes"):
            return None
        if digest_file(path, "sha256") != recorded.get("sha256"):
            return None
    return manifest


def publish_outputs(
    output_dir: Path,
    staging_dir: Path,
    names: Iterable[str],
    manifest: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        os.replace(staging_dir / name, output_dir / name)
    manifest_tmp = output_dir / f".manifest.{os.getpid()}.tmp"
    manifest_tmp.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(manifest_tmp, output_dir / "manifest.json")


def download_beir_archive(dataset: str, spec: BeirSpec, cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    destination = cache_dir / f"{dataset}.zip"
    if destination.is_file() and digest_file(destination, "md5") == spec.md5:
        return destination
    destination.unlink(missing_ok=True)

    url = f"{BEIR_BASE_URL}/{dataset}.zip"
    temporary = cache_dir / f".{dataset}.{os.getpid()}.part"
    temporary.unlink(missing_ok=True)
    md5 = md5_digest()
    try:
        request = urllib.request.Request(
            url, headers={"User-Agent": "lance-mem-wal-public-fts/1"}
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            with temporary.open("wb") as output:
                if urllib.parse.urlparse(response.geturl()).scheme != "https":
                    raise RuntimeError(
                        "refusing a BEIR download redirected away from HTTPS"
                    )
                while block := response.read(8 * 1024 * 1024):
                    output.write(block)
                    md5.update(block)
        if md5.hexdigest() != spec.md5:
            raise ValueError(
                f"{dataset} archive MD5 mismatch: expected {spec.md5}, "
                f"got {md5.hexdigest()}"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def checked_zip_member(
    archive: zipfile.ZipFile, dataset: str, relative_path: str
) -> zipfile.ZipInfo:
    suffix = (dataset, *PurePosixPath(relative_path).parts)
    matches = []
    for info in archive.infolist():
        path = PurePosixPath(info.filename)
        parts = path.parts
        if path.is_absolute() or any(part in {"", ".", ".."} for part in parts):
            continue
        file_type = (info.external_attr >> 16) & 0o170000
        if file_type == stat.S_IFLNK:
            continue
        if not info.is_dir() and tuple(parts[-len(suffix) :]) == suffix:
            matches.append(info)
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one safe {dataset}/{relative_path} member, "
            f"found {len(matches)}"
        )
    return matches[0]


def json_lines(archive: zipfile.ZipFile, member: zipfile.ZipInfo) -> Iterable[dict]:
    with archive.open(member, "r") as raw:
        for line_number, line in enumerate(raw, start=1):
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"invalid JSON in {member.filename}:{line_number}"
                ) from error
            if not isinstance(value, dict):
                raise ValueError(
                    f"expected an object in {member.filename}:{line_number}"
                )
            yield value


def prepare_beir(args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    dataset = args.dataset
    spec = BEIR_SPECS[dataset]
    output_dir = args.output.expanduser().resolve()
    url = f"{BEIR_BASE_URL}/{dataset}.zip"
    request = {
        "dataset": dataset,
        "kind": "beir",
        "md5": spec.md5,
        "query_operator": "or",
        "source_url": url,
        "split": spec.split,
        "tokenizer": TOKENIZER_NAME,
    }
    output_names = ("corpus.txt", "docids.txt", "queries.tsv", "qrels.tsv")
    if manifest := reusable_manifest(output_dir, request, output_names):
        return manifest, True

    archive_path = download_beir_archive(dataset, spec, args.cache_dir.expanduser())
    archive_sha256 = digest_file(archive_path, "sha256")
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".prepare-", dir=output_dir) as temporary:
        staging_dir = Path(temporary)
        with zipfile.ZipFile(archive_path, "r") as archive:
            corpus_member = checked_zip_member(archive, dataset, "corpus.jsonl")
            queries_member = checked_zip_member(archive, dataset, "queries.jsonl")
            qrels_member = checked_zip_member(
                archive, dataset, f"qrels/{spec.split}.tsv"
            )

            qrel_qids: set[str] = set()
            qrel_count = 0
            with DigestWriter(staging_dir / "qrels.tsv") as qrels_output:
                with archive.open(qrels_member, "r") as raw:
                    with io.TextIOWrapper(raw, encoding="utf-8", newline="") as source:
                        reader = csv.DictReader(source, delimiter="\t")
                        required = {"query-id", "corpus-id", "score"}
                        if reader.fieldnames is None or not required.issubset(
                            reader.fieldnames
                        ):
                            raise ValueError(
                                f"unexpected qrels header in {qrels_member.filename}: "
                                f"{reader.fieldnames}"
                            )
                        for row in reader:
                            qid = checked_id(row["query-id"], "qrel query id")
                            docid = checked_id(row["corpus-id"], "qrel document id")
                            try:
                                relevance = str(int(row["score"]))
                            except (TypeError, ValueError) as error:
                                raise ValueError(
                                    f"invalid relevance for query {qid}: "
                                    f"{row['score']!r}"
                                ) from error
                            qrels_output.write_line(qid, docid, relevance)
                            qrel_qids.add(qid)
                            qrel_count += 1

            query_count = 0
            seen_qids: set[str] = set()
            with DigestWriter(staging_dir / "queries.tsv") as queries_output:
                for row in json_lines(archive, queries_member):
                    qid = checked_id(row.get("_id"), "query id")
                    if qid not in qrel_qids:
                        continue
                    if qid in seen_qids:
                        raise ValueError(f"duplicate query id in queries.jsonl: {qid}")
                    text = row.get("text")
                    if not isinstance(text, str):
                        raise ValueError(f"query {qid} has a non-string text field")
                    queries_output.write_line(qid, canonicalize_text(text))
                    seen_qids.add(qid)
                    query_count += 1

            if seen_qids != qrel_qids:
                missing = sorted(qrel_qids - seen_qids)[:5]
                raise ValueError(f"qrels reference missing query ids: {missing}")

            corpus_count = 0
            with DigestWriter(staging_dir / "corpus.txt") as corpus_output:
                with DigestWriter(staging_dir / "docids.txt") as docids_output:
                    for row in json_lines(archive, corpus_member):
                        docid = checked_id(row.get("_id"), "document id")
                        title = row.get("title", "") or ""
                        text = row.get("text")
                        if not isinstance(title, str) or not isinstance(text, str):
                            raise ValueError(
                                f"document {docid} has a non-string title or text field"
                            )
                        corpus_output.write_line(canonicalize_text(f"{title} {text}"))
                        docids_output.write_line(docid)
                        corpus_count += 1

        counts = {
            "corpus": corpus_count,
            "queries": query_count,
            "qrels": qrel_count,
        }
        if counts != spec.expected_counts:
            raise ValueError(
                f"{dataset}/{spec.split} count mismatch: expected "
                f"{spec.expected_counts}, got {counts}"
            )
        files = {
            "corpus.txt": corpus_output.metadata,
            "docids.txt": docids_output.metadata,
            "queries.tsv": queries_output.metadata,
            "qrels.tsv": qrels_output.metadata,
        }
        manifest = {
            "counts": counts,
            "dataset": dataset,
            "files": files,
            "manifest_version": MANIFEST_VERSION,
            "request": request,
            "query_operator": "or",
            "source": {
                "archive_sha256": archive_sha256,
                "md5": spec.md5,
                "url": url,
            },
            "split": spec.split,
            "status": "complete",
            "tokenizer": tokenizer_manifest(),
        }
        publish_outputs(output_dir, staging_dir, output_names, manifest)
    return manifest, False


def parse_rows(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([kKmMgG]?)", value)
    if match is None:
        raise argparse.ArgumentTypeError(
            "rows must be a positive integer with an optional k, m, or g suffix"
        )
    scale = {"": 1, "k": 1_000, "m": 1_000_000, "g": 1_000_000_000}
    return int(match.group(1)) * scale[match.group(2).lower()]


def fragment_sort_key(seed: int, fragment_id: int) -> bytes:
    value = f"{seed}\0{fragment_id}".encode("ascii")
    return hashlib.sha256(value).digest()


def query_from_document(text: str, docid: str, seed: int) -> str | None:
    tokens: list[str] = []
    seen: set[str] = set()
    for token in TOKEN_RE.findall(text):
        if token not in seen:
            seen.add(token)
            tokens.append(token)
    if len(tokens) < 2:
        return None
    digest = hashlib.sha256(f"{seed}\0{docid}".encode("utf-8")).digest()
    first = int.from_bytes(digest[:8], "big") % len(tokens)
    second = int.from_bytes(digest[8:16], "big") % (len(tokens) - 1)
    if second >= first:
        second += 1
    return f"{tokens[first]} {tokens[second]}"


def prepare_fineweb_edu(args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    output_dir = args.output.expanduser().resolve()
    request = {
        "dataset": "lance-format/fineweb-edu",
        "hf_download_mode": "http",
        "kind": "fineweb-edu",
        "lance_version": FINEWEB_EDU_VERSION,
        "query_operator": "or",
        "query_count": FINEWEB_QUERY_COUNT,
        "revision": FINEWEB_EDU_REVISION,
        "rows": args.rows,
        "seed": args.seed,
        "source_uri": FINEWEB_EDU_URI,
        "tokenizer": TOKENIZER_NAME,
    }
    output_names = ("corpus.txt", "docids.txt", "queries.tsv")
    if manifest := reusable_manifest(output_dir, request, output_names):
        (output_dir / "qrels.tsv").unlink(missing_ok=True)
        return manifest, True

    try:
        lance = importlib.import_module("lance")
        pa = importlib.import_module("pyarrow")
    except ImportError as error:
        raise RuntimeError(
            "the fineweb-edu command requires the lance and pyarrow packages"
        ) from error

    dataset = lance.dataset(
        FINEWEB_EDU_URI,
        version=FINEWEB_EDU_VERSION,
        storage_options={
            "hf_download_mode": "http",
            "hf_revision": FINEWEB_EDU_REVISION,
        },
    )
    if dataset.version != FINEWEB_EDU_VERSION:
        raise ValueError(
            f"expected Lance version {FINEWEB_EDU_VERSION}, got {dataset.version}"
        )
    expected_schema = {
        "text": pa.string(),
        "id": pa.string(),
        "token_count": pa.int64(),
    }
    for name, expected_type in expected_schema.items():
        if name not in dataset.schema.names:
            raise ValueError(f"FineWeb-Edu schema is missing {name!r}")
        actual_type = dataset.schema.field(name).type
        if actual_type != expected_type:
            raise ValueError(
                f"FineWeb-Edu field {name!r} has type {actual_type}, "
                f"expected {expected_type}"
            )

    total_rows = dataset.count_rows()
    if total_rows < args.rows:
        raise ValueError(
            f"FineWeb-Edu version {FINEWEB_EDU_VERSION} has {total_rows} rows, "
            f"fewer than requested {args.rows}"
        )
    fragments = sorted(
        dataset.get_fragments(),
        key=lambda fragment: fragment_sort_key(args.seed, fragment.fragment_id),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    scanned_fragment_ids = []
    corpus_count = 0
    query_count = 0
    with tempfile.TemporaryDirectory(prefix=".prepare-", dir=output_dir) as temporary:
        staging_dir = Path(temporary)
        with DigestWriter(staging_dir / "corpus.txt") as corpus_output:
            with DigestWriter(staging_dir / "docids.txt") as docids_output:
                with DigestWriter(staging_dir / "queries.tsv") as queries_output:
                    for fragment in fragments:
                        if corpus_count >= args.rows:
                            break
                        scanned_fragment_ids.append(fragment.fragment_id)
                        for batch in fragment.to_batches(
                            columns=["text", "id", "token_count"],
                            batch_size=args.batch_size,
                        ):
                            remaining = args.rows - corpus_count
                            if remaining == 0:
                                break
                            batch = batch.slice(0, min(batch.num_rows, remaining))
                            texts = batch.column(
                                batch.schema.get_field_index("text")
                            ).to_pylist()
                            ids = batch.column(
                                batch.schema.get_field_index("id")
                            ).to_pylist()
                            token_counts = batch.column(
                                batch.schema.get_field_index("token_count")
                            ).to_pylist()
                            for text, docid_value, token_count in zip(
                                texts, ids, token_counts
                            ):
                                docid = checked_id(
                                    docid_value, "FineWeb-Edu document id"
                                )
                                if not isinstance(text, str) or not isinstance(
                                    token_count, int
                                ):
                                    raise ValueError(
                                        f"FineWeb-Edu document {docid} has null or "
                                        "invalid text/token_count"
                                    )
                                canonical = canonicalize_text(text)
                                corpus_output.write_line(canonical)
                                docids_output.write_line(docid)
                                if (
                                    query_count < FINEWEB_QUERY_COUNT
                                    and token_count >= 2
                                ):
                                    query = query_from_document(
                                        canonical, docid, args.seed
                                    )
                                    if query is not None:
                                        queries_output.write_line(
                                            f"fineweb-edu-{query_count:06d}", query
                                        )
                                        query_count += 1
                                corpus_count += 1

        if corpus_count != args.rows:
            raise ValueError(
                f"FineWeb-Edu scan produced {corpus_count} rows, expected {args.rows}"
            )
        if query_count != FINEWEB_QUERY_COUNT:
            raise ValueError(
                f"FineWeb-Edu scan produced {query_count} eligible queries, "
                f"expected {FINEWEB_QUERY_COUNT}"
            )
        files = {
            "corpus.txt": corpus_output.metadata,
            "docids.txt": docids_output.metadata,
            "queries.tsv": queries_output.metadata,
        }
        fragment_order_sha256 = hashlib.sha256(
            ",".join(str(fragment.fragment_id) for fragment in fragments).encode(
                "ascii"
            )
        ).hexdigest()
        manifest = {
            "counts": {
                "corpus": corpus_count,
                "queries": query_count,
                "qrels": 0,
            },
            "dataset": "lance-format/fineweb-edu",
            "files": files,
            "manifest_version": MANIFEST_VERSION,
            "request": request,
            "query_operator": "or",
            "sampling": {
                "fragment_order": "sha256(seed NUL fragment_id)",
                "fragment_order_sha256": fragment_order_sha256,
                "scanned_fragment_ids": scanned_fragment_ids,
            },
            "schema": {name: str(kind) for name, kind in expected_schema.items()},
            "source": {
                "lance_version": FINEWEB_EDU_VERSION,
                "revision": FINEWEB_EDU_REVISION,
                "row_count": total_rows,
                "uri": FINEWEB_EDU_URI,
            },
            "split": "train",
            "status": "complete",
            "tokenizer": tokenizer_manifest(),
        }
        (output_dir / "qrels.tsv").unlink(missing_ok=True)
        publish_outputs(output_dir, staging_dir, output_names, manifest)
    return manifest, False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    beir = subparsers.add_parser("beir", help="prepare an official BEIR dataset")
    beir.add_argument("--dataset", choices=sorted(BEIR_SPECS), required=True)
    beir.add_argument("--output", type=Path, required=True)
    beir.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("~/.cache/lance-mem-wal/fts/beir"),
    )
    beir.set_defaults(handler=prepare_beir)

    fineweb = subparsers.add_parser(
        "fineweb-edu", help="prepare a deterministic FineWeb-Edu prefix"
    )
    fineweb.add_argument("--output", type=Path, required=True)
    fineweb.add_argument("--rows", type=parse_rows, default=parse_rows("1m"))
    fineweb.add_argument("--seed", type=int, default=42)
    fineweb.add_argument("--batch-size", type=int, default=8_192)
    fineweb.set_defaults(handler=prepare_fineweb_edu)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if getattr(args, "batch_size", 1) <= 0:
        raise ValueError("batch size must be positive")
    manifest, reused = args.handler(args)
    summary = {
        "counts": manifest["counts"],
        "dataset": manifest["request"]["dataset"],
        "output": str(args.output.expanduser().resolve()),
        "reused": reused,
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, RuntimeError, ValueError, zipfile.BadZipFile) as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
