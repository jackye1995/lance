#!/usr/bin/env python3
"""Evaluate public-corpus FTS rankings against qrels.

The benchmark runners emit zero-based corpus row IDs.  This program maps
those rows back to the public dataset's document IDs before computing the
standard retrieval metrics, so row ordering is never mistaken for identity.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Dict, List, Sequence


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docids", required=True, type=Path)
    parser.add_argument("--qrels", required=True, type=Path)
    parser.add_argument("--topk", required=True, type=Path)
    parser.add_argument("--engine", required=True)
    parser.add_argument("--dataset")
    return parser.parse_args()


def read_docids(path: Path) -> List[str]:
    docids: List[str] = []
    seen = set()
    with path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            docid = raw.rstrip("\r\n")
            if not docid:
                raise ValueError(f"{path}:{line_number}: empty document ID")
            if docid in seen:
                raise ValueError(
                    f"{path}:{line_number}: duplicate document ID {docid!r}"
                )
            seen.add(docid)
            docids.append(docid)
    if not docids:
        raise ValueError(f"{path}: no document IDs")
    return docids


def read_qrels(path: Path) -> tuple[Dict[str, Dict[str, int]], List[str]]:
    qrels: Dict[str, Dict[str, int]] = {}
    query_order: List[str] = []
    with path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            line = raw.rstrip("\r\n")
            if not line:
                raise ValueError(f"{path}:{line_number}: blank qrels row")
            fields = line.split("\t")
            if len(fields) != 3:
                raise ValueError(
                    f"{path}:{line_number}: expected qid<TAB>docid<TAB>relevance"
                )
            qid, docid, relevance_text = fields
            if not qid or not docid:
                raise ValueError(f"{path}:{line_number}: empty query or document ID")
            try:
                relevance = int(relevance_text)
            except ValueError as error:
                raise ValueError(
                    f"{path}:{line_number}: relevance is not an integer"
                ) from error
            if relevance < 0:
                raise ValueError(f"{path}:{line_number}: negative relevance")
            if qid not in qrels:
                qrels[qid] = {}
                query_order.append(qid)
            if docid in qrels[qid]:
                raise ValueError(
                    f"{path}:{line_number}: duplicate qrel for {qid!r}, {docid!r}"
                )
            qrels[qid][docid] = relevance
    if not qrels:
        raise ValueError(f"{path}: no qrels")
    return qrels, query_order


def read_rankings(
    path: Path, docids: Sequence[str]
) -> tuple[Dict[str, List[str]], List[str]]:
    rankings: Dict[str, List[str]] = {}
    query_order: List[str] = []
    with path.open(encoding="utf-8") as source:
        for line_number, raw in enumerate(source, 1):
            line = raw.rstrip("\r\n")
            if "\t" not in line:
                raise ValueError(
                    f"{path}:{line_number}: expected qid<TAB>space-separated-row-ids"
                )
            qid, row_ids_text = line.split("\t", 1)
            if not qid:
                raise ValueError(f"{path}:{line_number}: empty query ID")
            if qid in rankings:
                raise ValueError(f"{path}:{line_number}: duplicate query ID {qid!r}")

            row_ids: List[int] = []
            seen_rows = set()
            for token in row_ids_text.split():
                try:
                    row_id = int(token)
                except ValueError as error:
                    raise ValueError(
                        f"{path}:{line_number}: row ID {token!r} is not an integer"
                    ) from error
                if row_id < 0 or row_id >= len(docids):
                    raise ValueError(
                        f"{path}:{line_number}: row ID {row_id} outside "
                        f"[0, {len(docids)})"
                    )
                if row_id in seen_rows:
                    raise ValueError(
                        f"{path}:{line_number}: duplicate row ID {row_id} for {qid!r}"
                    )
                seen_rows.add(row_id)
                row_ids.append(row_id)

            rankings[qid] = [docids[row_id] for row_id in row_ids]
            query_order.append(qid)
    if not rankings:
        raise ValueError(f"{path}: no rankings")
    return rankings, query_order


def dcg(relevances: Sequence[int], cutoff: int) -> float:
    return sum(
        (2.0**relevance - 1.0) / math.log2(rank + 1.0)
        for rank, relevance in enumerate(relevances[:cutoff], 1)
    )


def evaluate_query(ranking: Sequence[str], qrels: Dict[str, int]) -> Dict[str, float]:
    relevance_at_rank = [qrels.get(docid, 0) for docid in ranking]
    ideal = sorted(qrels.values(), reverse=True)
    ideal_dcg = dcg(ideal, 10)
    ndcg = dcg(relevance_at_rank, 10) / ideal_dcg if ideal_dcg else 0.0

    mrr = 0.0
    for rank, relevance in enumerate(relevance_at_rank[:10], 1):
        if relevance > 0:
            mrr = 1.0 / rank
            break

    relevant = {docid for docid, relevance in qrels.items() if relevance > 0}
    retrieved_relevant = 0
    precision_sum = 0.0
    for rank, docid in enumerate(ranking[:100], 1):
        if docid in relevant:
            retrieved_relevant += 1
            precision_sum += retrieved_relevant / rank

    recall = retrieved_relevant / len(relevant) if relevant else 0.0
    ap_denominator = min(len(relevant), 100)
    average_precision = precision_sum / ap_denominator if ap_denominator else 0.0
    return {
        "ndcg_at_10": ndcg,
        "mrr_at_10": mrr,
        "recall_at_100": recall,
        "map_at_100": average_precision,
    }


def main() -> None:
    args = parse_args()
    docids = read_docids(args.docids)
    qrels, qrel_order = read_qrels(args.qrels)
    rankings, ranking_order = read_rankings(args.topk, docids)

    qrel_qids = set(qrels)
    ranking_qids = set(rankings)
    missing = [qid for qid in qrel_order if qid not in ranking_qids]
    extra = [qid for qid in ranking_order if qid not in qrel_qids]
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing qids={missing[:10]!r}")
        if extra:
            details.append(f"unexpected qids={extra[:10]!r}")
        raise ValueError("ranking/qrels query ID mismatch: " + "; ".join(details))

    corpus_docids = set(docids)
    missing_docids = sorted(
        {
            docid
            for judgments in qrels.values()
            for docid in judgments
            if docid not in corpus_docids
        }
    )
    if missing_docids:
        raise ValueError(
            "qrels reference document IDs absent from docids.txt: "
            f"{missing_docids[:10]!r}"
        )

    totals = {
        "ndcg_at_10": 0.0,
        "mrr_at_10": 0.0,
        "recall_at_100": 0.0,
        "map_at_100": 0.0,
    }
    for qid in qrel_order:
        per_query = evaluate_query(rankings[qid], qrels[qid])
        for metric, value in per_query.items():
            totals[metric] += value

    query_count = len(qrel_order)
    result = {
        "engine": args.engine,
        "dataset": args.dataset,
        "document_count": len(docids),
        "query_count": query_count,
        "ranking_depth_max": max(len(ranking) for ranking in rankings.values()),
        "metrics": {metric: value / query_count for metric, value in totals.items()},
    }
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
