"""Merge canonical-PG retries and calculate reproducible pilot metrics."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .prompts import load_taxonomy


def read_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.glob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    return rows


def index_rows(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for row in rows:
        trace_id = row.get("trace_id")
        if not isinstance(trace_id, str):
            raise ValueError("result row lacks a string trace_id")
        if trace_id in indexed:
            raise ValueError(f"duplicate trace_id in one result root: {trace_id}")
        indexed[trace_id] = row
    return indexed


def usage_totals(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    keys = ("input_tokens", "output_tokens", "total_tokens")
    return {key: sum((row.get("usage") or {}).get(key) or 0 for row in rows) for key in keys}


def rate(rows: list[dict[str, Any]], axis: str) -> dict[str, Any]:
    values = [row["score"][axis] for row in rows if row.get("score", {}).get(axis) is not None]
    correct = sum(bool(value) for value in values)
    return {
        "correct": correct,
        "denominator": len(values),
        "accuracy": correct / len(values) if values else None,
    }


def mode_macro_f1(rows: list[dict[str, Any]], codes: tuple[str, ...]) -> dict[str, Any]:
    per_code: dict[str, dict[str, Any]] = {}
    for code in codes:
        tp = fp = fn = 0
        for row in rows:
            gold = ((row.get("canonical_ground_truth") or {}).get("mode"))
            predicted = ((row.get("prediction") or {}).get("error_mode"))
            if predicted == code and gold == code:
                tp += 1
            elif predicted == code:
                fp += 1
            elif gold == code:
                fn += 1
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_code[code] = {"tp": tp, "fp": fp, "fn": fn, "f1": f1}
    return {
        "codes": len(codes),
        "macro_f1": sum(item["f1"] for item in per_code.values()) / len(codes),
        "per_code": per_code,
    }


def expected_trace_ids(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    with path.open(encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip() and not line.startswith("#")}


def write_rows(rows: list[dict[str, Any]], out: Path) -> None:
    by_benchmark: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_benchmark.setdefault(str(row["benchmark"]), []).append(row)
    out.mkdir(parents=True, exist_ok=True)
    for benchmark, benchmark_rows in by_benchmark.items():
        destination = out / f"{benchmark}.jsonl"
        with destination.open("w", encoding="utf-8") as handle:
            for row in sorted(benchmark_rows, key=lambda item: item["trace_id"]):
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Merge and summarize canonical-PG result retries")
    parser.add_argument("--primary", required=True, type=Path, help="Model result directory containing JSONL files")
    parser.add_argument("--retry", action="append", type=Path, default=[], help="Later retry result directory; may repeat")
    parser.add_argument("--taxonomy-path", required=True, type=Path)
    parser.add_argument("--expected-trace-id-file", type=Path, default=None)
    parser.add_argument("--out", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argparser().parse_args(argv)
    primary_rows = read_rows(args.primary)
    merged = index_rows(primary_rows)
    retry_rows: list[dict[str, Any]] = []
    replaced_trace_ids: set[str] = set()
    unresolved_retry_trace_ids: set[str] = set()
    for root in args.retry:
        for row in read_rows(root):
            trace_id = row["trace_id"]
            if trace_id not in merged:
                raise ValueError(f"retry trace is absent from primary results: {trace_id}")
            if not row.get("raw_output") and merged[trace_id].get("raw_output"):
                raise ValueError(f"retry would replace a visible output with an empty one: {trace_id}")
            merged[trace_id] = row
            retry_rows.append(row)
            replaced_trace_ids.add(trace_id)
            if not row.get("raw_output"):
                unresolved_retry_trace_ids.add(trace_id)
    expected = expected_trace_ids(args.expected_trace_id_file)
    if expected is not None and set(merged) != expected:
        missing = sorted(expected - set(merged))
        extra = sorted(set(merged) - expected)
        raise ValueError(f"merged result IDs differ from manifest; missing={missing[:3]} extra={extra[:3]}")

    merged_rows = sorted(merged.values(), key=lambda row: (row["benchmark"], row["trace_id"]))
    write_rows(merged_rows, args.out)
    taxonomy = load_taxonomy(args.taxonomy_path.parent)
    summary = {
        "protocols": sorted({row.get("protocol", "unknown") for row in merged_rows}),
        "methods": sorted({row.get("method", "canonical") for row in merged_rows}),
        "records": len(merged_rows),
        "replaced_traces": len(replaced_trace_ids),
        "retry_attempt_records": len(retry_rows),
        "unresolved_retry_trace_ids": sorted(unresolved_retry_trace_ids),
        "errors": sum(bool(row.get("error")) for row in merged_rows),
        "empty_visible_outputs": sum(not row.get("raw_output") for row in merged_rows),
        "parse_warning_rows": sum(bool((row.get("prediction") or {}).get("parse_warnings")) for row in merged_rows),
        "metrics": {
            "actor": rate(merged_rows, "actor"),
            "when": rate(merged_rows, "when"),
            "mode_accuracy": rate(merged_rows, "mode"),
            "all": rate(merged_rows, "all"),
            "mode_macro_f1": mode_macro_f1(merged_rows, taxonomy.codes),
        },
        "coverage": {
            "benchmarks": dict(sorted(Counter(row["benchmark"] for row in merged_rows).items())),
            "frameworks": dict(sorted(Counter(row["framework"] for row in merged_rows).items())),
        },
        "effective_usage": usage_totals(merged_rows),
        "attempt_usage": usage_totals([*primary_rows, *retry_rows]),
        "effective_duration_s": sum(row.get("duration_s") or 0.0 for row in merged_rows),
        "attempt_duration_s": sum(row.get("duration_s") or 0.0 for row in [*primary_rows, *retry_rows]),
    }
    (args.out / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
