"""Paired comparison of canonical all-at-once and graph results."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable


AXES = ("actor", "when", "mode", "all")


def read_rows(root: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for path in sorted(root.glob("*.jsonl")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                trace_id = row.get("trace_id")
                if not isinstance(trace_id, str) or trace_id in rows:
                    raise ValueError(f"invalid or duplicate trace_id in {path}")
                rows[trace_id] = row
    return rows


def metric(rows: Iterable[dict[str, Any]], axis: str) -> dict[str, Any]:
    values = [row.get("score", {}).get(axis) for row in rows]
    values = [value for value in values if value is not None]
    correct = sum(bool(value) for value in values)
    return {"correct": correct, "denominator": len(values),
            "accuracy": correct / len(values) if values else None}


def grouped_metrics(rows: list[dict[str, Any]], key_fn: Callable[[dict[str, Any]], Any]) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(key_fn(row) or "unknown")].append(row)
    return {
        key: {axis: metric(group, axis) for axis in AXES}
        for key, group in sorted(groups.items())
    }


def paired_axis(baseline: dict[str, dict[str, Any]], graph: dict[str, dict[str, Any]], axis: str) -> dict[str, Any]:
    baseline_correct = graph_correct = unchanged_correct = unchanged_incorrect = 0
    baseline_denominator = graph_denominator = 0
    for trace_id in sorted(baseline):
        base_value = baseline[trace_id].get("score", {}).get(axis)
        graph_value = graph[trace_id].get("score", {}).get(axis)
        if base_value is None or graph_value is None:
            if base_value is not None or graph_value is not None:
                raise ValueError(f"paired score denominator differs for {axis} at {trace_id}")
            continue
        baseline_denominator += 1
        graph_denominator += 1
        baseline_correct += bool(base_value)
        graph_correct += bool(graph_value)
        if base_value and graph_value:
            unchanged_correct += 1
        elif not base_value and not graph_value:
            unchanged_incorrect += 1
    graph_wins = graph_only_correct = 0
    baseline_wins = baseline_only_correct = 0
    for trace_id in sorted(baseline):
        a = baseline[trace_id].get("score", {}).get(axis)
        b = graph[trace_id].get("score", {}).get(axis)
        if a is None or b is None:
            continue
        if b and not a:
            graph_wins += 1
        elif a and not b:
            baseline_wins += 1
    if baseline_denominator != graph_denominator:
        raise ValueError(f"paired score denominators differ for {axis}")
    baseline_accuracy = baseline_correct / baseline_denominator if baseline_denominator else None
    graph_accuracy = graph_correct / graph_denominator if graph_denominator else None
    return {
        "baseline": {"correct": baseline_correct, "denominator": baseline_denominator,
                     "accuracy": baseline_accuracy},
        "graph": {"correct": graph_correct, "denominator": graph_denominator,
                  "accuracy": graph_accuracy},
        "delta_accuracy": graph_accuracy - baseline_accuracy if baseline_accuracy is not None else None,
        "graph_only_correct": graph_wins,
        "baseline_only_correct": baseline_wins,
        "both_correct": unchanged_correct,
        "both_incorrect": unchanged_incorrect,
    }


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path, help="Merged baseline result directory")
    parser.add_argument("--graph", required=True, type=Path, help="Merged graph result directory")
    parser.add_argument("--out", required=True, type=Path, help="Output JSON path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argparser().parse_args(argv)
    baseline = read_rows(args.baseline)
    graph = read_rows(args.graph)
    if not baseline or set(baseline) != set(graph):
        raise ValueError(
            f"paired trace IDs differ: baseline={len(baseline)} graph={len(graph)} "
            f"missing={len(set(baseline) - set(graph))} extra={len(set(graph) - set(baseline))}"
        )
    if any(row.get("method", "canonical") != "canonical" for row in baseline.values()):
        raise ValueError("baseline directory contains non-canonical rows")
    if any(row.get("method") != "graph" for row in graph.values()):
        raise ValueError("graph directory contains non-graph rows")
    for trace_id in baseline:
        if baseline[trace_id].get("canonical_ground_truth") != graph[trace_id].get("canonical_ground_truth"):
            raise ValueError(f"hidden canonical labels differ between runs at {trace_id}")

    base_rows = [baseline[key] for key in sorted(baseline)]
    graph_rows = [graph[key] for key in sorted(graph)]
    comparison = {
        "records": len(baseline),
        "baseline_protocols": sorted({row.get("protocol", "unknown") for row in base_rows}),
        "graph_protocols": sorted({row.get("protocol", "unknown") for row in graph_rows}),
        "paired_metrics": {axis: paired_axis(baseline, graph, axis) for axis in AXES},
        "by_benchmark": {
            "baseline": grouped_metrics(base_rows, lambda row: row.get("benchmark")),
            "graph": grouped_metrics(graph_rows, lambda row: row.get("benchmark")),
        },
        "by_framework": {
            "baseline": grouped_metrics(base_rows, lambda row: row.get("framework")),
            "graph": grouped_metrics(graph_rows, lambda row: row.get("framework")),
        },
        "by_error_mode": {
            "baseline": grouped_metrics(base_rows, lambda row: (row.get("canonical_ground_truth") or {}).get("mode")),
            "graph": grouped_metrics(graph_rows, lambda row: (row.get("canonical_ground_truth") or {}).get("mode")),
        },
        "usage": {
            "baseline": {key: sum((row.get("usage") or {}).get(key) or 0 for row in base_rows)
                          for key in ("input_tokens", "output_tokens", "total_tokens")},
            "graph": {key: sum((row.get("usage") or {}).get(key) or 0 for row in graph_rows)
                      for key in ("input_tokens", "output_tokens", "total_tokens")},
        },
        "visible_outputs": {
            "baseline": sum(bool(row.get("raw_output")) for row in base_rows),
            "graph": sum(bool(row.get("raw_output")) for row in graph_rows),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(comparison, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                         encoding="utf-8")
    print(json.dumps(comparison["paired_metrics"], ensure_ascii=False, indent=2))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
