"""Paired comparison of canonical all-at-once and graph results."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Iterable

from .dag_blade import METHOD as DAG_BLADE_METHOD
from .prompts import load_taxonomy
from .summarize_canonical_pg import mode_macro_f1


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


def expected_trace_ids(path: Path | None) -> set[str] | None:
    if path is None:
        return None
    with path.open(encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip() and not line.startswith("#")}


def _method_summary(rows: list[dict[str, Any]], taxonomy_codes: tuple[str, ...]) -> dict[str, Any]:
    metrics: dict[str, Any] = {axis: metric(rows, axis) for axis in AXES}
    if taxonomy_codes:
        metrics["mode_macro_f1"] = mode_macro_f1(rows, taxonomy_codes)
    return {
        "protocols": sorted({row.get("protocol", "unknown") for row in rows}),
        "methods": sorted({row.get("method", "canonical") for row in rows}),
        "records": len(rows),
        "metrics": metrics,
        "by_benchmark": grouped_metrics(rows, lambda row: row.get("benchmark")),
        "by_framework": grouped_metrics(rows, lambda row: row.get("framework")),
        "usage": {
            key: sum((row.get("usage") or {}).get(key) or 0 for row in rows)
            for key in ("input_tokens", "output_tokens", "total_tokens")
        },
        "visible_outputs": sum(bool(row.get("raw_output")) for row in rows),
    }


def build_comparison(
    baseline: dict[str, dict[str, Any]],
    graph: dict[str, dict[str, Any]],
    *,
    blade: dict[str, dict[str, Any]] | None = None,
    expected_ids: set[str] | None = None,
    taxonomy_codes: tuple[str, ...] = (),
) -> dict[str, Any]:
    runs = {"all_at_once": baseline, "dag": graph}
    if blade is not None:
        runs["dag_blade"] = blade
    reference_ids = set(baseline)
    for name, indexed in runs.items():
        if not indexed or set(indexed) != reference_ids:
            raise ValueError(
                f"paired trace IDs differ for {name}: expected={len(reference_ids)} "
                f"actual={len(indexed)} missing={len(reference_ids - set(indexed))} "
                f"extra={len(set(indexed) - reference_ids)}"
            )
    if expected_ids is not None and reference_ids != expected_ids:
        raise ValueError(
            f"trace IDs differ from expected manifest: expected={len(expected_ids)} "
            f"actual={len(reference_ids)} missing={len(expected_ids - reference_ids)} "
            f"extra={len(reference_ids - expected_ids)}"
        )

    expected_methods = {"all_at_once": "canonical", "dag": "graph", "dag_blade": DAG_BLADE_METHOD}
    for name, indexed in runs.items():
        if any(row.get("method", "canonical") != expected_methods[name] for row in indexed.values()):
            raise ValueError(f"{name} result directory contains unexpected method rows")
    for trace_id in reference_ids:
        gold = baseline[trace_id].get("canonical_ground_truth")
        for name, indexed in runs.items():
            if indexed[trace_id].get("canonical_ground_truth") != gold:
                raise ValueError(f"hidden canonical labels differ for {name} at {trace_id}")

    ordered = {
        name: [indexed[key] for key in sorted(indexed)]
        for name, indexed in runs.items()
    }
    methods = {
        name: _method_summary(rows, taxonomy_codes)
        for name, rows in ordered.items()
    }
    pairs = [("all_at_once", "dag")]
    if blade is not None:
        pairs.extend([("all_at_once", "dag_blade"), ("dag", "dag_blade")])
    pairwise: dict[str, Any] = {}
    for left_name, right_name in pairs:
        key = f"{left_name}_vs_{right_name}"
        left, right = runs[left_name], runs[right_name]
        pairwise[key] = {
            "left_method": left_name,
            "right_method": right_name,
            "metrics": {axis: paired_axis(left, right, axis) for axis in AXES},
        }
        if taxonomy_codes:
            left_f1 = methods[left_name]["metrics"]["mode_macro_f1"]["macro_f1"]
            right_f1 = methods[right_name]["metrics"]["mode_macro_f1"]["macro_f1"]
            pairwise[key]["mode_macro_f1_delta"] = right_f1 - left_f1

    result = {
        "records": len(reference_ids),
        "expected_trace_ids_verified": expected_ids is not None,
        "methods": methods,
        "pairwise_metrics": pairwise,
        # Preserve the original two-method keys for existing consumers.
        "baseline_protocols": methods["all_at_once"]["protocols"],
        "graph_protocols": methods["dag"]["protocols"],
        "paired_metrics": pairwise["all_at_once_vs_dag"]["metrics"],
        "by_benchmark": {
            "baseline": methods["all_at_once"]["by_benchmark"],
            "graph": methods["dag"]["by_benchmark"],
        },
        "by_framework": {
            "baseline": methods["all_at_once"]["by_framework"],
            "graph": methods["dag"]["by_framework"],
        },
        "by_error_mode": {
            "baseline": grouped_metrics(
                ordered["all_at_once"],
                lambda row: (row.get("canonical_ground_truth") or {}).get("mode"),
            ),
            "graph": grouped_metrics(
                ordered["dag"],
                lambda row: (row.get("canonical_ground_truth") or {}).get("mode"),
            ),
        },
        "usage": {
            "baseline": methods["all_at_once"]["usage"],
            "graph": methods["dag"]["usage"],
        },
        "visible_outputs": {
            "baseline": methods["all_at_once"]["visible_outputs"],
            "graph": methods["dag"]["visible_outputs"],
        },
    }
    if blade is not None:
        result["by_benchmark"]["dag_blade"] = methods["dag_blade"]["by_benchmark"]
        result["by_framework"]["dag_blade"] = methods["dag_blade"]["by_framework"]
        result["by_error_mode"]["dag_blade"] = grouped_metrics(
            ordered["dag_blade"],
            lambda row: (row.get("canonical_ground_truth") or {}).get("mode"),
        )
        result["usage"]["dag_blade"] = methods["dag_blade"]["usage"]
        result["visible_outputs"]["dag_blade"] = methods["dag_blade"]["visible_outputs"]
    return result


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path, help="Merged baseline result directory")
    parser.add_argument("--graph", required=True, type=Path, help="Merged graph result directory")
    parser.add_argument("--blade", type=Path, help="Merged DAG+BLADE result directory")
    parser.add_argument("--expected-trace-id-file", type=Path,
                        help="Optional exact trace ID manifest required for full-run comparisons")
    parser.add_argument("--taxonomy-path", type=Path,
                        help="taxonomy.yaml; required to report 17-class macro-F1")
    parser.add_argument("--out", required=True, type=Path, help="Output JSON path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argparser().parse_args(argv)
    baseline = read_rows(args.baseline)
    graph = read_rows(args.graph)
    blade = read_rows(args.blade) if args.blade else None
    taxonomy_codes: tuple[str, ...] = ()
    if args.taxonomy_path:
        taxonomy_codes = load_taxonomy(args.taxonomy_path.parent).codes
    if blade is not None and not taxonomy_codes:
        raise ValueError("--taxonomy-path is required with --blade for canonical macro-F1")
    comparison = build_comparison(
        baseline, graph, blade=blade,
        expected_ids=expected_trace_ids(args.expected_trace_id_file),
        taxonomy_codes=taxonomy_codes,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(comparison, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                         encoding="utf-8")
    print(json.dumps(comparison["pairwise_metrics"], ensure_ascii=False, indent=2))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
