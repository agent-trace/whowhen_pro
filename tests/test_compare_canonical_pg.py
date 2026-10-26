import unittest

from whowhen_eval.canonical import score_canonical
from whowhen_eval.compare_canonical_pg import build_comparison
from whowhen_eval.dag_blade import METHOD as DAG_BLADE_METHOD


TRACE_ID = "a" * 32
GOLD = {"actor": "worker", "target_span_id": "span-1", "mode": "R.2"}


def row(method, protocol, prediction):
    return {
        "trace_id": TRACE_ID,
        "method": method,
        "protocol": protocol,
        "benchmark": "example",
        "framework": "example-framework",
        "canonical_ground_truth": GOLD,
        "prediction": prediction,
        "score": score_canonical(prediction, {"label": GOLD}),
        "usage": {"input_tokens": 10, "output_tokens": 4, "total_tokens": 14},
        "raw_output": "{}",
    }


class CanonicalMethodComparisonTests(unittest.TestCase):
    def test_three_methods_share_canonical_axes_and_macro_f1(self):
        baseline = {TRACE_ID: row("canonical", "canonical_db_all_at_once_v1", {
            "actor": "worker", "target_span_id": "span-1", "error_mode": "R.2",
        })}
        graph = {TRACE_ID: row("graph", "canonical_db_graph_all_at_once_v1", {
            "actor": "wrong", "target_span_id": "span-1", "error_mode": "R.2",
        })}
        blade = {TRACE_ID: row(DAG_BLADE_METHOD, "canonical_db_dag_blade_v2", {
            "actor": "worker", "target_span_id": "span-1", "error_mode": "A.1",
        })}

        result = build_comparison(
            baseline, graph, blade=blade,
            expected_ids={TRACE_ID}, taxonomy_codes=("R.2", "A.1"),
        )

        self.assertEqual(result["records"], 1)
        self.assertTrue(result["expected_trace_ids_verified"])
        self.assertEqual(result["methods"]["dag_blade"]["metrics"]["all"], {
            "correct": 0, "denominator": 1, "accuracy": 0.0,
        })
        self.assertEqual(
            result["pairwise_metrics"]["dag_vs_dag_blade"]["metrics"]["mode"]["delta_accuracy"],
            -1.0,
        )
        self.assertEqual(
            result["pairwise_metrics"]["dag_vs_dag_blade"]["mode_macro_f1_delta"],
            -0.5,
        )

    def test_comparison_rejects_missing_ids_and_different_gold(self):
        baseline = {TRACE_ID: row("canonical", "canonical", {
            "actor": "worker", "target_span_id": "span-1", "error_mode": "R.2",
        })}
        graph = {}
        with self.assertRaisesRegex(ValueError, "paired trace IDs differ"):
            build_comparison(baseline, graph)

        graph = {TRACE_ID: row("graph", "graph", {
            "actor": "worker", "target_span_id": "span-1", "error_mode": "R.2",
        })}
        graph[TRACE_ID]["canonical_ground_truth"] = {**GOLD, "mode": "A.1"}
        with self.assertRaisesRegex(ValueError, "labels differ"):
            build_comparison(baseline, graph)


if __name__ == "__main__":
    unittest.main()
