import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, uuid5

from whowhen_eval.dag_pg import (
    DAGInputError,
    build_dag_bundle,
    build_edges_messages,
    parse_json_object,
    run_dag_backtrace,
    score_dag_prediction,
    validate_edges,
)
from whowhen_eval.dag_pg_source import PostgresDAGRepository
from whowhen_eval.run_dag_pg import evaluate_one


TRACE_ID = "a" * 32


def _sid(local_name):
    return uuid5(UUID(TRACE_ID), local_name).hex[:16]


def _raw_record():
    return {
        "id": "sample-case",
        "framework": "debate",
        "task": json.dumps({"query": "Find the arithmetic error."}),
        "ground_truth": json.dumps({
            "agent": "SECRET_LABEL",
            "step": 1,
            "mode": "SECRET_MODE",
        }),
        "trajectory": json.dumps([
            {"kind": "user", "content": "Find the arithmetic error."},
            {
                "kind": "round",
                "round": 0,
                "is_live": True,
                "is_injected": True,
                "is_replayed": False,
                "turns": [
                    {"agent_id": "worker-1", "output": "2 + 2 = 5", "is_injected": True},
                    {"agent_id": "worker-2", "output": "I will check that.", "is_injected": False},
                ],
            },
            {
                "kind": "action",
                "step_number": 1,
                "action": "submit",
                "observation": "done",
                "error": None,
            },
            {"kind": "planning", "plan": "Review the work.", "phase": "injected"},
        ]),
    }


def _bundle():
    # Two round turns -> llm-0001/2, the action -> tool-0001, planning -> llm-0003.
    span_ids = {_sid("llm-0001"), _sid("llm-0002"), _sid("tool-0001"), _sid("llm-0003")}
    return build_dag_bundle(TRACE_ID, json.dumps(_raw_record()), span_ids)


class DAGPgAdapterTests(unittest.TestCase):
    def test_labels_and_injection_markers_are_removed_and_rounds_are_expanded(self):
        bundle = _bundle()
        self.assertEqual(bundle.question, "Find the arithmetic error.")
        self.assertEqual(len(bundle.history), 5)
        self.assertEqual(bundle.nodes[1]["agent"], "worker-1")
        self.assertEqual(bundle.source_coordinates[1], {
            "trajectory_index": 1, "turn_index": 0, "kind": "round",
        })
        self.assertIn(_sid("llm-0001"), bundle.step_span_ids[1])
        self.assertIn(_sid("llm-0002"), bundle.step_span_ids[2])
        self.assertIn(_sid("tool-0001"), bundle.step_span_ids[3])

        prompt = json.dumps(build_edges_messages(bundle), ensure_ascii=False)
        self.assertNotIn("SECRET_LABEL", prompt)
        self.assertNotIn("SECRET_MODE", prompt)
        self.assertNotIn("is_injected", prompt)
        self.assertNotIn("is_replayed", prompt)
        self.assertNotIn("injection_via", prompt)
        self.assertNotIn('"phase"', prompt)
        self.assertNotIn('"ground_truth"', prompt)

    def test_crosswalk_must_cover_exactly_the_pg_child_spans(self):
        with self.assertRaisesRegex(DAGInputError, "crosswalk mismatch"):
            build_dag_bundle(TRACE_ID, json.dumps(_raw_record()), {_sid("llm-0001")})

    def test_edges_and_json_responses_are_strictly_validated(self):
        self.assertEqual(
            validate_edges([
                {"source": 0, "target": 1, "reason": "uses prior result"},
                {"source": 0, "target": 1, "reason": "duplicate"},
            ], 2),
            [{"source": 0, "target": 1, "reason": "uses prior result"}],
        )
        with self.assertRaises(ValueError):
            validate_edges([{"source": 1, "target": 0, "reason": "backwards"}], 2)
        with self.assertRaises(ValueError):
            validate_edges([{"source": True, "target": 1, "reason": "bool is not an ID"}], 2)
        self.assertEqual(parse_json_object("```json\n{\"edges\": []}\n```"), {"edges": []})

    def test_pg_query_does_not_select_failure_label_attributes(self):
        self.assertIn("source.raw_json", PostgresDAGRepository._TRACE_SQL)
        self.assertNotIn("failure.", PostgresDAGRepository._TRACE_SQL)
        self.assertNotIn("ground_truth", PostgresDAGRepository._TRACE_SQL)


class DAGPgExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_backtrace_scores_who_when_only_and_never_sends_labels(self):
        bundle = _bundle()
        seen_operations = []

        async def fake_request(messages, operation):
            seen_operations.append(operation)
            serialized = json.dumps(messages, ensure_ascii=False)
            self.assertNotIn("SECRET_LABEL", serialized)
            self.assertNotIn("SECRET_MODE", serialized)
            self.assertNotIn("ground_truth", serialized)
            if operation == "edges":
                return {"edges": [{"source": 1, "target": 3, "reason": "the action uses the answer"}]}
            if operation == "start":
                return {"start_step": 3, "failure": "the submitted result is wrong"}
            if operation == "step_3":
                return {"step_id": 3, "local_error": False, "upstream_steps": [1], "reason": "propagated"}
            if operation == "step_1":
                return {"step_id": 1, "local_error": True, "upstream_steps": [], "reason": "introduced the arithmetic error"}
            self.fail(f"unexpected model operation: {operation}")

        result = await run_dag_backtrace(bundle, fake_request)
        self.assertEqual(seen_operations, ["edges", "start", "step_3", "step_1"])
        self.assertEqual(result["prediction"]["step_id"], 1)
        self.assertEqual(result["prediction"]["source_coordinate"]["turn_index"], 0)

        scored = score_dag_prediction(
            result["prediction"],
            {"label": {
                "actor": "worker-1",
                "mode": "SECRET_MODE",
                "target_span_id": _sid("llm-0001"),
            }},
            bundle,
        )
        self.assertEqual(scored, {"actor": True, "when": True, "mode": None, "all": None})

    async def test_runner_preserves_system_and_user_roles_and_aggregates_usage(self):
        bundle = _bundle()

        class FakeRepository:
            def load_bundle(self, trace_id):
                self_trace = trace_id
                if self_trace != TRACE_ID:
                    raise AssertionError("unexpected trace ID")
                return bundle

        responses = iter([
            {"edges": [{"source": 1, "target": 3, "reason": "dependency"}]},
            {"start_step": 3, "failure": "wrong final action"},
            {"step_id": 3, "local_error": False, "upstream_steps": [1], "reason": "propagated"},
            {"step_id": 1, "local_error": True, "upstream_steps": [], "reason": "wrong calculation"},
        ])
        call_count = 0

        def fake_generate(model, messages, **kwargs):
            nonlocal call_count
            call_count += 1
            self.assertEqual(messages[0]["role"], "system")
            self.assertEqual(messages[1]["role"], "user")
            serialized = json.dumps(messages, ensure_ascii=False)
            self.assertNotIn("SECRET_LABEL", serialized)
            self.assertNotIn("SECRET_MODE", serialized)
            self.assertNotIn("ground_truth", serialized)
            return json.dumps(next(responses)), {
                "input_tokens": 10,
                "output_tokens": 2,
                "total_tokens": 12,
            }

        args = SimpleNamespace(
            model="test/model", temperature=0.0, max_tokens=1000,
            reasoning_effort=None, api_base=None, dry_run=False,
        )
        record = {
            "trace_id": TRACE_ID,
            "raw_id": "sample-case",
            "framework": "debate",
            "benchmark": "math",
            "split": "text",
            "label": {
                "actor": "worker-1",
                "mode": "SECRET_MODE",
                "target_span_id": _sid("llm-0001"),
            },
        }
        with patch("whowhen_eval.run_dag_pg.generate", side_effect=fake_generate):
            result = await evaluate_one(
                api_sem=asyncio.Semaphore(1),
                db_sem=asyncio.Semaphore(1),
                repository=FakeRepository(),
                record=record,
                api_key=None,
                args=args,
            )
        self.assertEqual(call_count, 4)
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["score"]["when"], True)
        self.assertIsNone(result["score"]["mode"])
        self.assertEqual(result["api_calls"], 4)
        self.assertEqual(result["usage"]["total_tokens"], 48)
