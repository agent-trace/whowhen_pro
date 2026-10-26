import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from whowhen_eval.dag_blade import (
    AUDIT_LABEL_GROUPS,
    build_audit_messages,
    build_diagnosis_messages,
    build_relations_messages,
    run_dag_blade,
    select_handoffs,
    validate_audit,
    validate_diagnosis,
    validate_relations,
)
from whowhen_eval.dag_pg import DAGBundle
from whowhen_eval.prompts import Taxonomy
from whowhen_eval.run_dag_blade_pg import _fatal_api_error, _litellm_model_id, _safe_error, evaluate_one


TRACE_ID = "a" * 32
TARGET_SPAN = "target-span"
TAXONOMY = Taxonomy(codes=("R.2", "A.1"), block="- R.2: Reasoning error\n- A.1: Tool error")


def bundle():
    history = (
        {"kind": "user", "content": "Solve the task."},
        {"kind": "agent", "agent": "worker-a", "content": "Evidence says 2+2=4."},
        {"kind": "agent", "agent": "worker-b", "content": "I read it as 2+2=5."},
    )
    nodes = (
        {"step_id": 0, "agent": "user"},
        {"step_id": 1, "agent": "worker-a"},
        {"step_id": 2, "agent": "worker-b"},
    )
    coordinates = tuple(
        {"trajectory_index": idx, "turn_index": None, "kind": row["kind"]}
        for idx, row in enumerate(history)
    )
    spans = ((), ("source-span",), (TARGET_SPAN,))
    return DAGBundle(
        trace_id=TRACE_ID,
        question="Solve the task.",
        history=history,
        nodes=nodes,
        source_coordinates=coordinates,
        step_span_ids=spans,
        pg_span_ids=frozenset({"source-span", TARGET_SPAN}),
    )


def relation_response():
    rows = []
    for target in (1, 2):
        for relation in ("depends_on", "repeats", "contradicts", "repairs"):
            sources = []
            if relation == "depends_on":
                ids = [0] if target == 1 else [0, 1]
                sources = [
                    {"source": source, "confidence": 0.85 if source == 1 else 0.8,
                     "evidence": "uses prior content"}
                    for source in ids
                ]
            rows.append({"target": target, "relation": relation, "sources": sources})
    return {"relations": rows}


class DAGBladeValidationTests(unittest.TestCase):
    def test_relations_prompt_requires_empty_sources_array(self):
        messages = build_relations_messages(bundle())
        self.assertIn("empty sources array", messages[0]["content"])
        self.assertIn("one judgment for each relation", messages[0]["content"])
        self.assertNotIn("source=null", messages[0]["content"])

    def test_relation_ids_types_and_handoff_top_k_are_checked(self):
        checked = validate_relations(relation_response()["relations"], 3)
        selected = select_handoffs(checked, min_confidence=0.4, max_sources=2)
        self.assertEqual([(h["source"], h["target"]) for h in selected], [(0, 1), (1, 2), (0, 2)])
        invalid = relation_response()["relations"]
        invalid[0] = {"target": 1, "relation": "depends_on", "sources": [
            {"source": 1, "confidence": 0.9, "evidence": "future"}
        ]}
        with self.assertRaisesRegex(ValueError, "earlier"):
            validate_relations(invalid, 3)

    def test_audit_labels_keep_breach_and_propagation_semantics(self):
        handoffs = [{"source": 0, "target": 2}]
        kinds, findings = validate_audit({
            "event_kinds": [
                {"step_id": 0, "kind": "evidence"},
                {"step_id": 1, "kind": "decision"},
                {"step_id": 2, "kind": "decision"},
            ],
            "handoffs": [{
                "source": 0, "target": 2, "label": "MISINTERPRETATION",
                "confidence": 0.9, "evidence": "meaning changed",
            }],
        }, 3, handoffs)
        self.assertEqual(len(kinds), 3)
        self.assertEqual(findings[0]["group"], "breach")
        self.assertEqual(AUDIT_LABEL_GROUPS["PROPAGATION"], "inherited_or_repaired")
        with self.assertRaisesRegex(ValueError, "unselected"):
            validate_audit({
                "event_kinds": [{"step_id": i, "kind": "decision"} for i in range(3)],
                "handoffs": [{"source": 1, "target": 2, "label": "MISINTERPRETATION",
                              "confidence": 0.8, "evidence": "x"}],
            }, 3, handoffs)

    def test_pipeline_keeps_full_trajectory_candidate_set_and_does_not_send_labels(self):
        trace = bundle()
        operations = []

        async def fake_request(messages, operation):
            operations.append(operation)
            serialized = json.dumps(messages, ensure_ascii=False)
            self.assertNotIn("SECRET_LABEL", serialized)
            self.assertNotIn("ground_truth", serialized)
            if operation == "requirements":
                return {"requirements": ["Solve the task."]}
            if operation == "relations":
                return relation_response()
            if operation == "audit":
                return {
                    "event_kinds": [
                        {"step_id": i, "kind": "decision" if i else "evidence"}
                        for i in range(3)
                    ],
                    "handoffs": [
                        {"source": 0, "target": 1, "label": "FAITHFUL_USE", "confidence": 0.9,
                         "evidence": "faithful"},
                        {"source": 1, "target": 2, "label": "MISINTERPRETATION", "confidence": 0.9,
                         "evidence": "misread"},
                        {"source": 0, "target": 2, "label": "PROPAGATION", "confidence": 0.7,
                         "evidence": "carried"},
                    ],
                }
            if operation == "diagnosis":
                # Step 2 has a finding in the graph, but the global stage may select any event.
                self.assertIn("Every event is eligible", messages[0]["content"])
                self.assertEqual(len(json.loads(messages[1]["content"])["events"]), 3)
                self.assertIn("error mode from this taxonomy", messages[0]["content"])
                events = json.loads(messages[1]["content"])["events"]
                self.assertEqual(events[1]["pg_span_ids"], ["source-span"])
                return {
                    "step_id": 1, "target_span_id": "source-span",
                    "error_mode": "R.2", "reason": "decisive interpretation error",
                }
            self.fail(f"unexpected operation {operation}")

        result = asyncio.run(run_dag_blade(trace, fake_request, taxonomy=TAXONOMY))
        self.assertEqual(operations, ["requirements", "relations", "audit", "diagnosis"])
        self.assertEqual(result["prediction"]["step_id"], 1)
        self.assertEqual(result["prediction"]["target_span_id"], "source-span")
        self.assertEqual(result["prediction"]["error_mode"], "R.2")
        self.assertEqual(result["prediction"]["represented_pg_span_ids"], ["source-span"])
        self.assertTrue(result["audit_findings"])
        self.assertEqual(result["blame_graph"]["confidence_kind"], "self_reported_confidence")

    def test_diagnosis_rejects_a_span_from_another_step_or_unknown_mode(self):
        with self.assertRaisesRegex(ValueError, "belong to the selected step"):
            validate_diagnosis({
                "step_id": 1, "target_span_id": TARGET_SPAN,
                "error_mode": "R.2", "reason": "reason",
            }, bundle(), TAXONOMY)
        with self.assertRaisesRegex(ValueError, "canonical taxonomy codes"):
            validate_diagnosis({
                "step_id": 2, "target_span_id": TARGET_SPAN,
                "error_mode": "UNKNOWN", "reason": "reason",
            }, bundle(), TAXONOMY)

    def test_api_errors_redact_credentials_and_trigger_pause(self):
        secret = "example-secret-token"
        error = _safe_error(RuntimeError(f"bad key {secret}"), secret)
        self.assertNotIn(secret, error)
        self.assertIn("[REDACTED]", error)
        self.assertTrue(_fatal_api_error({"error": "RateLimitError: 429 quota exceeded"}))
        self.assertFalse(_fatal_api_error({"error": "ValueError: invalid JSON"}))


class DAGBladeRunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_unprefixed_model_uses_openai_provider_for_custom_base(self):
        self.assertEqual(
            _litellm_model_id("DeepSeek-V4.1-Flash", "http://localhost:8080/v1"),
            "openai/DeepSeek-V4.1-Flash",
        )
        self.assertEqual(
            _litellm_model_id("vendor/model", "http://localhost:8080/v1"),
            "vendor/model",
        )
        self.assertEqual(_litellm_model_id("gpt-4.1", None), "gpt-4.1")

    async def test_runner_scores_who_when_and_records_four_stage_usage(self):
        trace = bundle()
        responses = iter([
            {"requirements": ["Solve the task."]},
            relation_response(),
            {
                "event_kinds": [
                    {"step_id": i, "kind": "decision" if i else "evidence"}
                    for i in range(3)
                ],
                "handoffs": [
                    {"source": 0, "target": 1, "label": "FAITHFUL_USE", "confidence": 0.9, "evidence": "ok"},
                    {"source": 1, "target": 2, "label": "MISINTERPRETATION", "confidence": 0.9, "evidence": "wrong"},
                    {"source": 0, "target": 2, "label": "PROPAGATION", "confidence": 0.7, "evidence": "passed on"},
                ],
            },
            {"step_id": 2, "target_span_id": TARGET_SPAN,
             "error_mode": "R.2", "reason": "misinterpretation caused failure"},
        ])
        class FakeRepository:
            def load_bundle(self, trace_id):
                self_trace = trace_id
                if self_trace != TRACE_ID:
                    raise AssertionError("wrong trace")
                return trace
        def fake_generate(model, messages, **kwargs):
            self.assertNotIn("SECRET_LABEL", json.dumps(messages, ensure_ascii=False))
            self.assertNotIn("ground_truth", json.dumps(messages, ensure_ascii=False))
            return json.dumps(next(responses)), {
                "input_tokens": 10, "output_tokens": 4, "total_tokens": 14,
                "reasoning_tokens": 2, "finish_reason": "stop",
            }
        args = SimpleNamespace(
            model="test/model", temperature=0.0, max_tokens=None,
            reasoning_effort="low", api_base=None, dry_run=False,
            min_confidence=0.4, max_sources=2, num_retries=0,
        )
        record = {
            "trace_id": TRACE_ID, "raw_id": "test", "framework": "debate",
            "benchmark": "dylan", "split": "text",
            "label": {"actor": "worker-b", "target_span_id": TARGET_SPAN, "mode": "R.2"},
        }
        with patch("whowhen_eval.run_dag_blade_pg.generate", side_effect=fake_generate):
            result = await evaluate_one(
                api_sem=asyncio.Semaphore(1), db_sem=asyncio.Semaphore(1),
                repository=FakeRepository(), record=record, api_key="not-logged", args=args,
                taxonomy=TAXONOMY,
            )
        self.assertEqual(result["status"], "success")
        self.assertEqual(result["score"], {"actor": True, "when": True, "mode": True, "all": True})
        self.assertEqual(result["api_calls"], 4)
        self.assertEqual(result["usage"]["total_tokens"], 56)
        self.assertEqual(result["calls"][0]["usage"]["finish_reason"], "stop")


if __name__ == "__main__":
    unittest.main()
