import asyncio
import time
import unittest
from pathlib import Path
from threading import Lock
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from whowhen_eval.canonical import canonical_prompt, parse_canonical, score_canonical
from whowhen_eval.pg_source import trace_bundle_from_rows
from whowhen_eval.prompts import Taxonomy
from whowhen_eval.run_pg import evaluate_one, select_records


TAXONOMY = Taxonomy(codes=("A.1", "R.2"), block="- A.1: **Mode A**\n- R.2: **Mode R**")


def _rows():
    return [
        {
            "trace_id": "a" * 32,
            "span_id": "1" * 16,
            "parent_span_id": None,
            "start_time": "2026-01-01T00:00:00Z",
            "name": "agent.interaction",
            "type": "span",
            "level": "DEFAULT",
            "status_message": None,
            "input": None,
            "output": None,
            "metadata": {
                "prompt_text": "Find the drawer key.",
                "failure.mistake_agent": "hidden-label",
                "failure.ground_truth": "hidden-ground-truth",
                "source.raw_json": "SECRET RAW ROW",
            },
        },
        {
            "trace_id": "a" * 32,
            "span_id": "2" * 16,
            "parent_span_id": "1" * 16,
            "start_time": "2026-01-01T00:00:01Z",
            "name": "llm.response",
            "type": "generation",
            "level": "DEFAULT",
            "status_message": None,
            "input": [{"role": "user", "content": "Find a key"}],
            "output": [{"role": "assistant", "content": "Open the drawer."}],
            "metadata": {
                "span.type": "llm",
                "agent.name": "planner",
                "failure.root_span": True,
            },
        },
        {
            "trace_id": "a" * 32,
            "span_id": "3" * 16,
            "parent_span_id": "2" * 16,
            "start_time": "2026-01-01T00:00:02Z",
            "name": "tool.open_drawer",
            "type": "span",
            "level": "ERROR",
            "status_message": "tool failure",
            "input": None,
            "output": None,
            "metadata": {
                "span.type": "tool",
                "gen_ai.tool.name": "open_drawer",
                "gen_ai.tool.call.arguments": '{"drawer":"left"}',
                "gen_ai.tool.call.result": "Error: locked",
            },
        },
    ]


class CanonicalPgProtocolTests(unittest.TestCase):
    def test_bundle_strips_labels_and_raw_source_envelope(self):
        bundle = trace_bundle_from_rows("a" * 32, _rows())
        prompt = canonical_prompt(bundle, TAXONOMY)

        self.assertEqual(bundle.task, "Find the drawer key.")
        self.assertEqual([node.label for node in bundle.nodes], ["S001", "S002"])
        self.assertIn("[S001] parent=ROOT type=llm", prompt)
        self.assertIn("Tool arguments:\n{\"drawer\":\"left\"}", prompt)
        self.assertNotIn("hidden-label", prompt)
        self.assertNotIn("hidden-ground-truth", prompt)
        self.assertNotIn("SECRET RAW ROW", prompt)
        self.assertNotIn("failure.root_span", prompt)

    def test_parse_and_score_target_span(self):
        bundle = trace_bundle_from_rows("a" * 32, _rows())
        parsed = parse_canonical(
            "Actor: planner\nTrace Node: S001\nError Mode: R.2\nReason: Wrong plan.",
            TAXONOMY.codes,
        )
        prediction = {
            "actor": parsed.actor,
            "target_span_id": bundle.node_by_label[parsed.trace_node].span_id,
            "error_mode": parsed.error_mode,
        }
        gold = {"label": {"actor": "planner", "target_span_id": "2" * 16, "mode": "R.2"}}
        self.assertEqual(score_canonical(prediction, gold),
                         {"actor": True, "when": True, "mode": True, "all": True})

    def test_single_agent_is_excluded_from_actor_axis_but_can_be_joint_correct(self):
        prediction = {"actor": "anything", "target_span_id": "2" * 16, "error_mode": "A.1"}
        gold = {"label": {"actor": None, "target_span_id": "2" * 16, "mode": "A.1"}}
        self.assertEqual(score_canonical(prediction, gold),
                         {"actor": None, "when": True, "mode": True, "all": True})

    def test_trace_id_filter_selects_only_requested_canonical_record(self):
        args = SimpleNamespace(
            trace_ids=["target"], trace_id_file=None,
            modality="text", framework=None, benchmark="all",
        )
        records = [
            {"trace_id": "other", "split": "text", "framework": "f", "benchmark": "b"},
            {"trace_id": "target", "split": "text", "framework": "f", "benchmark": "b"},
        ]
        self.assertEqual(select_records(records, args), {"b": [records[1]]})

    def test_trace_id_file_selects_requested_record(self):
        with TemporaryDirectory() as directory:
            selection = Path(directory) / "trace-ids.txt"
            selection.write_text("# stage sample\ntarget\n", encoding="utf-8")
            args = SimpleNamespace(
                trace_ids=None, trace_id_file=selection,
                modality="text", framework=None, benchmark="all",
            )
            records = [
                {"trace_id": "other", "split": "text", "framework": "f", "benchmark": "b"},
                {"trace_id": "target", "split": "text", "framework": "f", "benchmark": "b"},
            ]
            self.assertEqual(select_records(records, args), {"b": [records[1]]})


class CanonicalPgTimingTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_duration_excludes_api_semaphore_queue_wait(self):
        class Repository:
            def load_bundle(self, trace_id):
                return trace_bundle_from_rows(trace_id, _rows())

        first_call_started = asyncio.Event()
        loop = asyncio.get_running_loop()
        calls = 0
        calls_lock = Lock()

        def fake_generate(*args, **kwargs):
            nonlocal calls
            with calls_lock:
                calls += 1
                call_number = calls
            if call_number == 1:
                loop.call_soon_threadsafe(first_call_started.set)
                time.sleep(0.4)
            else:
                time.sleep(0.02)
            return (
                "Actor: planner\nTrace Node: S001\nError Mode: R.2\nReason: test",
                {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            )

        args = SimpleNamespace(
            method="canonical", model="test/model", prompt_max_chars=None,
            dry_run=False, temperature=0.0, max_tokens=1000,
            reasoning_effort=None, api_base=None,
        )
        record = {
            "trace_id": "a" * 32,
            "framework": "test",
            "benchmark": "test",
            "split": "text",
            "label": {"actor": "planner", "target_span_id": "2" * 16, "mode": "R.2"},
        }
        api_sem = asyncio.Semaphore(1)
        db_sem = asyncio.Semaphore(1)
        with patch("whowhen_eval.run_pg.generate", side_effect=fake_generate):
            first = asyncio.create_task(evaluate_one(
                api_sem=api_sem, db_sem=db_sem, repository=Repository(),
                record=record, taxonomy=TAXONOMY, api_key=None, args=args,
            ))
            await asyncio.wait_for(first_call_started.wait(), timeout=1)
            second = asyncio.create_task(evaluate_one(
                api_sem=api_sem, db_sem=db_sem, repository=Repository(),
                record=record, taxonomy=TAXONOMY, api_key=None, args=args,
            ))
            first_result, second_result = await asyncio.gather(first, second)

        self.assertIsNone(first_result["error"])
        self.assertIsNone(second_result["error"])
        self.assertLess(second_result["duration_s"], 0.2)
