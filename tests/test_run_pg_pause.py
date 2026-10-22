import asyncio
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from whowhen_eval.run_pg import _process_pending
from whowhen_eval.store import ResultsStore


class PauseOnErrorTests(unittest.IsolatedAsyncioTestCase):
    async def test_error_stops_scheduling_and_leaves_failed_trace_retryable(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            args = Namespace(
                out=out,
                concurrency=1,
                pause_on_error=True,
                dry_run=False,
            )
            state = {
                "total_traces": 2,
                "completed_traces": 0,
                "remaining_traces": 2,
                "attempted_this_run": 0,
                "failed_attempts_this_run": 0,
            }
            records = [
                {"trace_id": "trace-1", "benchmark": "bench"},
                {"trace_id": "trace-2", "benchmark": "bench"},
            ]
            store = ResultsStore(out / "results" / "bench.jsonl")
            stores = {"bench": store}
            calls = []

            async def fake_evaluate(**kwargs):
                calls.append(kwargs["record"]["trace_id"])
                return {
                    "trace_id": kwargs["record"]["trace_id"],
                    "benchmark": "bench",
                    "error": "RateLimitError: rate limit reached",
                }

            with patch("whowhen_eval.run_pg.evaluate_one", new=fake_evaluate):
                attempted, succeeded, failed, paused = await _process_pending(
                    records, stores, api_sem=object(), db_sem=object(),
                    repository=object(), taxonomy=object(), api_key="secret-test-key",
                    args=args, state=state,
                )

            self.assertEqual(calls, ["trace-1"])
            self.assertEqual((attempted, succeeded, failed, paused), (1, 0, 1, True))
            self.assertEqual(store.done_trace_ids(), set())
            self.assertEqual(state["status"], "paused")
            self.assertEqual(state["remaining_traces"], 2)

            event = json.loads((out / "pause-events.jsonl").read_text(encoding="utf-8"))
            self.assertEqual(event["trace_id"], "trace-1")
            self.assertIn("RateLimitError", event["error"])
            self.assertNotIn("secret-test-key", json.dumps(event))


if __name__ == "__main__":
    unittest.main()
