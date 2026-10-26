"""Run the isolated DeepSeek BLADE-inspired pilot against PG-backed traces."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .dag_blade import METHOD, PROTOCOL, run_dag_blade, score_dag_blade_prediction
from .dag_pg import build_dag_bundle, parse_json_object
from .dag_pg_source import PostgresDAGRepository
from .llm import generate
from .prompts import Taxonomy, load_taxonomy
from .run_pg import load_snapshot, select_records
from .store import ResultsStore

_NO_USAGE = {"input_tokens": None, "output_tokens": None, "total_tokens": None, "reasoning_tokens": None}
_STOP_MARKERS = (
    "429", "rate limit", "rate_limit", "insufficient_quota", "quota exceeded",
    "unauthorized", "authentication", "invalid api key", "status code 401", "status code 403",
)


def _message_text(messages: list[dict[str, str]]) -> str:
    return "\n".join(message.get("content", "") for message in messages)


def _safe_error(exc: Exception, api_key: Optional[str]) -> str:
    message = str(exc)
    if api_key:
        message = message.replace(api_key, "[REDACTED]")
    message = re.sub(r"(?i)bearer\s+[^\s,;]+", "Bearer [REDACTED]", message)
    return f"{type(exc).__name__}: {message[:1000]}"


def _fatal_api_error(result: dict[str, Any]) -> bool:
    text = " ".join(
        [str(result.get("error") or "")]
        + [str(call.get("error") or "") for call in result.get("calls", [])]
    ).casefold()
    return any(marker in text for marker in _STOP_MARKERS)


def _sum_usage(calls: list[dict[str, Any]]) -> dict[str, Optional[int]]:
    result: dict[str, Optional[int]] = {}
    for key in ("input_tokens", "output_tokens", "total_tokens", "reasoning_tokens"):
        values = [call.get("usage", {}).get(key) for call in calls]
        known = [int(value) for value in values if isinstance(value, int)]
        result[key] = sum(known) if known else None
    return result


def _write_run_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _merge_attempt_rows(stores: dict[str, ResultsStore], out: Path) -> Path:
    """Write one effective row per trace, preferring a successful retry."""
    merged_dir = out / "merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    for benchmark, store in stores.items():
        effective: dict[str, dict[str, Any]] = {}
        if store.path.exists():
            with store.path.open(encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    trace_id = row.get("trace_id")
                    if not isinstance(trace_id, str):
                        continue
                    previous = effective.get(trace_id)
                    if previous is None or row.get("status") == "success" or previous.get("status") != "success":
                        effective[trace_id] = row
        destination = merged_dir / f"{benchmark}.jsonl"
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            for trace_id in sorted(effective):
                handle.write(json.dumps(effective[trace_id], ensure_ascii=False, sort_keys=True) + "\n")
        os.replace(temporary, destination)
    return merged_dir


def _litellm_model_id(model: str, api_base: Optional[str]) -> str:
    """Route unprefixed names through LiteLLM's OpenAI-compatible provider."""
    if api_base and "/" not in model:
        return f"openai/{model}"
    return model

def _prompt_info(messages: list[dict[str, str]]) -> dict[str, Any]:
    text = _message_text(messages)
    return {"chars": len(text), "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest()}


async def evaluate_one(
    *, api_sem: asyncio.Semaphore, db_sem: asyncio.Semaphore,
    repository: PostgresDAGRepository, record: dict[str, Any],
    api_key: Optional[str], args: argparse.Namespace, taxonomy: Taxonomy,
    pause_event: Optional[asyncio.Event] = None,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "protocol": PROTOCOL, "method": METHOD, "model": args.model,
        "trace_id": record["trace_id"], "raw_id": record.get("raw_id"),
        "framework": record.get("framework"), "benchmark": record.get("benchmark"),
        "modality": record.get("split"),
        "trace_path": f"postgresql/events_dedup/{record['trace_id']}",
        "canonical_ground_truth": record.get("label"),
        "error_mode": (record.get("label") or {}).get("mode"),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "confidence_kind": "self_reported_confidence",
    }
    started_total = time.monotonic()
    try:
        async with db_sem:
            bundle = await asyncio.to_thread(repository.load_bundle, record["trace_id"])
        label = record.get("label") or {}
        target_span_id = label.get("target_span_id")
        if target_span_id not in bundle.pg_span_ids:
            raise ValueError("canonical target span is absent from the PostgreSQL trace")
        target_steps = [
            step for step, span_ids in enumerate(bundle.step_span_ids)
            if target_span_id in span_ids
        ]
        if len(target_steps) != 1:
            raise ValueError(f"canonical target must map to exactly one DAG step; mapped_to={len(target_steps)}")
        if args.dry_run:
            return {
                **base, "status": "dry_run", "raw_output": None,
                "prediction": None, "score": None,
                "usage": dict(_NO_USAGE), "api_calls": 0, "calls": [],
                "duration_s": 0.0,
                "dry_run": {"step_count": len(bundle.nodes), "pg_span_count": len(bundle.pg_span_ids)},
                "error": None,
            }
    except Exception as exc:  # noqa: BLE001
        return {
            **base, "status": "failed", "raw_output": None,
            "prediction": None, "score": None,
            "usage": dict(_NO_USAGE), "api_calls": 0, "calls": [],
            "duration_s": round(time.monotonic() - started_total, 3),
            "error": f"pg_bundle: {_safe_error(exc, api_key)}",
        }

    calls: list[dict[str, Any]] = []

    async def request_json(messages: list[dict[str, str]], operation: str) -> dict[str, Any]:
        prompt_info = _prompt_info(messages)
        call: dict[str, Any] = {"operation": operation, "prompt": prompt_info, "usage": dict(_NO_USAGE)}
        request_started: Optional[float] = None
        raw: Optional[str] = None
        try:
            if pause_event is not None and pause_event.is_set():
                raise RuntimeError("Paused because another request hit an API quota exceeded or authentication error")
            async with api_sem:
                if pause_event is not None and pause_event.is_set():
                    raise RuntimeError("Paused because another request hit an API quota exceeded or authentication error")
                request_started = time.monotonic()
                raw, usage = await asyncio.to_thread(
                    generate, _litellm_model_id(args.model, args.api_base), messages,
                    temperature=args.temperature,
                    max_tokens=args.max_tokens,
                    reasoning_effort=args.reasoning_effort,
                    api_base=args.api_base,
                    api_key=api_key,
                    num_retries=args.num_retries,
                )
            call["duration_s"] = round(time.monotonic() - request_started, 3)
            call["usage"] = usage
            call["response_chars"] = len(raw or "")
            call["raw_output"] = raw
            parsed = parse_json_object(raw)
            call["status"] = "success"
            calls.append(call)
            return parsed
        except Exception as exc:  # noqa: BLE001
            call["duration_s"] = round(time.monotonic() - request_started, 3) if request_started else 0.0
            call["response_chars"] = len(raw or "") if raw is not None else None
            call["raw_output"] = raw
            call["status"] = "failed"
            call["error"] = _safe_error(exc, api_key)
            calls.append(call)
            if pause_event is not None and _fatal_api_error({"calls": [call]}):
                pause_event.set()
            raise

    try:
        result = await run_dag_blade(
            bundle, request_json,
            taxonomy=taxonomy,
            min_confidence=args.min_confidence,
            max_sources=args.max_sources,
        )
        prediction = result["prediction"]
        score = score_dag_blade_prediction(prediction, record, bundle)
        diagnosis_call = next(call for call in reversed(calls) if call["operation"] == "diagnosis")
        return {
            **base, **result, "status": "success", "score": score,
            "raw_output": diagnosis_call.get("raw_output"),
            "usage": _sum_usage(calls), "api_calls": len(calls), "calls": calls,
            "duration_s": round(time.monotonic() - started_total, 3), "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            **base, "status": "failed",
            "raw_output": next(
                (call.get("raw_output") for call in reversed(calls)
                 if call["operation"] == "diagnosis"), None,
            ),
            "prediction": None,
            "score": score_dag_blade_prediction(None, record, bundle),
            "usage": _sum_usage(calls), "api_calls": len(calls), "calls": calls,
            "duration_s": round(time.monotonic() - started_total, 3),
            "error": _safe_error(exc, api_key),
        }


async def run(args: argparse.Namespace) -> int:
    if args.concurrency < 1 or args.db_concurrency < 1:
        raise ValueError("concurrency values must be positive integers")
    by_benchmark = select_records(load_snapshot(args.canonical_snapshot), args)
    if not by_benchmark:
        print("No canonical snapshot records matched the selected filters", file=sys.stderr)
        return 1
    taxonomy = load_taxonomy(args.taxonomy_path.parent)
    repository = PostgresDAGRepository(args.pg_dsn)
    api_key = os.environ.get(args.api_key_env) if args.api_key_env else None
    if args.api_key_env and not api_key and not args.dry_run:
        raise RuntimeError(f"required API key environment variable is not set: {args.api_key_env}")
    api_sem = asyncio.Semaphore(args.concurrency)
    db_sem = asyncio.Semaphore(min(args.db_concurrency, args.concurrency))
    stores = {
        benchmark: ResultsStore.for_cell(args.out / METHOD, args.model, benchmark)
        for benchmark in by_benchmark
    }
    pending_by_benchmark: dict[str, list[dict[str, Any]]] = {}
    skipped = 0
    for benchmark, rows in by_benchmark.items():
        store = stores[benchmark]
        done: set[str] = set()
        if args.resume and store.path.exists():
            for line in store.path.read_text(encoding="utf-8").splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("status") == "success" and isinstance(row.get("trace_id"), str):
                    done.add(row["trace_id"])
        pending = [row for row in rows if row["trace_id"] not in done]
        skipped += len(rows) - len(pending)
        if args.max_traces is not None:
            pending = pending[:args.max_traces]
        pending_by_benchmark[benchmark] = pending
        print(f"[{benchmark}] {len(pending)} trace(s) -> {store.path}", file=sys.stderr)

    pending_records = [row for rows in pending_by_benchmark.values() for row in rows]
    state_path = args.out / METHOD / "models" / (args.model.replace("/", "_") + ".run-state.json")
    selected_total = sum(len(rows) for rows in by_benchmark.values())
    state: dict[str, Any] = {
        "status": "running",
        "method": METHOD,
        "protocol": PROTOCOL,
        "model": args.model,
        "modality": args.modality,
        "total_selected": selected_total,
        "scheduled_this_run": len(pending_records),
        "completed_this_run": 0,
        "skipped_prior_success": skipped,
        "remaining_traces": len(pending_records),
        "failed_this_run": 0,
        "last_trace_id": None,
        "last_error": None,
        "results_files": {key: str(value.path) for key, value in stores.items()},
    }
    _write_run_state(state_path, state)

    pending_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for record in pending_records:
        pending_queue.put_nowait(record)
    pause_event = asyncio.Event()
    completed = 0
    failed = 0
    worker_count = min(args.concurrency, len(pending_records))

    async def worker() -> None:
        nonlocal completed, failed
        while not pause_event.is_set():
            try:
                record = pending_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            result = await evaluate_one(
                api_sem=api_sem, db_sem=db_sem, repository=repository,
                record=record, api_key=api_key, args=args, taxonomy=taxonomy,
                pause_event=pause_event,
            )
            if not args.dry_run:
                stores[result["benchmark"]].append(result)
            completed += 1
            if result.get("status") == "failed":
                failed += 1
            if _fatal_api_error(result):
                pause_event.set()
                state["last_error"] = result.get("error") or next(
                    (call.get("error") for call in result.get("calls", []) if call.get("error")),
                    "API quota/authentication error",
                )
            state.update({
                "status": "paused" if pause_event.is_set() else "running",
                "completed_this_run": completed,
                "remaining_traces": len(pending_records) - completed,
                "failed_this_run": failed,
                "last_trace_id": record["trace_id"],
                "results_files": {key: str(value.path) for key, value in stores.items()},
            })
            _write_run_state(state_path, state)
            print(
                f"[global] {completed}/{len(pending_records)} failed={failed} "
                f"status={result.get('status')} calls={result.get('api_calls', 0)} "
                f"trace={record['trace_id']}",
                file=sys.stderr,
            )
            pending_queue.task_done()

    if worker_count:
        await asyncio.gather(*(worker() for _ in range(worker_count)))

    model_results_dir = next(iter(stores.values())).path.parent
    merged_dir = None if args.dry_run else _merge_attempt_rows(stores, model_results_dir)
    remaining = pending_queue.qsize()
    if pause_event.is_set():
        final_status = "paused"
    elif remaining:
        final_status = "partial"
    elif failed:
        final_status = "complete_with_trace_failures"
    else:
        final_status = "complete"
    state.update({
        "status": final_status,
        "completed_this_run": completed,
        "remaining_traces": remaining,
        "failed_this_run": failed,
        "merged_results_dir": str(merged_dir) if merged_dir else None,
    })
    _write_run_state(state_path, state)
    print(
        f"DAG+BLADE: done={completed}, skipped={skipped}, failed={failed}, "
        f"remaining={remaining}, status={final_status}",
        file=sys.stderr,
    )
    return 3 if final_status == "paused" else (0 if final_status == "complete" else 2)

def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BLADE-inspired DeepSeek pilot on PG-backed trajectories")
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument("--canonical-snapshot", required=True, type=Path)
    parser.add_argument("--taxonomy-path", required=True, type=Path,
                        help="taxonomy.yaml shared with canonical/all-at-once and DAG scoring")
    parser.add_argument("--pg-dsn", default=None)
    parser.add_argument("--benchmark", default="all")
    parser.add_argument("--modality", default="text")
    parser.add_argument("--framework", default=None)
    parser.add_argument("--trace-id", dest="trace_ids", action="append", default=None)
    parser.add_argument("--trace-id-file", type=Path, default=None)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--db-concurrency", type=int, default=6)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--reasoning-effort", default="low")
    parser.add_argument("--min-confidence", type=float, default=0.4)
    parser.add_argument("--max-sources", type=int, default=2)
    parser.add_argument("--num-retries", type=int, default=0,
                        help="Provider retries per call; keep at zero for a bounded pilot")
    parser.add_argument("--out", type=Path, default=Path("./results"))
    parser.add_argument("--max-traces", type=int, default=None)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.set_defaults(resume=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    return asyncio.run(run(build_argparser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
