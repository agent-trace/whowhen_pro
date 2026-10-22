"""Run canonical all-at-once attribution using PostgreSQL trace reads.

``run.py`` remains the paper/release-compatible JSONL runner. This entry point
implements the separate ``canonical_db_all_at_once_v1`` protocol: trace content
comes only from ``events_dedup``; labels come only from an offline canonical
snapshot and are never placed in a model prompt.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .canonical import PROTOCOL, canonical_prompt, parse_canonical, score_canonical
from .graph import GRAPH_PROTOCOL, graph_prompt
from .llm import generate
from .pg_source import PostgresTraceRepository
from .prompts import load_taxonomy, user_msg
from .store import ResultsStore


_NO_USAGE = {
    "input_tokens": None, "output_tokens": None, "total_tokens": None,
    "reasoning_tokens": None, "finish_reason": None,
}


def load_snapshot(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            trace_id = record.get("trace_id")
            if record.get("protocol") != PROTOCOL or not isinstance(trace_id, str):
                raise ValueError(f"invalid canonical snapshot record at line {line_number}")
            if trace_id in seen:
                raise ValueError(f"duplicate trace_id in canonical snapshot: {trace_id}")
            seen.add(trace_id)
            records.append(record)
    if not records:
        raise ValueError(f"canonical snapshot is empty: {path}")
    return records


def select_records(records: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, list[dict[str, Any]]]:
    selected: dict[str, list[dict[str, Any]]] = {}
    wanted_trace_ids = set(args.trace_ids or [])
    if args.trace_id_file:
        with args.trace_id_file.open(encoding="utf-8") as handle:
            wanted_trace_ids.update(
                line.strip() for line in handle if line.strip() and not line.startswith("#")
            )
    for record in records:
        if wanted_trace_ids and record.get("trace_id") not in wanted_trace_ids:
            continue
        if args.modality and record.get("split") != args.modality:
            continue
        if args.framework and record.get("framework") != args.framework:
            continue
        if args.benchmark != "all" and record.get("benchmark") != args.benchmark:
            continue
        selected.setdefault(str(record.get("benchmark") or "unknown"), []).append(record)
    return selected


async def evaluate_one(*, api_sem: asyncio.Semaphore, db_sem: asyncio.Semaphore,
                       repository: PostgresTraceRepository, record: dict[str, Any],
                       taxonomy, api_key: Optional[str], args: argparse.Namespace) -> dict[str, Any]:
    trace_id = record["trace_id"]
    protocol = GRAPH_PROTOCOL if args.method == "graph" else PROTOCOL
    prompt_builder = graph_prompt if args.method == "graph" else canonical_prompt
    base: dict[str, Any] = {
        "protocol": protocol,
        "method": args.method,
        "model": args.model,
        "trace_id": trace_id,
        "max_tokens": args.max_tokens,
        "finish_reason": None,
        "reasoning_tokens": None,
        "framework": record.get("framework"),
        "benchmark": record.get("benchmark"),
        "modality": record.get("split"),
        "trace_path": f"postgresql/events_dedup/{trace_id}",
        "canonical_ground_truth": record.get("label"),
        "error_mode": (record.get("label") or {}).get("mode"),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        async with db_sem:
            bundle = await asyncio.to_thread(repository.load_bundle, trace_id)
        expected_target = (record.get("label") or {}).get("target_span_id")
        if expected_target not in {node.span_id for node in bundle.nodes}:
            raise ValueError(
                "canonical target span is absent from the label-safe PostgreSQL bundle"
            )
        prompt = prompt_builder(bundle, taxonomy, max_chars=args.prompt_max_chars)
    except Exception as exc:  # noqa: BLE001 - per-trace fault evidence
        return {**base, "raw_output": None, "prediction": None, "score": None,
                "usage": dict(_NO_USAGE), "duration_s": 0.0,
                "error": f"pg_bundle: {type(exc).__name__}: {exc}"}

    prompt_info = {
        "chars": len(prompt), "nodes": len(bundle.nodes),
        "sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
    }
    if args.dry_run:
        return {**base, "raw_output": None, "prediction": None, "score": None,
                "usage": dict(_NO_USAGE), "duration_s": 0.0,
                "dry_run": prompt_info, "error": None}

    raw: Optional[str] = None
    usage = dict(_NO_USAGE)
    error: Optional[str] = None
    started: Optional[float] = None
    try:
        async with api_sem:
            # Measure provider latency only. Starting before acquiring the
            # semaphore would include queue wait and inflate per-request
            # duration for large sweeps.
            started = time.monotonic()
            raw, usage = await asyncio.to_thread(
                generate, args.model, [user_msg([{"type": "text", "text": prompt}])],
                temperature=args.temperature, max_tokens=args.max_tokens,
                reasoning_effort=args.reasoning_effort, api_base=args.api_base,
                api_key=api_key,
            )
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    duration = round(time.monotonic() - started, 3) if started is not None else 0.0

    prediction: Optional[dict[str, Any]] = None
    if raw is not None:
        parsed = parse_canonical(raw, taxonomy.codes)
        node = bundle.node_by_label.get(parsed.trace_node or "")
        prediction = {
            "actor": parsed.actor,
            "trace_node": parsed.trace_node,
            "target_span_id": node.span_id if node else None,
            "error_mode": parsed.error_mode,
            "reason": parsed.reason,
            "parse_warnings": parsed.parse_warnings,
        }
    return {**base, "raw_output": raw, "prediction": prediction,
            "score": score_canonical(prediction, record), "usage": usage,
            "finish_reason": usage.get("finish_reason"),
            "reasoning_tokens": usage.get("reasoning_tokens"),
            "duration_s": duration, "prompt": prompt_info, "error": error}



def _write_run_state(out: Path, state: dict[str, Any]) -> None:
    out.mkdir(parents=True, exist_ok=True)
    target = out / "run-state.json"
    temporary = target.with_suffix(".json.tmp")
    state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n",
                         encoding="utf-8")
    os.replace(temporary, target)


def _append_pause_event(out: Path, result: dict[str, Any],
                        api_key: Optional[str]) -> dict[str, Any]:
    error = str(result.get("error") or "unknown error")
    if api_key:
        error = error.replace(api_key, "[REDACTED]")
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "trace_id": result.get("trace_id"),
        "benchmark": result.get("benchmark"),
        "phase": "postgresql" if error.startswith("pg_bundle:") else "api_or_runner",
        "error": error,
    }
    out.mkdir(parents=True, exist_ok=True)
    with (out / "pause-events.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
        handle.flush()
    return event


async def _process_pending(records: list[dict[str, Any]],
                           stores: dict[str, ResultsStore], *,
                           api_sem: asyncio.Semaphore,
                           db_sem: asyncio.Semaphore,
                           repository: PostgresTraceRepository,
                           taxonomy,
                           api_key: Optional[str],
                           args: argparse.Namespace,
                           state: dict[str, Any]) -> tuple[int, int, int, bool]:
    """Process a bounded worker queue; optionally stop scheduling after any error."""
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for record in records:
        queue.put_nowait(record)
    stop = asyncio.Event()
    attempted = succeeded = failed = 0
    initial_attempted = int(state.get("attempted_this_run", 0))
    initial_failed = int(state.get("failed_attempts_this_run", 0))
    initial_completed = int(state.get("completed_traces", 0))
    progress_every = max(1, len(records) // 20) if records else 1
    started = time.monotonic()

    def update_state(status: str) -> None:
        state["status"] = status
        state["attempted_this_run"] = initial_attempted + attempted
        state["failed_attempts_this_run"] = initial_failed + failed
        state["completed_traces"] = initial_completed + succeeded
        state["remaining_traces"] = max(
            int(state.get("total_traces", 0)) - state["completed_traces"], 0
        )
        if state["remaining_traces"] == 0 and state["failed_attempts_this_run"] == 0:
            state["status"] = "complete"
        _write_run_state(args.out, state)

    async def worker() -> None:
        nonlocal attempted, succeeded, failed
        while not stop.is_set():
            try:
                record = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                result = await evaluate_one(
                    api_sem=api_sem, db_sem=db_sem, repository=repository,
                    record=record, taxonomy=taxonomy, api_key=api_key, args=args,
                )
            except Exception as exc:  # noqa: BLE001 - keep a checkpoint on unexpected faults
                result = {
                    "trace_id": record.get("trace_id"),
                    "benchmark": record.get("benchmark"),
                    "error": f"runner: {type(exc).__name__}: {exc}",
                }

            attempted += 1
            if result.get("error"):
                failed += 1
                if args.pause_on_error:
                    event = _append_pause_event(args.out, result, api_key)
                    state["last_error"] = event
                    stop.set()
                elif not args.dry_run:
                    stores[result["benchmark"]].append(result)
            else:
                succeeded += 1
                if not args.dry_run:
                    stores[result["benchmark"]].append(result)

            if attempted % progress_every == 0 or attempted == len(records) or stop.is_set():
                elapsed = time.monotonic() - started
                rate = attempted / elapsed if elapsed else 0.0
                print(
                    f"[progress] {attempted}/{len(records)} attempted "
                    f"succeeded={succeeded} failed={failed} queued={queue.qsize()} "
                    f"{rate:.2f}/s",
                    file=sys.stderr,
                )
                update_state("paused" if stop.is_set() else "running")
            queue.task_done()

    workers = [asyncio.create_task(worker())
               for _ in range(min(max(1, args.concurrency), len(records)))]
    if workers:
        await asyncio.gather(*workers)
    paused = stop.is_set()
    update_state("paused" if paused else "running")
    return attempted, succeeded, failed, paused


async def run(args: argparse.Namespace) -> int:
    taxonomy = load_taxonomy(args.taxonomy_path.parent)
    all_records = load_snapshot(args.canonical_snapshot)
    by_benchmark = select_records(all_records, args)
    if not by_benchmark:
        print("No canonical snapshot records matched the selected filters", file=sys.stderr)
        return 1
    repository = PostgresTraceRepository(args.pg_dsn)
    api_key = None
    if args.api_key_env:
        api_key = os.environ.get(args.api_key_env)
        if not api_key:
            raise RuntimeError(
                f"required API key environment variable is not set: {args.api_key_env}"
            )
    api_sem = asyncio.Semaphore(args.concurrency)
    db_sem = asyncio.Semaphore(min(args.db_concurrency, args.concurrency))
    total_done = total_failed = total_skipped = 0
    stores: dict[str, ResultsStore] = {}
    pending_by_benchmark: dict[str, list[dict[str, Any]]] = {}
    for benchmark, records in by_benchmark.items():
        store = ResultsStore.for_cell(args.out, args.model, benchmark)
        stores[benchmark] = store
        done = store.done_trace_ids() if args.resume else set()
        pending = [record for record in records if record["trace_id"] not in done]
        total_skipped += len(records) - len(pending)
        if args.max_traces is not None:
            pending = pending[:args.max_traces]
        print(f"[{benchmark}] {len(pending)} canonical-PG trace(s) -> {store.path}", file=sys.stderr)

        pending_by_benchmark[benchmark] = pending

    total_records = sum(len(records) for records in by_benchmark.values())
    run_state: dict[str, Any] = {
        "status": "running",
        "model": args.model,
        "method": args.method,
        "modality": args.modality,
        "api_base": args.api_base,
        "snapshot": str(args.canonical_snapshot),
        "output_dir": str(args.out),
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_traces": total_records,
        "completed_traces": total_skipped,
        "remaining_traces": max(total_records - total_skipped, 0),
        "attempted_this_run": 0,
        "failed_attempts_this_run": 0,
        "last_error": None,
    }
    _write_run_state(args.out, run_state)
    total_succeeded = 0
    paused = False

    if args.schedule_scope == "global":
        pending_records = [
            record for pending in pending_by_benchmark.values() for record in pending
        ]
        attempted, succeeded, failed, paused = await _process_pending(
            pending_records, stores, api_sem=api_sem, db_sem=db_sem,
            repository=repository, taxonomy=taxonomy, api_key=api_key,
            args=args, state=run_state,
        )
        total_done += attempted
        total_succeeded += succeeded
        total_failed += failed
    else:
        for benchmark, pending in pending_by_benchmark.items():
            attempted, succeeded, failed, paused = await _process_pending(
                pending, stores, api_sem=api_sem, db_sem=db_sem,
                repository=repository, taxonomy=taxonomy, api_key=api_key,
                args=args, state=run_state,
            )
            total_done += attempted
            total_succeeded += succeeded
            total_failed += failed
            if paused:
                break

    if paused:
        run_state["status"] = "paused"
    elif run_state["remaining_traces"] == 0 and total_failed == 0:
        run_state["status"] = "complete"
    else:
        run_state["status"] = "partial"
    _write_run_state(args.out, run_state)
    print(f"canonical-PG summary: done={total_done}, skipped={total_skipped}, failed={total_failed}, status={run_state['status']}", file=sys.stderr)
    return 3 if paused else (0 if total_failed == 0 else 2)


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Canonical all-at-once runner backed by PostgreSQL")
    parser.add_argument("--model", required=True)
    parser.add_argument("--method", choices=("canonical", "graph"), default="canonical",
                        help="canonical baseline or graph-aware parent-child attribution prompt")
    parser.add_argument("--api-base", default=None,
                        help="Optional OpenAI-compatible API base URL; never persisted in result JSONL")
    parser.add_argument("--api-key-env", default=None,
                        help="Name of an environment variable to pass as the provider API key")
    parser.add_argument("--canonical-snapshot", required=True, type=Path)
    parser.add_argument("--taxonomy-path", required=True, type=Path,
                        help="taxonomy.yaml only; trace content is read from PostgreSQL")
    parser.add_argument("--pg-dsn", default=None,
                        help="PostgreSQL DSN; defaults to WHOWHEN_PG_DSN then OPENGAUSS_DSN")
    parser.add_argument("--benchmark", default="all")
    parser.add_argument("--modality", default="text")
    parser.add_argument("--framework", default=None)
    parser.add_argument("--trace-id", dest="trace_ids", action="append", default=None,
                        help="Restrict to an exact canonical trace ID; may be repeated")
    parser.add_argument("--trace-id-file", type=Path, default=None,
                        help="Newline-delimited canonical trace IDs; comments begin with #")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--db-concurrency", type=int, default=8)
    parser.add_argument("--schedule-scope", choices=("benchmark", "global"), default="benchmark",
                        help="Schedule each benchmark in sequence (default) or share one pool globally")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=2048)
    parser.add_argument("--no-max-tokens", dest="max_tokens", action="store_const", const=None,
                        help="Omit max_tokens so the provider chooses its own output limit")
    parser.add_argument("--reasoning-effort", default=None)
    parser.add_argument("--prompt-max-chars", type=int, default=None,
                        help="Optional explicit prompt cap; omitted means no prompt truncation")
    parser.add_argument("--out", type=Path, default=Path("./results"))
    parser.add_argument("--max-traces", type=int, default=None)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.set_defaults(resume=True)
    parser.add_argument("--pause-on-error", action="store_true",
                        help="stop scheduling new traces on the first trace-level error; failed trace is left retryable on resume")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    return asyncio.run(run(build_argparser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
