"""Run DAG Backtrace on source trajectories read from PostgreSQL."""
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

from .dag_pg import METHOD, PROTOCOL, build_edges_messages, parse_json_object, run_dag_backtrace, score_dag_prediction
from .dag_pg_source import PostgresDAGRepository
from .llm import generate
from .run_pg import load_snapshot, select_records
from .store import ResultsStore


_NO_USAGE = {"input_tokens": None, "output_tokens": None, "total_tokens": None}


def _message_text(messages: list[dict[str, str]]) -> str:
    return "\n".join(message.get("content", "") for message in messages)


def _successful_trace_ids(path: Path) -> set[str]:
    """Only completed successful cases are skipped on resume."""
    if not path.exists():
        return set()
    done: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("status") == "success" and isinstance(row.get("trace_id"), str):
                done.add(row["trace_id"])
    return done


def _sum_usage(calls: list[dict[str, Any]]) -> dict[str, Optional[int]]:
    result: dict[str, Optional[int]] = {}
    for key in ("input_tokens", "output_tokens", "total_tokens"):
        values = [call.get("usage", {}).get(key) for call in calls]
        known = [int(value) for value in values if isinstance(value, int)]
        result[key] = sum(known) if known else None
    return result


def _prompt_info(messages: list[dict[str, str]]) -> dict[str, Any]:
    text = _message_text(messages)
    return {"chars": len(text), "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest()}


async def evaluate_one(
    *,
    api_sem: asyncio.Semaphore,
    db_sem: asyncio.Semaphore,
    repository: PostgresDAGRepository,
    record: dict[str, Any],
    api_key: Optional[str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    trace_id = record["trace_id"]
    base: dict[str, Any] = {
        "protocol": PROTOCOL,
        "method": METHOD,
        "model": args.model,
        "trace_id": trace_id,
        "raw_id": record.get("raw_id"),
        "framework": record.get("framework"),
        "benchmark": record.get("benchmark"),
        "modality": record.get("split"),
        "trace_path": f"postgresql/events_dedup/{trace_id}",
        # Scoring labels remain outside the model-facing bundle and prompts.
        "canonical_ground_truth": record.get("label"),
        "error_mode": (record.get("label") or {}).get("mode"),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    wall_started = time.monotonic()
    try:
        async with db_sem:
            bundle = await asyncio.to_thread(repository.load_bundle, trace_id)
        label = record.get("label") or {}
        target_span_id = label.get("target_span_id")
        if target_span_id not in bundle.pg_span_ids:
            raise ValueError("canonical target span is absent from the PostgreSQL trace")
        target_steps = [
            step_id for step_id, span_ids in enumerate(bundle.step_span_ids)
            if target_span_id in span_ids
        ]
        if len(target_steps) != 1:
            raise ValueError(
                "canonical target must map to exactly one DAG source step; "
                f"mapped_to={len(target_steps)}"
            )
        build_prompt = build_edges_messages(bundle)
        prompt_info = {
            "step_count": len(bundle.nodes),
            "pg_span_count": len(bundle.pg_span_ids),
            "build": _prompt_info(build_prompt),
        }
        if args.dry_run:
            return {
                **base,
                "status": "dry_run",
                "prediction": None,
                "score": None,
                "usage": dict(_NO_USAGE),
                "api_calls": 0,
                "calls": [],
                "duration_s": 0.0,
                "dry_run": prompt_info,
                "error": None,
            }
    except Exception as exc:  # noqa: BLE001 - trace-local source failure
        return {
            **base,
            "status": "failed",
            "prediction": None,
            "score": None,
            "usage": dict(_NO_USAGE),
            "api_calls": 0,
            "calls": [],
            "duration_s": round(time.monotonic() - wall_started, 3),
            "error": f"pg_bundle: {type(exc).__name__}: {exc}",
        }

    calls: list[dict[str, Any]] = []

    async def request_json(messages: list[dict[str, str]], operation: str) -> dict[str, Any]:
        prompt_info_for_call = _prompt_info(messages)
        started: Optional[float] = None
        raw: Optional[str] = None
        usage: dict[str, Optional[int]] = dict(_NO_USAGE)
        call: dict[str, Any] = {
            "operation": operation,
            "prompt": prompt_info_for_call,
            "usage": dict(_NO_USAGE),
        }
        try:
            async with api_sem:
                # Exclude semaphore queue wait from provider request duration.
                started = time.monotonic()
                raw, usage = await asyncio.to_thread(
                    generate,
                    args.model,
                    messages,
                    temperature=args.temperature,
                    max_tokens=args.max_tokens,
                    reasoning_effort=args.reasoning_effort,
                    api_base=args.api_base,
                    api_key=api_key,
                )
            call["duration_s"] = round(time.monotonic() - started, 3) if started is not None else 0.0
            call["usage"] = usage
            call["response_chars"] = len(raw or "")
            parsed = parse_json_object(raw)
            call["status"] = "success"
            calls.append(call)
            return parsed
        except Exception as exc:  # noqa: BLE001 - retained as per-call evidence
            call["duration_s"] = round(time.monotonic() - started, 3) if started is not None else 0.0
            call["usage"] = usage
            call["response_chars"] = len(raw or "") if raw is not None else None
            call["status"] = "failed"
            call["error"] = f"{type(exc).__name__}: {exc}"
            calls.append(call)
            raise

    try:
        result = await run_dag_backtrace(bundle, request_json)
        prediction = result["prediction"]
        score = score_dag_prediction(prediction, record, bundle)
        return {
            **base,
            **result,
            "status": "success",
            "score": score,
            "usage": _sum_usage(calls),
            "api_calls": len(calls),
            "calls": calls,
            "prompt": prompt_info,
            "duration_s": round(time.monotonic() - wall_started, 3),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 - preserve partial call/candidate evidence
        return {
            **base,
            "status": "failed",
            "prediction": None,
            "score": score_dag_prediction(None, record, bundle),
            "usage": _sum_usage(calls),
            "api_calls": len(calls),
            "calls": calls,
            "prompt": prompt_info,
            "duration_s": round(time.monotonic() - wall_started, 3),
            "error": f"{type(exc).__name__}: {exc}",
        }


async def run(args: argparse.Namespace) -> int:
    if args.concurrency < 1 or args.db_concurrency < 1:
        raise ValueError("concurrency values must be positive integers")
    all_records = load_snapshot(args.canonical_snapshot)
    by_benchmark = select_records(all_records, args)
    if not by_benchmark:
        print("No canonical snapshot records matched the selected filters", file=sys.stderr)
        return 1

    repository = PostgresDAGRepository(args.pg_dsn)
    api_key = None
    if args.api_key_env:
        api_key = os.environ.get(args.api_key_env)
        if not api_key and not args.dry_run:
            raise RuntimeError(
                f"required API key environment variable is not set: {args.api_key_env}"
            )
    api_sem = asyncio.Semaphore(args.concurrency)
    db_sem = asyncio.Semaphore(min(args.db_concurrency, args.concurrency))
    stores: dict[str, ResultsStore] = {}
    pending_by_benchmark: dict[str, list[dict[str, Any]]] = {}
    skipped = 0

    for benchmark, records in by_benchmark.items():
        # Keep DAG output and resume state isolated from canonical/all-at-once runs.
        store = ResultsStore.for_cell(args.out / METHOD, args.model, benchmark)
        stores[benchmark] = store
        done = _successful_trace_ids(store.path) if args.resume else set()
        pending = [record for record in records if record["trace_id"] not in done]
        skipped += len(records) - len(pending)
        if args.max_traces is not None:
            pending = pending[:args.max_traces]
        pending_by_benchmark[benchmark] = pending
        print(
            f"[{benchmark}] {len(pending)} DAG-PG trace(s) -> {store.path}",
            file=sys.stderr,
        )

    pending_records = [record for rows in pending_by_benchmark.values() for record in rows]
    started = time.monotonic()
    failed = 0
    tasks = [
        evaluate_one(
            api_sem=api_sem,
            db_sem=db_sem,
            repository=repository,
            record=record,
            api_key=api_key,
            args=args,
        )
        for record in pending_records
    ]
    for index, future in enumerate(asyncio.as_completed(tasks), 1):
        result = await future
        if result.get("status") == "failed":
            failed += 1
        if not args.dry_run:
            stores[result["benchmark"]].append(result)
        if index % max(1, len(pending_records) // 20) == 0 or index == len(pending_records):
            elapsed = time.monotonic() - started
            rate = index / elapsed if elapsed else 0.0
            print(
                f"[global] {index}/{len(pending_records)} failed={failed} {rate:.2f}/s",
                file=sys.stderr,
            )

    print(
        f"DAG-PG summary: done={len(pending_records)}, skipped={skipped}, "
        f"failed={failed}, dry_run={args.dry_run}",
        file=sys.stderr,
    )
    return 0 if failed == 0 else 2


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="DAG Backtrace with trajectory data read from PostgreSQL"
    )
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument("--canonical-snapshot", required=True, type=Path,
                        help="Scoring labels only; trajectory content is fetched from PostgreSQL")
    parser.add_argument("--pg-dsn", default=None,
                        help="PostgreSQL DSN; defaults to WHOWHEN_PG_DSN then OPENGAUSS_DSN")
    parser.add_argument("--benchmark", default="all")
    parser.add_argument("--modality", default="text")
    parser.add_argument("--framework", default=None)
    parser.add_argument("--trace-id", dest="trace_ids", action="append", default=None)
    parser.add_argument("--trace-id-file", type=Path, default=None)
    parser.add_argument("--concurrency", type=int, default=4,
                        help="Maximum in-flight model requests across traces")
    parser.add_argument("--db-concurrency", type=int, default=8)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=2048)
    parser.add_argument("--reasoning-effort", default=None)
    parser.add_argument("--out", type=Path, default=Path("./results"))
    parser.add_argument("--max-traces", type=int, default=None)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    parser.set_defaults(resume=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate PG input, label isolation and span crosswalk without model calls")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    return asyncio.run(run(build_argparser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
