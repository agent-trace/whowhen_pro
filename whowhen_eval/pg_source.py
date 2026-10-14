"""Read a label-safe attribution trace bundle from PostgreSQL.

The database is the source of trace content for ``canonical_db_all_at_once_v1``.
It is deliberately *not* a generic metadata dump: failure labels and the
lossless source-record envelope remain in PostgreSQL for audit/scoring, but are
never copied into the bundle that is rendered for a model.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional


_SENSITIVE_PREFIXES = ("failure.", "source.raw_")
_MODEL_OPERATIONS = {"chat", "text_completion", "generate_content", "embeddings"}


def resolve_pg_dsn(explicit: Optional[str] = None) -> str:
    """Return an explicitly supplied DSN or an environment-provided one.

    Credentials are intentionally not stored in source or result artifacts.
    ``WHOWHEN_PG_DSN`` is the dedicated setting; ``OPENGAUSS_DSN`` remains a
    compatibility fallback for the existing local exporter setup.
    """
    dsn = explicit or os.environ.get("WHOWHEN_PG_DSN") or os.environ.get("OPENGAUSS_DSN")
    if not dsn:
        raise RuntimeError(
            "set WHOWHEN_PG_DSN (or OPENGAUSS_DSN), or pass --pg-dsn; "
            "the canonical database runner never embeds credentials"
        )
    return dsn


def _decode_json(value: Any, default: Any) -> Any:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _display_value(value: Any) -> Optional[str]:
    """Format a stored JSON value without reintroducing hidden metadata."""
    value = _decode_json(value, None)
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def _safe_metadata(value: Any) -> dict[str, Any]:
    metadata = _decode_json(value, {})
    if not isinstance(metadata, dict):
        return {}
    return {
        str(key): item
        for key, item in metadata.items()
        if not str(key).startswith(_SENSITIVE_PREFIXES)
    }


@dataclass(frozen=True)
class TraceNode:
    """One displayable span. ``label`` is the model-facing locator."""

    label: str
    span_id: str
    parent_label: str
    name: str
    node_type: str
    actor: Optional[str]
    status: str
    input_payload: Optional[str]
    output_payload: Optional[str]
    tool_name: Optional[str]
    tool_arguments: Optional[str]
    tool_result: Optional[str]


@dataclass(frozen=True)
class TraceBundle:
    """The safe, normalized model input derived from one database trace."""

    trace_id: str
    task: str
    nodes: tuple[TraceNode, ...]

    @property
    def node_by_label(self) -> dict[str, TraceNode]:
        return {node.label: node for node in self.nodes}


def trace_bundle_from_rows(trace_id: str, rows: Iterable[Mapping[str, Any]]) -> TraceBundle:
    """Build a safe :class:`TraceBundle` from ``events_dedup`` rows.

    The caller may pass a real psycopg ``dict_row`` result or a plain dict in
    tests. Rows are sorted again here so node labels remain deterministic even
    if a database driver's ordering changes.
    """
    ordered = sorted(rows, key=lambda row: (str(row.get("start_time") or ""), str(row["span_id"])))
    if not ordered:
        raise KeyError(f"trace_id={trace_id!r} was not found in events_dedup")

    root_rows = [row for row in ordered if not row.get("parent_span_id")]
    if len(root_rows) != 1:
        raise ValueError(f"trace_id={trace_id!r} must have one root span, got {len(root_rows)}")
    root = root_rows[0]
    root_meta = _safe_metadata(root.get("metadata"))
    task = str(root_meta.get("prompt_text") or root_meta.get("user_prompt") or "")

    child_rows = [row for row in ordered if row.get("span_id") != root["span_id"]]
    labels = {str(row["span_id"]): f"S{index:03d}" for index, row in enumerate(child_rows, 1)}
    labels[str(root["span_id"])] = "ROOT"

    nodes: list[TraceNode] = []
    for row in child_rows:
        metadata = _safe_metadata(row.get("metadata"))
        raw_type = str(metadata.get("span.type") or row.get("type") or "span")
        operation = str(metadata.get("gen_ai.operation.name") or "")
        node_type = "llm" if operation in _MODEL_OPERATIONS or row.get("type") == "generation" else raw_type
        span_id = str(row["span_id"])
        parent_id = str(row.get("parent_span_id") or root["span_id"])
        nodes.append(TraceNode(
            label=labels[span_id],
            span_id=span_id,
            parent_label=labels.get(parent_id, "ROOT"),
            name=str(row.get("name") or "unnamed"),
            node_type=node_type,
            actor=_display_value(metadata.get("agent.name")),
            status=str(row.get("level") or "DEFAULT"),
            input_payload=_display_value(row.get("input")),
            output_payload=_display_value(row.get("output")),
            tool_name=_display_value(metadata.get("gen_ai.tool.name")),
            tool_arguments=_display_value(metadata.get("gen_ai.tool.call.arguments")),
            tool_result=_display_value(metadata.get("gen_ai.tool.call.result")),
        ))
    return TraceBundle(trace_id=trace_id, task=task, nodes=tuple(nodes))


class PostgresTraceRepository:
    """Minimal psycopg-backed reader for the canonical PostgreSQL protocol."""

    _TRACE_SQL = """
        SELECT trace_id, span_id, parent_span_id, start_time, name, type, level,
               status_message, input, output,
               (
                   SELECT COALESCE(json_object_agg(entry.key, entry.value), '{}'::json)
                   FROM json_each(metadata) AS entry(key, value)
                   WHERE entry.key NOT LIKE 'failure.%%'
                     AND entry.key NOT LIKE 'source.raw_%%'
               ) AS metadata
        FROM events_dedup
        WHERE trace_id = %s
        ORDER BY start_time ASC, span_id ASC
    """

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = resolve_pg_dsn(dsn)

    def load_bundle(self, trace_id: str) -> TraceBundle:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - packaging/runtime error
            raise RuntimeError(
                "PostgreSQL source needs psycopg; install the harness dependencies"
            ) from exc
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(self._TRACE_SQL, (trace_id,))
                rows = cur.fetchall()
        return trace_bundle_from_rows(trace_id, rows)
