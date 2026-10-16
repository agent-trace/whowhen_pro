"""PostgreSQL source reader for the PG-backed DAG Backtrace protocol."""
from __future__ import annotations

from typing import Optional

from .dag_pg import DAGBundle, build_dag_bundle
from .pg_source import resolve_pg_dsn


class PostgresDAGRepository:
    """Read a trace's raw envelope and child span IDs without label attributes.

    Only ``source.raw_json`` is selected from the root metadata. No
    ``failure.*`` field (including ``failure.root_span``) is read by this
    repository; the canonical snapshot remains the separate scoring source.
    """

    _TRACE_SQL = """
        SELECT span_id::text AS span_id,
               parent_span_id::text AS parent_span_id,
               CASE WHEN parent_span_id IS NULL
                    THEN metadata->>'source.raw_json'
                    ELSE NULL
               END AS source_raw_json
        FROM events_dedup
        WHERE trace_id = %s
        ORDER BY start_time ASC, span_id ASC
    """

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = resolve_pg_dsn(dsn)

    def load_bundle(self, trace_id: str) -> DAGBundle:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - runtime packaging error
            raise RuntimeError(
                "PostgreSQL DAG source needs psycopg; install the harness dependencies"
            ) from exc

        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            with conn.cursor() as cur:
                cur.execute(self._TRACE_SQL, (trace_id,))
                rows = cur.fetchall()

        if not rows:
            raise KeyError(f"trace_id={trace_id!r} was not found in events_dedup")
        roots = [row for row in rows if row.get("parent_span_id") is None]
        if len(roots) != 1:
            raise ValueError(f"trace_id={trace_id!r} must have one root span, got {len(roots)}")
        source_raw_json = roots[0].get("source_raw_json")
        if not isinstance(source_raw_json, str) or not source_raw_json:
            raise ValueError(f"trace_id={trace_id!r} root span has no source.raw_json")
        pg_span_ids = {
            str(row["span_id"])
            for row in rows
            if row.get("parent_span_id") is not None
        }
        return build_dag_bundle(trace_id, source_raw_json, pg_span_ids)
