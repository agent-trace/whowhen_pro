"""Canonical database all-at-once prompt, parser and scorer.

Unlike the release protocol, ``canonical_db_all_at_once_v1`` asks for a
database node label (``S001``) and scores it against the canonical target span
ID. This removes framework-specific native-step conversions from this path.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Collection, Mapping, Optional

from .pg_source import TraceBundle
from .prompts import Taxonomy


PROTOCOL = "canonical_db_all_at_once_v1"
_CODE_RE = re.compile(r"(?<![A-Za-z0-9])([A-Z]+\.\d+)(?![A-Za-z0-9])")
_NODE_RE = re.compile(r"\b(S\d+)\b", flags=re.IGNORECASE)
_FIELD_NAMES = ("Actor", "Trace Node", "Error Mode", "Reason")
_FIELD_LOOKAHEAD = "|".join(re.escape(name) for name in _FIELD_NAMES)
_FIELD_RE = re.compile(
    rf"(?:\*\*)?(?P<key>{_FIELD_LOOKAHEAD})(?:\*\*)?[ \t]*:[ \t]*"
    rf"(?:\*\*)?[ \t]*(?P<val>.*?)"
    rf"(?=\n[ \t]*(?:\*\*)?(?:{_FIELD_LOOKAHEAD})(?:\*\*)?[ \t]*:|\Z)",
    flags=re.DOTALL | re.IGNORECASE,
)


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip().strip("`\"'")
    while value.startswith("**"):
        value = value[2:].lstrip()
    while value.endswith("**"):
        value = value[:-2].rstrip()
    return value or None


def _payload(label: str, value: Optional[str]) -> str:
    return f"{label}:\n{value}\n" if value else ""


def render_trace(bundle: TraceBundle, *, max_chars: Optional[int] = None) -> str:
    """Render model-visible trace data; no database metadata is dumped verbatim."""
    chunks = ["## Trace\n", f"Trace ID: {bundle.trace_id}\n", "[ROOT]\n"]
    if bundle.task:
        chunks.append(f"Task / user question:\n{bundle.task}\n")
    for node in bundle.nodes:
        header = (
            f"\n[{node.label}] parent={node.parent_label} type={node.node_type} "
            f"name={node.name} status={node.status}"
        )
        if node.actor:
            header += f" actor={node.actor}"
        chunks.append(header + "\n")
        chunks.append(_payload("LLM input", node.input_payload))
        chunks.append(_payload("LLM output", node.output_payload))
        if node.tool_name:
            chunks.append(f"Tool: {node.tool_name}\n")
        chunks.append(_payload("Tool arguments", node.tool_arguments))
        chunks.append(_payload("Tool result", node.tool_result))
    rendered = "".join(chunks)
    if max_chars is not None and max_chars > 0 and len(rendered) > max_chars:
        return rendered[:max_chars] + "\n[Prompt truncated by explicit --prompt-max-chars]\n"
    return rendered


def canonical_prompt(bundle: TraceBundle, taxonomy: Taxonomy,
                     *, max_chars: Optional[int] = None) -> str:
    """Build a database-native attribution prompt without labels or raw rows."""
    return (
        "# Failure attribution task\n\n"
        "The following failed agent trace is represented as numbered trace nodes. "
        "Find the first decisive error: the action or response that most directly "
        "causes the eventual failure. Do not infer labels from missing data.\n\n"
        "## Error mode taxonomy\n\n"
        f"{taxonomy.block}\n\n"
        f"{render_trace(bundle, max_chars=max_chars)}\n\n"
        "## Response format\n\n"
        "Return exactly these four fields:\n"
        "Actor: <actor name shown on the responsible node, or unknown>\n"
        "Trace Node: <one node label such as S001>\n"
        "Error Mode: <one taxonomy code>\n"
        "Reason: <one or two sentences>\n"
    )


@dataclass
class CanonicalPrediction:
    actor: Optional[str] = None
    trace_node: Optional[str] = None
    error_mode: Optional[str] = None
    reason: Optional[str] = None
    parse_warnings: list[str] = field(default_factory=list)


def parse_canonical(text: Optional[str], valid_codes: Collection[str]) -> CanonicalPrediction:
    """Leniently parse the four canonical protocol fields."""
    prediction = CanonicalPrediction()
    if not text:
        prediction.parse_warnings.append("empty response")
        return prediction
    values = {match.group("key").title(): match.group("val").strip()
              for match in _FIELD_RE.finditer(text)}
    prediction.actor = _clean(values.get("Actor"))
    raw_node = values.get("Trace Node")
    node_match = _NODE_RE.search(raw_node or "") or _NODE_RE.search(text)
    prediction.trace_node = node_match.group(1).upper() if node_match else None
    raw_mode = values.get("Error Mode") or ""
    candidates = _CODE_RE.findall(raw_mode) or _CODE_RE.findall(text)
    known = set(valid_codes)
    prediction.error_mode = next((code for code in candidates if code in known), None)
    prediction.reason = _clean(values.get("Reason"))
    for name, value in (("Actor", prediction.actor), ("Trace Node", prediction.trace_node),
                        ("Error Mode", prediction.error_mode), ("Reason", prediction.reason)):
        if not value:
            prediction.parse_warnings.append(f"missing field: {name}")
    return prediction


def _norm(value: Any) -> str:
    return str(value).strip().casefold() if value not in (None, "") else ""


def score_canonical(prediction: Optional[dict[str, Any]], gold: Mapping[str, Any]) -> dict[str, Optional[bool]]:
    """Score actor, canonical span target, mode, and their joint result.

    A source record with no labelled actor is a single-agent case. Its actor
    score is ``None`` (excluded from the actor denominator); joint scoring then
    depends on the node and mode axes, matching the benchmark's treatment of
    single-agent attribution.
    """
    label = gold.get("label") if isinstance(gold, dict) else None
    if not isinstance(label, dict):
        raise ValueError("canonical gold record lacks label object")
    if not prediction:
        return {"actor": None if label.get("actor") is None else False,
                "when": False, "mode": False, "all": False}
    expected_actor = label.get("actor")
    actor = None if expected_actor is None else _norm(prediction.get("actor")) == _norm(expected_actor)
    when = prediction.get("target_span_id") == label.get("target_span_id")
    mode = _norm(prediction.get("error_mode")).upper() == _norm(label.get("mode")).upper()
    return {"actor": actor, "when": when, "mode": mode,
            "all": bool(when and mode and actor is not False)}
