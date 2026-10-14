"""Graph-aware canonical prompt for database-backed attribution."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Optional

from .canonical import render_trace
from .pg_source import TraceBundle
from .prompts import Taxonomy


GRAPH_PROTOCOL = "canonical_db_graph_all_at_once_v1"


def render_dependency_graph(bundle: TraceBundle) -> str:
    """Render deterministic parent-to-child topology derived from PG spans."""
    children: dict[str, list[str]] = defaultdict(list)
    parent_by_label = {node.label: node.parent_label for node in bundle.nodes}
    labels = {node.label for node in bundle.nodes}
    for node in bundle.nodes:
        parent = node.parent_label if node.parent_label in labels else "ROOT"
        children[parent].append(node.label)
    for values in children.values():
        values.sort()

    def descendants(label: str) -> set[str]:
        found: set[str] = set()
        queue = deque(children.get(label, ()))
        while queue:
            child = queue.popleft()
            if child in found or child == label:
                continue
            found.add(child)
            queue.extend(children.get(child, ()))
        return found

    def depth(label: str) -> int:
        current = label
        seen: set[str] = set()
        result = 0
        while current in parent_by_label and current not in seen:
            seen.add(current)
            parent = parent_by_label[current]
            if parent == "ROOT" or parent not in labels:
                return result + 1
            current = parent
            result += 1
        return result + 1

    lines = [
        "## Span dependency graph",
        "Each directed parent -> child edge represents span nesting in PostgreSQL. "
        "Nesting provides execution context; it does not by itself prove causation.",
        "Adjacency list:",
    ]
    for parent in ["ROOT", *sorted(labels)]:
        child_labels = children.get(parent, [])
        if parent == "ROOT" or child_labels:
            lines.append(f"- {parent} -> {', '.join(child_labels) if child_labels else '(no children)'}")
    leaves = sorted(label for label in labels if not children.get(label))
    lines.append(f"Terminal nodes (no child spans): {', '.join(leaves) if leaves else '(none)'}")
    lines.append("Structural context:")
    for node in sorted(bundle.nodes, key=lambda item: item.label):
        direct = children.get(node.label, [])
        lines.append(
            f"- {node.label}: depth={depth(node.label)}, "
            f"children={','.join(direct) if direct else 'none'}, "
            f"descendants={len(descendants(node.label))}"
        )
    return "\n".join(lines) + "\n"


def graph_prompt(bundle: TraceBundle, taxonomy: Taxonomy,
                 *, max_chars: Optional[int] = None) -> str:
    """Build a graph-aware prompt while preserving the canonical answer schema."""
    return (
        "# Failure attribution task (graph-based)\n\n"
        "Find the first decisive error: the action or response that most directly "
        "causes the eventual task failure. Use the parent-child span graph to trace "
        "how evidence and consequences propagate through the execution.\n\n"
        "Reason in this order: inspect terminal-node outputs and tool results for "
        "observable failure evidence; follow parent links backward to compare the "
        "candidate with its ancestors and sibling branches; choose the earliest "
        "decisive mistake, not merely a downstream symptom or a later recovery attempt. "
        "All trace nodes are shown; topology alone is not a failure label.\n\n"
        "## Error mode taxonomy\n\n"
        f"{taxonomy.block}\n\n"
        f"{render_dependency_graph(bundle)}\n"
        f"{render_trace(bundle, max_chars=max_chars)}\n\n"
        "## Response format\n\n"
        "Return exactly these four fields:\n"
        "Actor: <actor name shown on the responsible node, or unknown>\n"
        "Trace Node: <one node label such as S001>\n"
        "Error Mode: <one taxonomy code>\n"
        "Reason: <one or two sentences; identify the graph evidence supporting the choice>\n"
    )
