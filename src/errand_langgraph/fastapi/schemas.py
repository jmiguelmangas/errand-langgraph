"""Derive a FastAPI request-body type from a compiled graph's own schema.

No introspection reimplemented here: verified against langgraph 1.2.11 that
``CompiledStateGraph.get_input_schema()`` already builds a Pydantic v2 model
from the graph's ``state_schema`` when it's a ``TypedDict`` -- a
``RootModel``-shaped wrapper whose ``model_dump()`` returns the plain state
dict directly (not wrapped in ``{"root": ...}``), which is exactly the shape
``GraphRunner.submit`` wants. This just calls that and falls back to
``dict[str, Any]`` -- still a valid FastAPI body type, just without
field-level validation -- when the schema isn't introspectable.
"""

from __future__ import annotations

from typing import Any


def build_input_type(graph: Any) -> Any:
    """Return a type usable as a FastAPI request-body annotation for ``graph``.

    A Pydantic model when ``graph.get_input_schema()`` succeeds, else
    ``dict[str, Any]``.
    """
    try:
        return graph.get_input_schema()
    except Exception:
        return dict[str, Any]
