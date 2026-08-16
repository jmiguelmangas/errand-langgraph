"""Derive a FastAPI request-body type from a compiled graph's own schema.

No introspection reimplemented here: verified against langgraph 1.2.11 that
``CompiledStateGraph.get_input_schema()`` already builds a Pydantic v2 model
from the graph's ``state_schema`` when it's a ``TypedDict`` -- a
``RootModel``-shaped wrapper whose ``model_dump()`` returns the plain state
dict directly (not wrapped in ``{"root": ...}``), which is exactly the shape
``GraphRunner.submit`` wants. This just calls that and falls back to
``dict[str, Any]`` -- still a valid FastAPI body type, just without
field-level validation -- when the schema isn't introspectable.

**A real, verified footgun, not a hypothetical:** on Python < 3.12, Pydantic
v2 raises ``PydanticUserError`` for a ``state_schema`` built with
``typing.TypedDict`` -- it requires ``typing_extensions.TypedDict`` there
(``typing.TypedDict``'s ``__required_keys__``/``__optional_keys__`` were
buggy before 3.12). Caught a real CI failure from exactly this: a fake test
graph used ``from typing import TypedDict``, introspection silently failed,
and the fallback accepted a body that should have 422'd. The ``warnings.warn``
below exists so a graph author hits a loud, actionable message instead of
quietly losing body validation.
"""

from __future__ import annotations

import warnings
from typing import Any


def build_input_type(graph: Any) -> Any:
    """Return a type usable as a FastAPI request-body annotation for ``graph``.

    A Pydantic model when ``graph.get_input_schema()`` succeeds, else
    ``dict[str, Any]`` -- with a warning, since the most common cause (a
    ``state_schema`` built with ``typing.TypedDict`` on Python < 3.12) is
    both common and silent otherwise.
    """
    try:
        return graph.get_input_schema()
    except Exception as exc:
        warnings.warn(
            f"Could not derive a request-body schema from the graph's "
            f"state_schema ({type(exc).__name__}: {exc}); falling back to "
            f"dict[str, Any] with no field-level validation. On Python < "
            f"3.12, this commonly means the state_schema TypedDict was "
            f"built with `from typing import TypedDict` instead of "
            f"`from typing_extensions import TypedDict`.",
            stacklevel=2,
        )
        return dict[str, Any]
