"""A three-node graph, slow enough that streaming is visibly useful.

No LLM calls -- each node just sleeps, standing in for one.
"""

from __future__ import annotations

import asyncio

from langgraph.graph import END, START, StateGraph

# typing_extensions, not typing: Pydantic v2 needs it for TypedDict schema
# introspection to work on Python < 3.12 -- see errand_langgraph's
# fastapi/schemas.py docstring.
from typing_extensions import NotRequired, TypedDict  # noqa: UP035


class PipelineState(TypedDict):
    topic: str
    outline: NotRequired[str]
    draft: NotRequired[str]
    polished: NotRequired[str]


async def outline(state: PipelineState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"outline": f"Outline for {state['topic']!r}"}


async def draft(state: PipelineState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"draft": f"Draft from: {state['outline']}"}


async def polish(state: PipelineState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"polished": f"Polished: {state['draft']}"}


def build_graph() -> object:
    builder = StateGraph(PipelineState)
    builder.add_node("outline", outline)
    builder.add_node("draft", draft)
    builder.add_node("polish", polish)
    builder.add_edge(START, "outline")
    builder.add_edge("outline", "draft")
    builder.add_edge("draft", "polish")
    builder.add_edge("polish", END)
    return builder
