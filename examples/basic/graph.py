"""A toy two-node graph -- no LLM calls, just enough to show the shape.

Simulates the kind of work that makes background execution worth it: each
node sleeps briefly, standing in for an LLM call or a slow tool.
"""

from __future__ import annotations

import asyncio
from typing import Any, NotRequired, TypedDict

from langgraph.graph import END, START, StateGraph


class AgentState(TypedDict):
    topic: str
    # Filled in by the graph's own nodes -- not required from the caller.
    draft: NotRequired[str]
    review: NotRequired[str]


async def write_draft(state: AgentState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"draft": f"Draft about {state['topic']!r}."}


async def review_draft(state: AgentState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"review": f"Reviewed: {state['draft']} Looks good."}


def build_graph() -> Any:
    builder = StateGraph(AgentState)
    builder.add_node("write_draft", write_draft)
    builder.add_node("review_draft", review_draft)
    builder.add_edge(START, "write_draft")
    builder.add_edge("write_draft", "review_draft")
    builder.add_edge("review_draft", END)
    return builder
