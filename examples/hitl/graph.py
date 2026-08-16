"""A two-node graph that pauses for human approval mid-run.

No LLM calls -- `draft_message` stands in for one, `interrupt()` is the
actual point of this example.
"""

from __future__ import annotations

import asyncio

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

# typing_extensions, not typing: Pydantic v2 needs it for TypedDict schema
# introspection to work on Python < 3.12 -- see errand_langgraph's
# fastapi/schemas.py docstring.
from typing_extensions import NotRequired, TypedDict  # noqa: UP035


class ApprovalState(TypedDict):
    topic: str
    message: NotRequired[str]
    approved: NotRequired[bool]
    sent: NotRequired[bool]


async def draft_message(state: ApprovalState) -> dict[str, str]:
    await asyncio.sleep(1)
    return {"message": f"Hi! Quick update on {state['topic']!r}."}


def request_approval(state: ApprovalState) -> dict[str, bool]:
    approved = interrupt({"question": "send this?", "message": state["message"]})
    return {"approved": approved}


async def send_message(state: ApprovalState) -> dict[str, bool]:
    if not state["approved"]:
        return {"sent": False}
    await asyncio.sleep(1)
    return {"sent": True}


def build_graph() -> object:
    builder = StateGraph(ApprovalState)
    builder.add_node("draft_message", draft_message)
    builder.add_node("request_approval", request_approval)
    builder.add_node("send_message", send_message)
    builder.add_edge(START, "draft_message")
    builder.add_edge("draft_message", "request_approval")
    builder.add_edge("request_approval", "send_message")
    builder.add_edge("send_message", END)
    return builder
