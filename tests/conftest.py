"""Fake graph builders shared across tests.

Plain Python node functions only -- no LLM/network calls, per CLAUDE.md.
The point of these graphs is to exercise LangGraph's own
execution/checkpoint/interrupt machinery, not any model.
"""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class CounterState(TypedDict):
    value: int
    # Only the approval graph's nodes touch this -- NotRequired so the
    # counter/failing graphs' schema-derived request bodies don't demand it.
    approved: NotRequired[bool]


def build_counter_graph(*, with_checkpointer: bool = True) -> Any:
    """A one-node graph: value -> value + 1. No interrupts, no failures."""

    def increment(state: CounterState) -> dict[str, int]:
        return {"value": state["value"] + 1}

    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    checkpointer = InMemorySaver() if with_checkpointer else None
    return builder.compile(checkpointer=checkpointer)


def build_approval_graph() -> Any:
    """value -> +1 -> interrupt() waiting for approval -> value * 2 if approved."""

    def increment(state: CounterState) -> dict[str, int]:
        return {"value": state["value"] + 1}

    def ask_for_approval(state: CounterState) -> dict[str, bool]:
        approved = interrupt({"question": "approve?", "value": state["value"]})
        return {"approved": approved}

    def apply_decision(state: CounterState) -> dict[str, int]:
        if not state["approved"]:
            return {}
        return {"value": state["value"] * 2}

    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_node("ask_for_approval", ask_for_approval)
    builder.add_node("apply_decision", apply_decision)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", "ask_for_approval")
    builder.add_edge("ask_for_approval", "apply_decision")
    builder.add_edge("apply_decision", END)
    return builder.compile(checkpointer=InMemorySaver())


def build_failing_graph() -> Any:
    """A node that always raises -- for exercising the FAILED path."""

    def boom(state: CounterState) -> dict[str, int]:
        raise RuntimeError("boom")

    builder = StateGraph(CounterState)
    builder.add_node("boom", boom)
    builder.add_edge(START, "boom")
    builder.add_edge("boom", END)
    return builder.compile(checkpointer=InMemorySaver())
