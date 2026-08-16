"""Fake graph builders shared across tests.

Plain Python node functions only -- no LLM/network calls, per CLAUDE.md.
The point of these graphs is to exercise LangGraph's own
execution/checkpoint/interrupt machinery, not any model.
"""

from __future__ import annotations

from typing import Any

# typing_extensions, not typing: Pydantic v2 requires
# typing_extensions.TypedDict on Python < 3.12 for schema introspection to
# work at all (see fastapi/schemas.py's module docstring -- a real CI
# failure caught this, not a guess).
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import NotRequired, TypedDict  # noqa: UP035


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


def build_approval_graph(*, with_checkpointer: bool = True) -> Any:
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
    checkpointer = InMemorySaver() if with_checkpointer else None
    return builder.compile(checkpointer=checkpointer)


def build_failing_graph() -> Any:
    """A node that always raises -- for exercising the FAILED path."""

    def boom(state: CounterState) -> dict[str, int]:
        raise RuntimeError("boom")

    builder = StateGraph(CounterState)
    builder.add_node("boom", boom)
    builder.add_edge(START, "boom")
    builder.add_edge("boom", END)
    return builder.compile(checkpointer=InMemorySaver())


def build_flaky_graph(
    *, fail_times: int, with_checkpointer: bool = True
) -> tuple[Any, dict[str, int]]:
    """increment (always succeeds) -> flaky (raises a retryable TimeoutError
    the first ``fail_times`` calls, then succeeds).

    Returns ``(graph, call_counts)`` -- ``call_counts`` is mutated as nodes
    run, so a test can assert resume-from-checkpoint doesn't re-run
    ``increment`` on retry (per CLAUDE.md's testing rules: assert call
    counts, not just the final result).
    """
    call_counts = {"increment": 0, "flaky": 0}

    def increment(state: CounterState) -> dict[str, int]:
        call_counts["increment"] += 1
        return {"value": state["value"] + 1}

    def flaky(state: CounterState) -> dict[str, int]:
        call_counts["flaky"] += 1
        if call_counts["flaky"] <= fail_times:
            raise TimeoutError("simulated transient failure")
        return {"value": state["value"] + 100}

    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_node("flaky", flaky)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", "flaky")
    builder.add_edge("flaky", END)
    checkpointer = InMemorySaver() if with_checkpointer else None
    return builder.compile(checkpointer=checkpointer), call_counts
