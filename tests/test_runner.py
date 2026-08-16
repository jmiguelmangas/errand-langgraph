from __future__ import annotations

import asyncio

import pytest
from conftest import build_approval_graph, build_counter_graph, build_failing_graph
from errand_jobs.models import Job, JobStatus

from errand_langgraph import GraphRunner, RunState, UnknownRunError
from errand_langgraph.runner import RunStatus, _extract_interrupts, _is_compiled


async def _wait_for(
    runner: GraphRunner, job_id: str, *states: RunState, within_seconds: float = 10.0
) -> RunStatus:
    try:
        async with asyncio.timeout(within_seconds):
            while True:
                status = await runner.status(job_id)
                if status.state in states:
                    return status
                await asyncio.sleep(0.01)
    except TimeoutError:
        raise AssertionError(f"job {job_id} did not reach {states} in time") from None


async def test_submit_succeeds() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)
        assert status.state == RunState.SUCCEEDED
        assert status.thread_id == handle.thread_id
        assert status.result == {"value": 2}
        assert status.interrupt is None
        assert status.error is None
    finally:
        await runner.shutdown()


async def test_submit_reports_running_before_terminal() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await runner.status(handle.job_id)
        assert status.state in (RunState.QUEUED, RunState.RUNNING, RunState.SUCCEEDED)
        await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)
    finally:
        await runner.shutdown()


async def test_submit_reports_interrupted() -> None:
    runner = GraphRunner(build_approval_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        status = await _wait_for(runner, handle.job_id, RunState.INTERRUPTED)
        assert status.result is None
        assert isinstance(status.interrupt, list)
        assert len(status.interrupt) == 1
        assert status.interrupt[0]["value"] == {"question": "approve?", "value": 2}
        assert isinstance(status.interrupt[0]["id"], str)
        assert status.interrupt[0]["id"]
    finally:
        await runner.shutdown()


async def test_submit_reports_failed() -> None:
    runner = GraphRunner(build_failing_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        status = await _wait_for(runner, handle.job_id, RunState.FAILED)
        assert status.result is None
        assert status.error is not None
        assert "boom" in status.error
    finally:
        await runner.shutdown()


async def test_submit_uses_given_thread_id() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1}, thread_id="t-fixed")
        assert handle.thread_id == "t-fixed"
        await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)
    finally:
        await runner.shutdown()


async def test_status_unknown_job_id_raises() -> None:
    runner = GraphRunner(build_counter_graph())
    with pytest.raises(UnknownRunError):
        await runner.status("does-not-exist")


async def test_compiled_graph_with_checkpointer_kwarg_raises() -> None:
    with pytest.raises(ValueError, match="already compiled"):
        GraphRunner(build_counter_graph(), checkpointer=object())


async def test_uncompiled_graph_is_compiled_with_given_checkpointer() -> None:
    from conftest import CounterState
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import END, START, StateGraph

    def increment(state: CounterState) -> dict[str, int]:
        return {"value": state["value"] + 1}

    builder = StateGraph(CounterState)
    builder.add_node("increment", increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)

    runner = GraphRunner(builder, checkpointer=InMemorySaver())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)
        assert status.result == {"value": 2}
    finally:
        await runner.shutdown()


async def test_lifespan_delegates_to_errand() -> None:
    runner = GraphRunner(build_counter_graph())
    async with runner.lifespan(None):
        handle = await runner.submit({"value": 1})
        await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)


def test_is_compiled_duck_typing() -> None:
    assert _is_compiled(build_counter_graph()) is True
    assert _is_compiled(object()) is False


def test_extract_interrupts_returns_none_when_empty() -> None:
    class _Snapshot:
        tasks: tuple[object, ...] = ()

    assert _extract_interrupts(_Snapshot()) is None


async def test_status_reconciles_a_job_shutdown_marked_failed_before_running() -> None:
    # errand's own drain marks anything still queued at shutdown FAILED
    # (see errand_jobs.runner.Runner.stop) without our task function ever
    # touching the RunRecord -- the one case _sync_queued_record exists for.
    runner = GraphRunner(build_counter_graph())
    failed_job = Job(
        name="irrelevant", status=JobStatus.FAILED, error="Cancelled during shutdown"
    )
    await runner._errand._store.create(failed_job)
    record = runner._index.create(job_id="r-1", thread_id="t-1", graph_input=None)
    record.errand_job_id = failed_job.id

    status = await runner.status("r-1")

    assert status.state == RunState.FAILED
    assert status.error == "Cancelled during shutdown"
