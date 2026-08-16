from __future__ import annotations

import asyncio

import pytest
from conftest import (
    build_approval_graph,
    build_counter_graph,
    build_failing_graph,
    build_flaky_graph,
)
from errand_jobs.models import Job, JobStatus

from errand_langgraph import GraphRunner, RunState, UnknownRunError
from errand_langgraph.retry import RetryPolicy
from errand_langgraph.runner import RunStatus, _extract_interrupts, _is_compiled

_FAST_RETRY = RetryPolicy(max_attempts=3, base_delay=0.001, max_delay=0.01)


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


async def test_submit_before_startup_warns() -> None:
    runner = GraphRunner(build_counter_graph())
    with pytest.warns(UserWarning, match="called before startup"):
        await runner.submit({"value": 1})


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


async def test_full_hitl_cycle_invoke_interrupt_resume_succeeded() -> None:
    # The integration test DESIGN.md sec 8 asks for: a real compiled graph
    # + InMemorySaver, not mocked at the LangGraph layer.
    runner = GraphRunner(build_approval_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        interrupted = await _wait_for(runner, handle.job_id, RunState.INTERRUPTED)
        assert interrupted.interrupt is not None

        resumed = await runner.resume(handle.job_id, True)
        assert resumed.thread_id == handle.thread_id
        assert resumed.job_id != handle.job_id

        final = await _wait_for(runner, resumed.job_id, RunState.SUCCEEDED)
        assert final.result == {"value": 4, "approved": True}

        # The original job's own record is untouched -- history stays
        # immutable (DESIGN.md sec 5), the resume created a new job instead.
        original = await runner.status(handle.job_id)
        assert original.state == RunState.INTERRUPTED
    finally:
        await runner.shutdown()


async def test_resume_unknown_job_id_raises() -> None:
    runner = GraphRunner(build_approval_graph())
    with pytest.raises(UnknownRunError):
        await runner.resume("does-not-exist", True)


async def test_resume_a_non_interrupted_run_raises() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)

        with pytest.raises(ValueError, match="not interrupted"):
            await runner.resume(handle.job_id, True)
    finally:
        await runner.shutdown()


async def test_resume_before_startup_warns() -> None:
    runner = GraphRunner(build_approval_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        await _wait_for(runner, handle.job_id, RunState.INTERRUPTED)
    finally:
        await runner.shutdown()

    with pytest.warns(UserWarning, match="called before startup"):
        await runner.resume(handle.job_id, True)


async def test_thread_state_reflects_interrupted_graph() -> None:
    runner = GraphRunner(build_approval_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        await _wait_for(runner, handle.job_id, RunState.INTERRUPTED)

        state = await runner.thread_state(handle.thread_id)
        assert state["values"] == {"value": 2, "approved": False}
        assert state["next"] == ["ask_for_approval"]
        assert state["interrupt"][0]["value"] == {"question": "approve?", "value": 2}
    finally:
        await runner.shutdown()


async def test_thread_state_for_unknown_thread_is_empty() -> None:
    runner = GraphRunner(build_counter_graph())
    state = await runner.thread_state("never-used-thread")
    assert state == {"values": {}, "next": [], "interrupt": None}


async def test_thread_history_lists_checkpoints_newest_first() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1}, thread_id="t-history")
        await _wait_for(runner, handle.job_id, RunState.SUCCEEDED)

        history = await runner.thread_history(handle.thread_id)
        assert len(history) >= 2
        assert history[0]["values"] == {"value": 2}
        assert all("checkpoint_id" in entry for entry in history)

        limited = await runner.thread_history(handle.thread_id, limit=1)
        assert len(limited) == 1
    finally:
        await runner.shutdown()


async def test_stream_events_yields_values_chunks_then_ends() -> None:
    runner = GraphRunner(build_counter_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})

        events = [event async for event in runner.stream_events(handle.job_id)]

        assert [e["data"] for e in events] == [{"value": 1}, {"value": 2}]
        assert [e["type"] for e in events] == ["values", "values"]
        status = await runner.status(handle.job_id)
        assert status.state == RunState.SUCCEEDED
    finally:
        await runner.shutdown()


async def test_stream_events_closes_on_failure() -> None:
    runner = GraphRunner(build_failing_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})

        events = [event async for event in runner.stream_events(handle.job_id)]

        # boom's node never emits a values chunk of its own -- only the
        # initial input state does, before the exception.
        assert [e["data"] for e in events] == [{"value": 1}]
        status = await runner.status(handle.job_id)
        assert status.state == RunState.FAILED
    finally:
        await runner.shutdown()


async def test_stream_events_closes_on_interrupt() -> None:
    runner = GraphRunner(build_approval_graph())
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})

        events = [event async for event in runner.stream_events(handle.job_id)]

        assert events[-1]["data"]["value"] == 2
        status = await runner.status(handle.job_id)
        assert status.state == RunState.INTERRUPTED
    finally:
        await runner.shutdown()


async def test_retry_resumes_from_checkpoint_without_rerunning_completed_nodes() -> (
    None
):
    graph, call_counts = build_flaky_graph(fail_times=2)
    runner = GraphRunner(graph, retry=_FAST_RETRY)
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(
            runner, handle.job_id, RunState.SUCCEEDED, RunState.FAILED
        )

        assert status.state == RunState.SUCCEEDED
        assert status.result == {"value": 102}
        # increment ran once, ever -- resume-from-checkpoint means it's
        # never re-executed across the 2 retries flaky needed.
        assert call_counts["increment"] == 1
        assert call_counts["flaky"] == 3
    finally:
        await runner.shutdown()


async def test_retry_without_checkpointer_restarts_from_scratch() -> None:
    graph, call_counts = build_flaky_graph(fail_times=2, with_checkpointer=False)
    with pytest.warns(UserWarning, match="no checkpointer"):
        runner = GraphRunner(graph, retry=_FAST_RETRY)
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(
            runner, handle.job_id, RunState.SUCCEEDED, RunState.FAILED
        )

        assert status.state == RunState.SUCCEEDED
        # No checkpoint to resume from -- every attempt re-sends the
        # original payload, so increment re-runs each time too.
        assert call_counts["increment"] == 3
        assert call_counts["flaky"] == 3
    finally:
        await runner.shutdown()


async def test_retry_exhausted_marks_failed() -> None:
    graph, call_counts = build_flaky_graph(fail_times=10)
    runner = GraphRunner(
        graph, retry=RetryPolicy(max_attempts=2, base_delay=0.001, max_delay=0.01)
    )
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(runner, handle.job_id, RunState.FAILED)

        assert status.state == RunState.FAILED
        assert status.error is not None
        assert "TimeoutError" in status.error
        assert call_counts["flaky"] == 2  # exactly max_attempts, not more
    finally:
        await runner.shutdown()


async def test_retry_publishes_a_retry_event() -> None:
    graph, _call_counts = build_flaky_graph(fail_times=1)
    runner = GraphRunner(graph, retry=_FAST_RETRY)
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})

        events = [event async for event in runner.stream_events(handle.job_id)]

        retry_events = [e for e in events if e["type"] == "retry"]
        assert len(retry_events) == 1
        assert retry_events[0]["data"]["attempt"] == 1
        assert "TimeoutError" in retry_events[0]["data"]["error"]
    finally:
        await runner.shutdown()


async def test_non_retryable_exception_fails_on_first_attempt() -> None:
    runner = GraphRunner(build_failing_graph(), retry=_FAST_RETRY)
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1})
        status = await _wait_for(runner, handle.job_id, RunState.FAILED)
        assert status.state == RunState.FAILED
    finally:
        await runner.shutdown()


def test_no_checkpointer_with_retries_enabled_warns() -> None:
    with pytest.warns(UserWarning, match="no checkpointer"):
        GraphRunner(build_counter_graph(with_checkpointer=False))


def test_no_checkpointer_with_retries_disabled_does_not_warn() -> None:
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        GraphRunner(
            build_counter_graph(with_checkpointer=False),
            retry=RetryPolicy(max_attempts=1),
        )


async def test_interrupt_detection_without_a_checkpointer() -> None:
    # aget_state needs a checkpointer (verified -- ValueError("No
    # checkpointer set") otherwise), so without one, interrupt detection
    # falls back to the __interrupt__ key astream's own chunks carry.
    with pytest.warns(UserWarning, match="no checkpointer"):
        runner = GraphRunner(build_approval_graph(with_checkpointer=False))
    await runner.startup()
    try:
        handle = await runner.submit({"value": 1, "approved": False})
        status = await _wait_for(runner, handle.job_id, RunState.INTERRUPTED)

        assert status.result is None
        assert status.interrupt[0]["value"] == {"question": "approve?", "value": 2}
    finally:
        await runner.shutdown()
