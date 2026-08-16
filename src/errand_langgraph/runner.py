"""GraphRunner: execute a LangGraph graph as a single errand task.

Verified against a real compiled graph + ``MemorySaver`` (not assumed from
docs, see DESIGN.md sec 5/sec 7): after ``ainvoke``, ``await
graph.aget_state(config)`` returns a snapshot whose ``.next`` is a non-empty
tuple of node names while an ``interrupt()`` is pending, and empty once the
graph has actually finished. That's the signal :class:`GraphRunner` uses to
tell "interrupted" apart from "succeeded" -- not the ``__interrupt__`` key
some graph outputs carry, which isn't guaranteed to survive across
LangGraph versions.

errand-jobs' own retry (verified against 0.2.1) retries *any* exception up
to ``max_retries`` with no way to classify it -- wrong tool for "resume from
checkpoint" retries (DESIGN.md sec 7, implemented starting M5). The
internal task this module registers therefore always uses
``max_retries=0``: errand attempts it exactly once, and never retries it
itself.

Execution goes through ``graph.astream(..., stream_mode="values")``, not
``ainvoke`` -- verified each chunk in "values" mode is the *full*
accumulated state after that step (not a per-node delta), so the last
chunk is exactly what ``ainvoke`` would have returned, letting one pass
serve both :meth:`GraphRunner.status`'s result and
:meth:`GraphRunner.stream_events`'s events without running the graph
twice.
"""

from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from errand_jobs import Errand, JobStatus
from langgraph.types import Command

from .events import EventBus
from .state import RunIndex, RunRecord, RunState

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@dataclass(frozen=True)
class RunHandle:
    """What :meth:`GraphRunner.submit`/``resume`` return: the two ids that matter.

    ``job_id`` identifies this specific run (pass it to :meth:`GraphRunner.status`).
    ``thread_id`` identifies the LangGraph conversation/case the run belongs
    to -- stable across the initial run and every subsequent resume.
    """

    job_id: str
    thread_id: str


@dataclass(frozen=True)
class RunStatus:
    """A point-in-time snapshot of a run, as returned by :meth:`GraphRunner.status`."""

    state: RunState
    thread_id: str
    result: dict[str, Any] | None
    interrupt: Any | None
    error: str | None


class UnknownRunError(Exception):
    """Raised by :meth:`GraphRunner.status` for a ``job_id`` it never issued."""


def _is_compiled(graph: Any) -> bool:
    # Duck-typed, not an isinstance check against a langgraph internal
    # class path -- CompiledStateGraph/Pregel's exact module has moved
    # before and the version range this package supports is wide
    # (langgraph>=0.2). ainvoke is the one thing a compiled graph is
    # guaranteed to have and an uncompiled StateGraph builder never does.
    return hasattr(graph, "ainvoke")


def _extract_interrupts(snapshot: Any) -> list[dict[str, Any]] | None:
    interrupts = [
        {"id": interrupt.id, "value": interrupt.value}
        for task in snapshot.tasks
        for interrupt in task.interrupts
    ]
    return interrupts or None


class GraphRunner:
    """Runs a LangGraph graph as a background errand job.

    Example::

        runner = GraphRunner(graph, checkpointer=checkpointer)
        await runner.startup()
        handle = await runner.submit({"messages": [("user", "hi")]})
        status = await runner.status(handle.job_id)
        # RunStatus(state=RunState.RUNNING, ...)
        await runner.shutdown()

    Accepts either an already-compiled graph (``checkpointer`` must then be
    ``None`` -- configure it via ``graph.compile(checkpointer=...)``
    instead) or an uncompiled ``StateGraph`` builder, which is compiled here
    with the given ``checkpointer``.

    Registers exactly one internal task on ``errand`` (a fresh
    ``errand_jobs.Errand()`` by default, or the instance you pass -- share
    one across multiple ``GraphRunner``s to share a worker pool). Owns no
    worker pool of its own: :meth:`startup`/:meth:`shutdown`/:meth:`lifespan`
    just delegate to it.
    """

    def __init__(
        self,
        graph: Any,
        *,
        checkpointer: Any = None,
        errand: Errand | None = None,
    ) -> None:
        if _is_compiled(graph):
            if checkpointer is not None:
                raise ValueError(
                    "graph is already compiled; pass checkpointer to "
                    "graph.compile(checkpointer=...) instead of GraphRunner(...)."
                )
            self._graph = graph
        else:
            self._graph = graph.compile(checkpointer=checkpointer)

        self._index = RunIndex()
        self._events = EventBus()
        self._errand = errand if errand is not None else Errand()
        self._task_name = f"errand_langgraph.run.{uuid4().hex}"
        self._started = False

        # A plain closure, not the bound method directly: errand's @task
        # sets an attribute on the callable it registers, which bound
        # methods don't allow (no __dict__ of their own).
        async def _task_entry(
            *, job_id: str, payload: Any, thread_id: str
        ) -> dict[str, Any]:
            return await self._run_graph(
                job_id=job_id, payload=payload, thread_id=thread_id
            )

        self._errand.task(name=self._task_name, max_retries=0)(_task_entry)

    async def submit(
        self, graph_input: dict[str, Any], *, thread_id: str | None = None
    ) -> RunHandle:
        """Enqueue a graph run and return immediately.

        ``thread_id`` defaults to a fresh id -- pass one explicitly to
        continue an existing LangGraph conversation from its initial state
        rather than starting a new one (resuming after an ``interrupt()``
        uses :meth:`resume` instead, not a second call to this method).
        """
        self._warn_if_not_started("submit")
        resolved_thread_id = thread_id if thread_id is not None else uuid4().hex
        return await self._enqueue_run(
            thread_id=resolved_thread_id, payload=graph_input, graph_input=graph_input
        )

    async def resume(self, job_id: str, value: Any) -> RunHandle:
        """Resume the interrupted run ``job_id`` with ``value``.

        ``value`` becomes the return value of the ``interrupt()`` call the
        graph is paused on. Creates a **new** run on the same ``thread_id``
        rather than mutating ``job_id``'s record (DESIGN.md sec 5: job
        history stays immutable and auditable).

        Raises :class:`UnknownRunError` if ``job_id`` is unknown, or
        :class:`ValueError` if that run isn't currently ``interrupted``.
        """
        self._warn_if_not_started("resume")
        record = self._index.get(job_id)
        if record is None:
            raise UnknownRunError(job_id)
        if record.state != RunState.INTERRUPTED:
            raise ValueError(
                f"run {job_id!r} is not interrupted (state={record.state.value}); "
                "only an interrupted run can be resumed"
            )
        return await self._enqueue_run(
            thread_id=record.thread_id, payload=Command(resume=value), graph_input=None
        )

    async def _enqueue_run(
        self, *, thread_id: str, payload: Any, graph_input: dict[str, Any] | None
    ) -> RunHandle:
        job_id = uuid4().hex
        self._index.create(job_id=job_id, thread_id=thread_id, graph_input=graph_input)
        job = self._errand.enqueue(
            self._task_name, job_id=job_id, payload=payload, thread_id=thread_id
        )
        record = self._index.get(job_id)
        assert record is not None
        record.errand_job_id = job.id
        return RunHandle(job_id=job_id, thread_id=thread_id)

    def _warn_if_not_started(self, method: str) -> None:
        if self._started:
            return
        warnings.warn(
            f"GraphRunner.{method}() called before startup() -- the run "
            "will stay queued forever with no worker to pick it up. "
            "Call `await runner.startup()`, or pass `lifespan=runner.lifespan` "
            "to FastAPI(...) if you're using mount_graph().",
            stacklevel=3,
        )

    async def status(self, job_id: str) -> RunStatus:
        """Return the current status of the run identified by ``job_id``.

        Raises :class:`UnknownRunError` if ``job_id`` was never issued by
        this runner's :meth:`submit`/:meth:`resume`.
        """
        record = self._index.get(job_id)
        if record is None:
            raise UnknownRunError(job_id)
        await self._sync_queued_record(record)
        return RunStatus(
            state=record.state,
            thread_id=record.thread_id,
            result=record.result,
            interrupt=record.interrupt,
            error=record.error,
        )

    def stream_events(self, job_id: str) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to ``job_id``'s stream of graph events (DESIGN.md sec 6.3).

        Yields buffered-then-live events published by :meth:`_run_graph` as
        the graph executes, and returns once the run is over -- whatever
        state it ended in. Each event is ``{"seq", "type", "data"}``, where
        ``data`` is a full state snapshot (langgraph's ``stream_mode="values"``).

        Doesn't validate that ``job_id`` is known -- callers that want a 404
        for an unknown job (like ``mount_graph``'s SSE route) should call
        :meth:`status` first. A ``job_id`` that's simply queued and hasn't
        produced its first event yet is not an error: the subscriber just
        waits, same as one that arrives mid-run.
        """
        return self._events.subscribe(job_id)

    async def thread_state(self, thread_id: str) -> dict[str, Any]:
        """Return the graph's current state for ``thread_id``.

        ``{"values", "next", "interrupt"}`` -- straight from
        ``graph.aget_state``, the same source :meth:`_run_graph` uses to
        detect an interrupt. A ``thread_id`` this runner never submitted to
        isn't an error (LangGraph itself doesn't distinguish "unknown
        thread" from "empty thread"): you get back empty values and no next
        steps, same as a thread that hasn't started running yet.
        """
        snapshot = await self._graph.aget_state(
            {"configurable": {"thread_id": thread_id}}
        )
        return {
            "values": snapshot.values,
            "next": list(snapshot.next),
            "interrupt": _extract_interrupts(snapshot),
        }

    async def thread_history(
        self, thread_id: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return ``thread_id``'s checkpoint history, newest first.

        Each entry is ``{"checkpoint_id", "values", "next"}``, straight from
        ``graph.aget_state_history``.
        """
        config = {"configurable": {"thread_id": thread_id}}
        history = []
        async for snapshot in self._graph.aget_state_history(config, limit=limit):
            history.append(
                {
                    "checkpoint_id": snapshot.config["configurable"]["checkpoint_id"],
                    "values": snapshot.values,
                    "next": list(snapshot.next),
                }
            )
        return history

    async def _sync_queued_record(self, record: RunRecord) -> None:
        # The task function itself drives record.state from RUNNING onward
        # (see _run_graph) -- the one gap is a job that never got to run at
        # all, e.g. cancelled by errand during shutdown drain while still
        # queued. Only worth reconciling against errand's Job in that one
        # case; once our own task function has touched the record, it's
        # the more specific and more current source of truth.
        if record.state != RunState.QUEUED or record.errand_job_id is None:
            return
        job = await self._errand.get_job(record.errand_job_id)
        if job is not None and job.status == JobStatus.FAILED:
            record.state = RunState.FAILED
            record.error = job.error

    async def _run_graph(
        self, *, job_id: str, payload: Any, thread_id: str
    ) -> dict[str, Any]:
        record = self._index.get(job_id)
        assert record is not None
        record.state = RunState.RUNNING

        config = {"configurable": {"thread_id": thread_id}}
        last_values: dict[str, Any] = {}
        try:
            async for chunk in self._graph.astream(
                payload, config=config, stream_mode="values"
            ):
                last_values = chunk
                self._events.publish(job_id, {"type": "values", "data": chunk})
        except Exception as exc:
            record.state = RunState.FAILED
            record.error = f"{type(exc).__name__}: {exc}"
            self._events.close(job_id)
            raise

        snapshot = await self._graph.aget_state(config)
        if snapshot.next:
            record.state = RunState.INTERRUPTED
            record.interrupt = _extract_interrupts(snapshot)
        else:
            record.state = RunState.SUCCEEDED
            record.result = {
                k: v for k, v in last_values.items() if k != "__interrupt__"
            }
        self._events.close(job_id)
        return last_values

    async def startup(self) -> None:
        """Start the underlying errand worker pool. Call once, before submitting."""
        await self._errand.startup()
        self._started = True

    async def shutdown(self) -> None:
        """Stop the underlying errand worker pool, draining in-flight runs."""
        await self._errand.shutdown()
        self._started = False

    @asynccontextmanager
    async def lifespan(self, app: Any) -> AsyncIterator[None]:
        """Pass to ``FastAPI(lifespan=runner.lifespan)`` to wire startup/shutdown."""
        await self.startup()
        try:
            yield
        finally:
            await self.shutdown()
