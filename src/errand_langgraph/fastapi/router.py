"""FastAPI adapter -- the only errand_langgraph module that imports FastAPI.

The import happens lazily, inside :func:`mount_graph`, never at module top
level -- same discipline as ``errand_jobs._fastapi``, so importing
``errand_langgraph`` (or even ``errand_langgraph.fastapi`` itself, since this
module is only reached by calling ``mount_graph``) never requires FastAPI to
be installed.

Route bodies live on :class:`_Routes`, one method per endpoint, so each
one's own error-translation logic reads independently instead of being
five nested closures stacked inside :func:`mount_graph`. Two endpoints
(``submit_run``, ``resume_run``) need a body-parameter type FastAPI can
only learn from the handler's signature, computed at ``mount_graph()`` call
time -- since ``__signature__`` can't be set on a *bound* method (no
``__dict__`` of its own), those two are wrapped in a small plain closure
that the signature override is applied to instead, and the bound method
does the actual work.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ..runner import GraphRunner, RunStatus, UnknownRunError
from .schemas import build_input_type
from .sse import build_sse_response

if TYPE_CHECKING:
    from fastapi import FastAPI


def _resolve_runner(
    graph_or_runner: Any, checkpointer: Any, errand: Any
) -> GraphRunner:
    if isinstance(graph_or_runner, GraphRunner):
        return graph_or_runner
    return GraphRunner(graph_or_runner, checkpointer=checkpointer, errand=errand)


def _bind_signature(fn: Any, params: list[tuple[str, Any, Any]]) -> None:
    """Give a plain function a synthetic signature FastAPI will read as-is."""
    fn.__signature__ = inspect.Signature(
        parameters=[
            inspect.Parameter(
                name,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=annotation,
                default=default,
            )
            for name, annotation, default in params
        ]
    )


@dataclass
class _Routes:
    """Endpoint bodies, bound to one ``GraphRunner``. See ``mount_graph``."""

    runner: GraphRunner

    async def submit_run(self, payload: Any) -> dict[str, Any]:
        from pydantic import BaseModel

        graph_input = (
            payload.model_dump() if isinstance(payload, BaseModel) else payload
        )
        handle = await self.runner.submit(graph_input)
        return {"job_id": handle.job_id, "thread_id": handle.thread_id}

    async def get_run(self, job_id: str) -> dict[str, Any]:
        return _serialize_status(await self._status_or_404(job_id))

    async def resume_run(self, job_id: str, value: Any) -> dict[str, Any]:
        from fastapi import HTTPException

        await self._status_or_404(job_id)
        try:
            handle = await self.runner.resume(job_id, value)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"job_id": handle.job_id, "thread_id": handle.thread_id}

    async def get_thread_state(self, thread_id: str) -> dict[str, Any]:
        return await self.runner.thread_state(thread_id)

    async def get_thread_history(
        self, thread_id: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await self.runner.thread_history(thread_id, limit=limit)

    async def stream_run_events(self, job_id: str) -> Any:
        await self._status_or_404(job_id)
        return build_sse_response(self.runner, job_id)

    async def _status_or_404(self, job_id: str) -> RunStatus:
        from fastapi import HTTPException

        try:
            return await self.runner.status(job_id)
        except UnknownRunError as exc:
            raise HTTPException(status_code=404, detail="Run not found") from exc


def mount_graph(
    app: FastAPI,
    graph_or_runner: Any,
    *,
    prefix: str,
    checkpointer: Any = None,
    errand: Any = None,
) -> GraphRunner:
    """Generate and mount a run-lifecycle router for a graph.

    ``graph_or_runner`` is either a graph (compiled or not, forwarded to
    :class:`~errand_langgraph.GraphRunner` along with ``checkpointer``/
    ``errand``) or an already-constructed ``GraphRunner`` -- pass one
    yourself when you need it before the app exists, e.g. to wire
    ``FastAPI(lifespan=runner.lifespan)`` at construction time (mirrors
    ``errand_jobs``'s own ``FastAPI(lifespan=tasks.lifespan)`` pattern).
    **This function does not start the runner** -- either wire ``lifespan``
    that way, or call ``await runner.startup()`` yourself; otherwise every
    submitted run stays ``queued`` forever with nothing to pick it up (the
    returned runner's own :meth:`~errand_langgraph.GraphRunner.submit` warns
    if you forget).

    Endpoints, mounted under ``prefix``:

    - ``POST {prefix}/runs`` -- body validated against the graph's own
      input schema when introspectable (see ``schemas.py``), 202, returns
      ``{"job_id", "thread_id"}``.
    - ``GET {prefix}/runs/{job_id}`` -- current :class:`RunStatus`, 404 if
      unknown.
    - ``POST {prefix}/runs/{job_id}/resume`` -- body is the raw value
      ``interrupt()`` should return, any JSON shape. 202 with a fresh
      ``{"job_id", "thread_id"}`` (DESIGN.md sec 5: resuming creates a new
      run, never mutates the interrupted one). 404 if ``job_id`` is
      unknown, 409 if it isn't currently ``interrupted``.
    - ``GET {prefix}/threads/{thread_id}/state`` -- current graph state for
      the thread.
    - ``GET {prefix}/threads/{thread_id}/history`` -- checkpoint history,
      newest first, optional ``?limit=``.
    - ``GET {prefix}/runs/{job_id}/events`` -- ``text/event-stream`` of the
      run's graph events as they happen. 404 if ``job_id`` is unknown.
      **In-process only** (DESIGN.md sec 6.3): this only works when the
      client and the worker running the graph share this process. Behind a
      load balancer with multiple worker processes, the SSE request has to
      land on the specific process that owns the run -- not handled here.

    Raises ``ImportError`` with an actionable message (``pip install
    errand-langgraph[fastapi]``) if FastAPI isn't installed.
    """
    try:
        from fastapi import APIRouter, Body
    except ImportError as exc:
        raise ImportError(
            "mount_graph requires FastAPI. Install it with "
            "`pip install errand-langgraph[fastapi]`."
        ) from exc

    runner = _resolve_runner(graph_or_runner, checkpointer, errand)
    routes = _Routes(runner)
    router = APIRouter()

    async def submit_run(payload: Any) -> dict[str, Any]:
        return await routes.submit_run(payload)

    _bind_signature(
        submit_run,
        [("payload", build_input_type(runner._graph), inspect.Parameter.empty)],
    )
    router.add_api_route("/runs", submit_run, methods=["POST"], status_code=202)

    async def resume_run(job_id: str, value: Any) -> dict[str, Any]:
        return await routes.resume_run(job_id, value)

    _bind_signature(
        resume_run,
        [("job_id", str, inspect.Parameter.empty), ("value", Any, Body(...))],
    )
    router.add_api_route(
        "/runs/{job_id}/resume", resume_run, methods=["POST"], status_code=202
    )

    router.add_api_route("/runs/{job_id}", routes.get_run, methods=["GET"])
    router.add_api_route(
        "/threads/{thread_id}/state", routes.get_thread_state, methods=["GET"]
    )
    router.add_api_route(
        "/threads/{thread_id}/history", routes.get_thread_history, methods=["GET"]
    )
    router.add_api_route(
        "/runs/{job_id}/events", routes.stream_run_events, methods=["GET"]
    )

    app.include_router(router, prefix=prefix)
    return runner


def _serialize_status(status: RunStatus) -> dict[str, Any]:
    return {
        "state": status.state.value,
        "thread_id": status.thread_id,
        "result": status.result,
        "interrupt": status.interrupt,
        "error": status.error,
    }
