"""FastAPI adapter -- the only errand_langgraph module that imports FastAPI.

The import happens lazily, inside :func:`mount_graph`, never at module top
level -- same discipline as ``errand_jobs._fastapi``, so importing
``errand_langgraph`` (or even ``errand_langgraph.fastapi`` itself, since this
module is only reached by calling ``mount_graph``) never requires FastAPI to
be installed.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from ..runner import GraphRunner, RunStatus, UnknownRunError
from .schemas import build_input_type

if TYPE_CHECKING:
    from fastapi import FastAPI


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

    Raises ``ImportError`` with an actionable message (``pip install
    errand-langgraph[fastapi]``) if FastAPI isn't installed.
    """
    try:
        from fastapi import APIRouter, Body, HTTPException
        from pydantic import BaseModel
    except ImportError as exc:
        raise ImportError(
            "mount_graph requires FastAPI. Install it with "
            "`pip install errand-langgraph[fastapi]`."
        ) from exc

    runner = (
        graph_or_runner
        if isinstance(graph_or_runner, GraphRunner)
        else GraphRunner(graph_or_runner, checkpointer=checkpointer, errand=errand)
    )
    input_type = build_input_type(runner._graph)

    router = APIRouter()

    async def submit_run(payload: Any) -> dict[str, Any]:
        graph_input = (
            payload.model_dump() if isinstance(payload, BaseModel) else payload
        )
        handle = await runner.submit(graph_input)
        return {"job_id": handle.job_id, "thread_id": handle.thread_id}

    # FastAPI reads the body type off the function's own signature, which
    # can't be written statically here since input_type is only known at
    # mount_graph() call time -- verified this override is honoured for
    # both a Pydantic model and a plain dict[str, Any] fallback.
    submit_run.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        parameters=[
            inspect.Parameter(
                "payload",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=input_type,
            )
        ]
    )
    router.add_api_route("/runs", submit_run, methods=["POST"], status_code=202)

    @router.get("/runs/{job_id}")
    async def get_run(job_id: str) -> dict[str, Any]:
        try:
            status = await runner.status(job_id)
        except UnknownRunError as exc:
            raise HTTPException(status_code=404, detail="Run not found") from exc
        return _serialize_status(status)

    @router.post("/runs/{job_id}/resume", status_code=202)
    async def resume_run(job_id: str, value: Any = Body(...)) -> dict[str, Any]:
        try:
            handle = await runner.resume(job_id, value)
        except UnknownRunError as exc:
            raise HTTPException(status_code=404, detail="Run not found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"job_id": handle.job_id, "thread_id": handle.thread_id}

    @router.get("/threads/{thread_id}/state")
    async def get_thread_state(thread_id: str) -> dict[str, Any]:
        return await runner.thread_state(thread_id)

    @router.get("/threads/{thread_id}/history")
    async def get_thread_history(
        thread_id: str, limit: int | None = None
    ) -> list[dict[str, Any]]:
        return await runner.thread_history(thread_id, limit=limit)

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
