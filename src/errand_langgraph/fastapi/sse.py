"""Builds the SSE ``StreamingResponse`` for ``GET {prefix}/runs/{job_id}/events``.

Imports FastAPI/Starlette only inside :func:`build_sse_response`, called
from ``router.py`` only after ``mount_graph``'s own lazy import already
succeeded -- never at this module's top level.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from starlette.responses import StreamingResponse

    from ..runner import GraphRunner


def build_sse_response(runner: GraphRunner, job_id: str) -> StreamingResponse:
    """Stream ``job_id``'s events as ``text/event-stream``.

    Each frame is ``data: <json>\\n\\n``, JSON-encoded with FastAPI's own
    ``jsonable_encoder`` -- graph state can hold values plain ``json.dumps``
    doesn't know (LangGraph's ``Interrupt`` dataclass, datetimes, ...), and
    this is the layer that owns turning those into wire format; ``events.py``
    and ``runner.py`` stay framework-agnostic and never see it.
    """
    from fastapi.encoders import jsonable_encoder
    from starlette.responses import StreamingResponse

    async def _generate() -> Any:
        async for event in runner.stream_events(job_id):
            yield f"data: {json.dumps(jsonable_encoder(event))}\n\n"

    return StreamingResponse(_generate(), media_type="text/event-stream")
