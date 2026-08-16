# errand-langgraph

Run [LangGraph](https://github.com/langchain-ai/langgraph) graphs as
[errand](https://github.com/jmiguelmangas/errand) jobs: background execution
with status polling, human-in-the-loop resume, and an auto-generated FastAPI
router — no Celery, no separate broker.

> **Status:** early development, not yet released to PyPI. 0.1 (submit/status
> + the FastAPI router) and 0.2 (human-in-the-loop resume) are implemented;
> streaming and smart retries are in progress — see the roadmap below.

**Requires Python 3.11+.** `interrupt()` is broken under Python 3.10 in
recent `langgraph` releases (a real, verified upstream bug, not a guess —
`RuntimeError: Called get_config outside of a runnable context`, reproduced
in plain `langgraph` with no `errand-langgraph` involved). Since
human-in-the-loop is this package's flagship feature, 3.10 isn't supported.

**Define your graph's state with `typing_extensions.TypedDict`, not
`typing.TypedDict`.** On Python < 3.12, Pydantic v2 can't introspect a
`typing.TypedDict` (`PydanticUserError`) — `mount_graph`'s request-body
validation silently falls back to an unvalidated `dict[str, Any]` if you get
this wrong (with a warning telling you why). `from typing_extensions import
TypedDict, NotRequired` avoids it entirely and works the same on every
supported Python version.

## Install

```bash
pip install errand-langgraph[fastapi]
```

## Why

A LangGraph agent run can take anywhere from a few seconds to several
minutes. `BackgroundTasks` in FastAPI gives you no state and no way to poll
progress; Celery gives you a broker and a deploy story you probably don't
need for a single machine. `errand` is the middle ground for background jobs
in general — this package specializes it for graphs: submit a run, poll its
status, resume it after an `interrupt()`, stream its events, and get retries
that resume from the last checkpoint instead of re-running the whole graph
(and re-paying for every LLM call along the way).

## Quickstart

```python
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver

from errand_langgraph import GraphRunner
from errand_langgraph.fastapi import mount_graph

from your_agent import graph  # an uncompiled StateGraph, or a compiled one

runner = GraphRunner(graph, checkpointer=InMemorySaver())
app = FastAPI(lifespan=runner.lifespan)  # starts/drains the worker pool
mount_graph(app, runner, prefix="/agent")
```

This mounts five endpoints:

| Method | Path | What |
|---|---|---|
| `POST` | `/agent/runs` | Submit a run. Body is validated against the graph's own input schema when it's introspectable. Returns `{"job_id", "thread_id"}`, 202. |
| `GET` | `/agent/runs/{job_id}` | Current status: `{"state", "thread_id", "result", "interrupt", "error"}`. `state` is one of `queued`, `running`, `succeeded`, `failed`, `interrupted`. 404 if unknown. |
| `POST` | `/agent/runs/{job_id}/resume` | Resume an `interrupted` run. Body is the raw value `interrupt()` should return (any JSON). Returns a **new** `{"job_id", "thread_id"}` on the same thread, 202. 404 if `job_id` is unknown, 409 if it isn't `interrupted`. |
| `GET` | `/agent/threads/{thread_id}/state` | Current graph state for the thread: `{"values", "next", "interrupt"}`. |
| `GET` | `/agent/threads/{thread_id}/history` | Checkpoint history, newest first, optional `?limit=`. |

```bash
curl -X POST localhost:8000/agent/runs -d '{"messages": [["user", "hi"]]}'
# {"job_id": "...", "thread_id": "..."}

curl localhost:8000/agent/runs/<job_id>
# {"state": "succeeded", "result": {...}, ...}
# or, if a node called interrupt():
# {"state": "interrupted", "interrupt": [{"id": "...", "value": {...}}], ...}

curl -X POST localhost:8000/agent/runs/<job_id>/resume -d 'true'
# {"job_id": "<new job id>", "thread_id": "<same thread>"}
```

Without FastAPI, drive the same thing directly:

```python
from errand_langgraph import GraphRunner

runner = GraphRunner(graph, checkpointer=InMemorySaver())
await runner.startup()
handle = await runner.submit({"messages": [("user", "hi")]})
status = await runner.status(handle.job_id)
# after an interrupt:
resumed = await runner.resume(handle.job_id, value=True)
await runner.shutdown()
```

See `examples/basic/` for a runnable submit/poll version and
`examples/hitl/` for the full interrupt/resume cycle (server + client, no
LLM calls, both actually run end to end).

**Don't forget startup.** Nothing runs submitted work without a running
worker pool — wire `lifespan=runner.lifespan` into `FastAPI(...)` (as above)
or call `await runner.startup()` yourself before submitting. `submit()`/
`resume()` warn if you forget.

## Roadmap

- **0.1 (done):** `GraphRunner.submit`/`status`, `mount_graph` with polling.
- **0.2 (done):** human-in-the-loop — `interrupt()` detection, `resume()`,
  `POST /runs/{id}/resume`, thread state/history endpoints.
- **0.3:** SSE streaming of graph events (in-process only — documented
  limitation, not hidden).
- **0.4:** smart retries — resume from the last checkpoint instead of
  re-running the graph (and re-paying for every LLM call) from scratch.

## License

MIT
