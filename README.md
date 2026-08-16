# errand-langgraph

<p align="center">
  <img src="https://raw.githubusercontent.com/jmiguelmangas/errand-langgraph/main/assets/logo.png" alt="errand-langgraph logo" width="200">
</p>

<p align="center">
  <a href="https://pypi.org/project/errand-langgraph/"><img src="https://img.shields.io/pypi/v/errand-langgraph.svg?cachebust=1" alt="PyPI version"></a>
  <a href="https://pypi.org/project/errand-langgraph/"><img src="https://img.shields.io/pypi/pyversions/errand-langgraph.svg?cachebust=1" alt="Supported Python versions"></a>
  <a href="https://github.com/jmiguelmangas/errand-langgraph/actions/workflows/ci.yml"><img src="https://github.com/jmiguelmangas/errand-langgraph/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://github.com/jmiguelmangas/errand-langgraph/blob/main/LICENSE"><img src="https://img.shields.io/pypi/l/errand-langgraph.svg?cachebust=1" alt="License"></a>
</p>

Run [LangGraph](https://github.com/langchain-ai/langgraph) graphs as
[errand](https://github.com/jmiguelmangas/errand) jobs: background execution
with status polling, human-in-the-loop resume, event streaming, smart
retries, and an auto-generated FastAPI router — no Celery, no separate
broker.

> **Status: 0.1.1, published.** Submit/status, the FastAPI router,
> human-in-the-loop resume, SSE streaming, and smart retries are all
> implemented, tested (100% coverage), and live on PyPI — see
> [`CHANGELOG.md`](https://github.com/jmiguelmangas/errand-langgraph/blob/main/CHANGELOG.md)
> for what shipped in each version.

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

**Pass a checkpointer.** Without one, retries re-run the whole graph from
scratch instead of resuming (`GraphRunner` warns at construction time), and
interrupt detection falls back to a less robust signal (still correct, just
more exposed to LangGraph's own internal changes across versions — see
`runner.py`'s docstring). HITL and efficient retries both assume one.

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

This mounts six endpoints:

| Method | Path | What |
|---|---|---|
| `POST` | `/agent/runs` | Submit a run. Body is validated against the graph's own input schema when it's introspectable. Returns `{"job_id", "thread_id"}`, 202. |
| `GET` | `/agent/runs/{job_id}` | Current status: `{"state", "thread_id", "result", "interrupt", "error"}`. `state` is one of `queued`, `running`, `succeeded`, `failed`, `interrupted`. 404 if unknown. |
| `POST` | `/agent/runs/{job_id}/resume` | Resume an `interrupted` run. Body is the raw value `interrupt()` should return (any JSON). Returns a **new** `{"job_id", "thread_id"}` on the same thread, 202. 404 if `job_id` is unknown, 409 if it isn't `interrupted`. |
| `GET` | `/agent/threads/{thread_id}/state` | Current graph state for the thread: `{"values", "next", "interrupt"}`. |
| `GET` | `/agent/threads/{thread_id}/history` | Checkpoint history, newest first, optional `?limit=`. |
| `GET` | `/agent/runs/{job_id}/events` | `text/event-stream` of the run's graph state as it executes — one event per completed node, `{"seq", "type", "data"}`. 404 if `job_id` is unknown. **In-process only**, see below. |

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

See `examples/basic/` for a runnable submit/poll version, `examples/hitl/`
for the full interrupt/resume cycle, and `examples/streaming/` for SSE —
all three actually run end to end (server + client, no LLM calls).

**Retries** are configured with a `RetryPolicy`:

```python
from errand_langgraph import GraphRunner, RetryPolicy

runner = GraphRunner(
    graph,
    checkpointer=InMemorySaver(),
    retry=RetryPolicy(max_attempts=5, base_delay=1.0, max_delay=30.0),
)
```

The default policy (3 attempts, exponential backoff with full jitter)
retries timeouts, connection errors, and HTTP 429/5xx — duck-typed off a
`status_code` attribute so it works with any HTTP-based provider SDK
without importing it. Everything else (state-validation errors,
`GraphRecursionError`, auth errors, a tool's own exceptions) isn't retried
by default. With a checkpointer, a retry resumes from the last completed
node instead of re-running the graph — pass your own `is_retryable`
predicate for anything more specific:

```python
RetryPolicy(is_retryable=lambda exc: isinstance(exc, MyProviderRateLimitError))
```

Pass `RetryPolicy(max_attempts=1)` to disable retrying entirely.

**Don't forget startup.** Nothing runs submitted work without a running
worker pool — wire `lifespan=runner.lifespan` into `FastAPI(...)` (as above)
or call `await runner.startup()` yourself before submitting. `submit()`/
`resume()` warn if you forget.

**Streaming is in-process only.** `GET /runs/{id}/events` reads from an
in-memory pubsub inside the worker that's actually running the graph — it
only works when the client's request and that worker share this process.
Behind a load balancer with multiple worker processes, the SSE request has
to land on the specific process that owns the run; this package doesn't
route that for you. Polling `GET /runs/{id}` always works regardless of
process topology — reach for streaming only where the single-process
constraint is already true (which is most `errand`-based deployments, but
not all).

## Roadmap

The full feature set below is implemented, tested, and published. See
[`CHANGELOG.md`](https://github.com/jmiguelmangas/errand-langgraph/blob/main/CHANGELOG.md)
for what shipped in each version.

- **Done:** `GraphRunner.submit`/`status`, `mount_graph` with polling.
- **Done:** human-in-the-loop — `interrupt()` detection, `resume()`,
  `POST /runs/{id}/resume`, thread state/history endpoints.
- **Done:** SSE streaming of graph events (in-process only — documented
  above, not hidden).
- **Done:** smart retries — error classification, backoff with full
  jitter, resume from the last checkpoint instead of re-running the graph
  (and re-paying for every LLM call) from scratch.

Not yet built: cancelling an in-flight run, metrics/observability, and a
registry for mounting more than one graph under a single FastAPI app
(today it's one `mount_graph` call per graph).

## License

MIT
