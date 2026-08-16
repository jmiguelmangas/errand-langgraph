# errand-langgraph

Run [LangGraph](https://github.com/langchain-ai/langgraph) graphs as
[errand](https://github.com/jmiguelmangas/errand) jobs: background execution
with status polling, human-in-the-loop resume, and an auto-generated FastAPI
router — no Celery, no separate broker.

> **Status:** early development, not yet released to PyPI. 0.1 (submit/status
> + the FastAPI router) is implemented; HITL resume, streaming, and smart
> retries are in progress — see the roadmap below.

**Requires Python 3.11+.** `interrupt()` is broken under Python 3.10 in
recent `langgraph` releases (a real, verified upstream bug, not a guess —
`RuntimeError: Called get_config outside of a runnable context`, reproduced
in plain `langgraph` with no `errand-langgraph` involved). Since
human-in-the-loop is this package's flagship feature, 3.10 isn't supported.

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

This mounts two endpoints:

| Method | Path | What |
|---|---|---|
| `POST` | `/agent/runs` | Submit a run. Body is validated against the graph's own input schema when it's introspectable. Returns `{"job_id", "thread_id"}`, 202. |
| `GET` | `/agent/runs/{job_id}` | Current status: `{"state", "thread_id", "result", "interrupt", "error"}`. `state` is one of `queued`, `running`, `succeeded`, `failed`, `interrupted`. 404 if unknown. |

```bash
curl -X POST localhost:8000/agent/runs -d '{"messages": [["user", "hi"]]}'
# {"job_id": "...", "thread_id": "..."}

curl localhost:8000/agent/runs/<job_id>
# {"state": "succeeded", "result": {...}, ...}
```

Without FastAPI, drive the same thing directly:

```python
from errand_langgraph import GraphRunner

runner = GraphRunner(graph, checkpointer=InMemorySaver())
await runner.startup()
handle = await runner.submit({"messages": [("user", "hi")]})
status = await runner.status(handle.job_id)
await runner.shutdown()
```

See `examples/basic/` for a runnable end-to-end version (server + polling
client, no LLM calls).

**Don't forget startup.** Nothing runs submitted work without a running
worker pool — wire `lifespan=runner.lifespan` into `FastAPI(...)` (as above)
or call `await runner.startup()` yourself before submitting. `submit()` warns
if you forget.

## Roadmap

- **0.1 (done):** `GraphRunner.submit`/`status`, `mount_graph` with polling.
- **0.2:** human-in-the-loop — detect `interrupt()`, `POST /runs/{id}/resume`,
  thread state/history endpoints.
- **0.3:** SSE streaming of graph events (in-process only — documented
  limitation, not hidden).
- **0.4:** smart retries — resume from the last checkpoint instead of
  re-running the graph (and re-paying for every LLM call) from scratch.

## License

MIT
