# streaming example

A three-node pipeline (no LLM calls, each node just sleeps) whose progress is
streamed to the client over SSE as it runs, instead of polled.

```bash
cd examples/streaming

# terminal 1
uv run --project ../.. --extra fastapi uvicorn app:app

# terminal 2
uv run --project ../.. --extra fastapi python client.py
```

`client.py` submits a run, then opens `GET /agent/runs/{job_id}/events` and
prints each event as it arrives in real time -- one per completed node,
plus the initial input.

**In-process only** (see the main README): this only works because the
client and the worker running the graph share this server process. Behind
a load balancer with multiple worker processes, the SSE request has to land
on the specific process that owns the run.
