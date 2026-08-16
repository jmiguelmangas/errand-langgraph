# basic example

A two-node graph (no LLM calls -- each node just sleeps briefly to stand in
for one) run in the background via `errand-langgraph`, polled over HTTP.

```bash
cd examples/basic

# terminal 1
uv run --project ../.. --extra fastapi uvicorn app:app

# terminal 2
uv run --project ../.. --extra fastapi python client.py
```

`client.py` submits a run, then polls `GET /agent/runs/{job_id}` until it's
`succeeded` -- printing each poll and the final result.
