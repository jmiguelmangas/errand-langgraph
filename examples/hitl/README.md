# hitl example

A graph that drafts a message, pauses via `interrupt()` for approval, then
"sends" it -- run in the background, resumed over HTTP.

```bash
cd examples/hitl

# terminal 1
uv run --project ../.. --extra fastapi uvicorn app:app

# terminal 2
uv run --project ../.. --extra fastapi python client.py
```

`client.py` submits a run, polls until it's `interrupted`, prints the
`interrupt` payload (what a real app would show a human), then calls
`POST /agent/runs/{job_id}/resume` with the approval and polls the new job
to `succeeded`.
