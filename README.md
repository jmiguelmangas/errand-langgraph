# errand-langgraph

Run [LangGraph](https://github.com/langchain-ai/langgraph) graphs as
[errand](https://github.com/jmiguelmangas/errand) jobs: background execution
with status polling, human-in-the-loop resume, and an auto-generated FastAPI
router — no Celery, no separate broker.

> **Status:** early development, not yet released. See `TASKS.md` (not
> published) for the milestone plan; this README fills in as each ships.

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

## License

MIT
