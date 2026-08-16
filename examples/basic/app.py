"""Run from this directory: uv run --extra fastapi uvicorn app:app --reload"""

from __future__ import annotations

from fastapi import FastAPI
from graph import build_graph
from langgraph.checkpoint.memory import InMemorySaver

from errand_langgraph import GraphRunner
from errand_langgraph.fastapi import mount_graph

runner = GraphRunner(build_graph(), checkpointer=InMemorySaver())
app = FastAPI(title="errand-langgraph basic example", lifespan=runner.lifespan)
mount_graph(app, runner, prefix="/agent")
