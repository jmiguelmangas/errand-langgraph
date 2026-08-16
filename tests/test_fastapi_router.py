from __future__ import annotations

import time
from typing import Any

from conftest import build_approval_graph, build_counter_graph, build_failing_graph
from fastapi import FastAPI
from fastapi.testclient import TestClient

from errand_langgraph import GraphRunner
from errand_langgraph.fastapi import mount_graph


def _build_app(graph: Any) -> tuple[FastAPI, GraphRunner]:
    runner = GraphRunner(graph)
    app = FastAPI(lifespan=runner.lifespan)
    mount_graph(app, runner, prefix="/agent")
    return app, runner


def _wait_until_terminal(
    client: TestClient, job_id: str, *, attempts: int = 500
) -> dict[str, Any]:
    for _ in range(attempts):
        response = client.get(f"/agent/runs/{job_id}")
        body: dict[str, Any] = response.json()
        if body["state"] in ("succeeded", "failed", "interrupted"):
            return body
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not reach a terminal state in time")


def test_submit_and_poll_to_succeeded() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        response = client.post("/agent/runs", json={"value": 1})
        assert response.status_code == 202
        body = response.json()
        assert "job_id" in body
        assert "thread_id" in body

        final = _wait_until_terminal(client, body["job_id"])
        assert final["state"] == "succeeded"
        assert final["result"] == {"value": 2}
        assert final["thread_id"] == body["thread_id"]


def test_submit_validates_body_against_graph_schema() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        response = client.post("/agent/runs", json={"value": "not-an-int"})
        assert response.status_code == 422


def test_get_run_reports_interrupted() -> None:
    app, _runner = _build_app(build_approval_graph())
    with TestClient(app) as client:
        response = client.post("/agent/runs", json={"value": 1, "approved": False})
        job_id = response.json()["job_id"]

        final = _wait_until_terminal(client, job_id)
        assert final["state"] == "interrupted"
        assert final["result"] is None
        assert final["interrupt"][0]["value"] == {"question": "approve?", "value": 2}


def test_get_run_reports_failed() -> None:
    app, _runner = _build_app(build_failing_graph())
    with TestClient(app) as client:
        response = client.post("/agent/runs", json={"value": 1, "approved": False})
        job_id = response.json()["job_id"]

        final = _wait_until_terminal(client, job_id)
        assert final["state"] == "failed"
        assert "boom" in final["error"]


def test_get_run_unknown_job_id_is_404() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        response = client.get("/agent/runs/does-not-exist")
        assert response.status_code == 404


def test_mount_graph_accepts_a_graph_directly() -> None:
    app = FastAPI()
    runner = mount_graph(app, build_counter_graph(), prefix="/agent")
    assert isinstance(runner, GraphRunner)

    async def _startup() -> None:
        await runner.startup()

    async def _shutdown() -> None:
        await runner.shutdown()

    app.router.on_startup.append(_startup)
    app.router.on_shutdown.append(_shutdown)

    with TestClient(app) as client:
        response = client.post("/agent/runs", json={"value": 1})
        job_id = response.json()["job_id"]
        final = _wait_until_terminal(client, job_id)
        assert final["state"] == "succeeded"
