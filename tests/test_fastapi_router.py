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


def test_resume_full_cycle_via_http() -> None:
    app, _runner = _build_app(build_approval_graph())
    with TestClient(app) as client:
        submitted = client.post(
            "/agent/runs", json={"value": 1, "approved": False}
        ).json()
        interrupted = _wait_until_terminal(client, submitted["job_id"])
        assert interrupted["state"] == "interrupted"

        resumed = client.post(f"/agent/runs/{submitted['job_id']}/resume", json=True)
        assert resumed.status_code == 202
        resumed_body = resumed.json()
        assert resumed_body["thread_id"] == submitted["thread_id"]
        assert resumed_body["job_id"] != submitted["job_id"]

        final = _wait_until_terminal(client, resumed_body["job_id"])
        assert final["state"] == "succeeded"
        assert final["result"] == {"value": 4, "approved": True}


def test_resume_unknown_job_id_is_404() -> None:
    app, _runner = _build_app(build_approval_graph())
    with TestClient(app) as client:
        response = client.post("/agent/runs/does-not-exist/resume", json=True)
        assert response.status_code == 404


def test_resume_not_interrupted_is_409() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        submitted = client.post("/agent/runs", json={"value": 1}).json()
        _wait_until_terminal(client, submitted["job_id"])

        response = client.post(f"/agent/runs/{submitted['job_id']}/resume", json=True)
        assert response.status_code == 409


def test_get_thread_state_reflects_interrupted_graph() -> None:
    app, _runner = _build_app(build_approval_graph())
    with TestClient(app) as client:
        submitted = client.post(
            "/agent/runs", json={"value": 1, "approved": False}
        ).json()
        _wait_until_terminal(client, submitted["job_id"])

        response = client.get(f"/agent/threads/{submitted['thread_id']}/state")
        assert response.status_code == 200
        body = response.json()
        assert body["values"] == {"value": 2, "approved": False}
        assert body["next"] == ["ask_for_approval"]
        assert body["interrupt"][0]["value"] == {"question": "approve?", "value": 2}


def test_get_thread_state_for_unknown_thread_is_empty() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        response = client.get("/agent/threads/never-used/state")
        assert response.status_code == 200
        assert response.json() == {"values": {}, "next": [], "interrupt": None}


def test_get_thread_history_lists_checkpoints() -> None:
    app, _runner = _build_app(build_counter_graph())
    with TestClient(app) as client:
        submitted = client.post("/agent/runs", json={"value": 1}).json()
        thread_id = submitted["thread_id"]
        _wait_until_terminal(client, submitted["job_id"])

        response = client.get(f"/agent/threads/{thread_id}/history")
        assert response.status_code == 200
        history = response.json()
        assert len(history) >= 2
        assert all("checkpoint_id" in entry for entry in history)

        limited = client.get(
            f"/agent/threads/{thread_id}/history", params={"limit": 1}
        ).json()
        assert len(limited) == 1
