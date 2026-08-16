"""Run this against `app.py` (see its docstring), while it's up:

uv run --extra fastapi python examples/hitl/client.py
"""

from __future__ import annotations

import time

import httpx

BASE_URL = "http://127.0.0.1:8000/agent"


def _poll(client: httpx.Client, job_id: str) -> dict:
    while True:
        status = client.get(f"/runs/{job_id}").json()
        print(f"poll: {status['state']}")
        if status["state"] in ("succeeded", "failed", "interrupted"):
            return status
        time.sleep(0.5)


def main() -> None:
    with httpx.Client(base_url=BASE_URL) as client:
        submitted = client.post("/runs", json={"topic": "the Q3 roadmap"}).json()
        print(f"submitted: {submitted}")

        interrupted = _poll(client, submitted["job_id"])
        print(f"interrupted: {interrupted}")
        assert interrupted["state"] == "interrupted"

        # A real app would show interrupted["interrupt"] to a human here and
        # wait for their decision. This example just approves automatically.
        approved = True
        print(f"resuming with approved={approved}")
        resumed = client.post(
            f"/runs/{submitted['job_id']}/resume", json=approved
        ).json()
        print(f"resumed: {resumed}")

        final = _poll(client, resumed["job_id"])
        print(f"final: {final}")


if __name__ == "__main__":
    main()
