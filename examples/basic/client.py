"""Run this against `app.py` (see its docstring), while it's up:

uv run --extra fastapi python examples/basic/client.py
"""

from __future__ import annotations

import time

import httpx

BASE_URL = "http://127.0.0.1:8000/agent"


def main() -> None:
    with httpx.Client(base_url=BASE_URL) as client:
        response = client.post("/runs", json={"topic": "background jobs"})
        response.raise_for_status()
        body = response.json()
        job_id = body["job_id"]
        print(f"submitted: {body}")

        while True:
            status = client.get(f"/runs/{job_id}").json()
            print(f"poll: {status['state']}")
            if status["state"] in ("succeeded", "failed", "interrupted"):
                print(f"final: {status}")
                break
            time.sleep(0.5)


if __name__ == "__main__":
    main()
