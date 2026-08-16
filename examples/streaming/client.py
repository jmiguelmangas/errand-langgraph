"""Run this against `app.py` (see its docstring), while it's up:

uv run --extra fastapi python examples/streaming/client.py
"""

from __future__ import annotations

import json

import httpx

BASE_URL = "http://127.0.0.1:8000/agent"


def main() -> None:
    with httpx.Client(base_url=BASE_URL, timeout=None) as client:
        submitted = client.post("/runs", json={"topic": "background jobs"}).json()
        print(f"submitted: {submitted}")

        print("streaming events as they arrive:")
        with client.stream("GET", f"/runs/{submitted['job_id']}/events") as response:
            for line in response.iter_lines():
                if not line.startswith("data: "):
                    continue
                event = json.loads(line.removeprefix("data: "))
                print(f"  [{event['seq']}] {event['data']}")

        final = client.get(f"/runs/{submitted['job_id']}").json()
        print(f"final: {final}")


if __name__ == "__main__":
    main()
