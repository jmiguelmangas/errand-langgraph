"""Engine-only: mount_graph without FastAPI installed must fail clearly.

Deliberately does NOT match tests/test_fastapi*.py, so it also runs in the
CI job that installs errand-langgraph without the fastapi extra -- same
convention as errand_jobs's own test_router_availability.py.
"""

from __future__ import annotations

import sys

import pytest
from conftest import build_counter_graph


def test_mount_graph_without_fastapi_raises_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "fastapi", None)

    from errand_langgraph.fastapi import mount_graph

    with pytest.raises(ImportError, match=r"pip install errand-langgraph\[fastapi\]"):
        mount_graph(object(), build_counter_graph(), prefix="/agent")
