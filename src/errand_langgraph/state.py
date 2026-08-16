"""RunRecord: this package's own index over what errand's Job doesn't persist.

``errand_jobs.Job`` (verified against errand-jobs 0.2.1) is a closed
dataclass with no arbitrary metadata field, and its ``result_repr`` is a
``str(result)`` truncated to 500 chars -- fine for logging, useless for
returning a structured graph result. ``RunIndex`` is the in-memory
``job_id -> RunRecord`` table that carries everything specific to a graph
run: the ``thread_id``, the full result, and (from 0.2 on) any pending
``interrupt()`` payload. See DESIGN.md sec 4.1.

One ``RunIndex`` per :class:`~errand_langgraph.runner.GraphRunner` --
not shared across runners, same lifetime as the graph it indexes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class RunState(str, Enum):
    """Lifecycle state of a run, finer-grained than errand's ``JobStatus``.

    ``INTERRUPTED`` has no equivalent in ``errand_jobs.JobStatus`` -- the
    underlying ``Job`` is left ``SUCCEEDED`` (see DESIGN.md sec 5), and this
    is the field that actually distinguishes "done" from "paused waiting on
    a human".
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass
class RunRecord:
    """Everything about one run that ``errand_jobs.Job`` can't hold.

    ``errand_job_id`` is the id of the underlying ``errand_jobs.Job`` --
    kept alongside the run's own ``job_id`` (see DESIGN.md sec 4.1 for why
    they're tracked separately) so generic job metadata (attempts,
    timestamps) can still be looked up when needed.
    """

    job_id: str
    thread_id: str
    graph_input: dict[str, Any] | None
    errand_job_id: str | None = None
    state: RunState = RunState.QUEUED
    result: dict[str, Any] | None = None
    interrupt: Any | None = None
    error: str | None = None


class RunIndex:
    """In-memory ``job_id -> RunRecord`` table.

    Example::

        index = RunIndex()
        record = index.create(job_id="r-1", thread_id="t-1", graph_input={})
        record.state = RunState.RUNNING
        index.get("r-1").state
        # RunState.RUNNING
    """

    def __init__(self) -> None:
        self._records: dict[str, RunRecord] = {}

    def create(
        self, *, job_id: str, thread_id: str, graph_input: dict[str, Any] | None
    ) -> RunRecord:
        """Create and store a new ``RunRecord``, returning it."""
        record = RunRecord(job_id=job_id, thread_id=thread_id, graph_input=graph_input)
        self._records[job_id] = record
        return record

    def get(self, job_id: str) -> RunRecord | None:
        """Return the record for ``job_id``, or ``None`` if unknown."""
        return self._records.get(job_id)
