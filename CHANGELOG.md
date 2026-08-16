# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2]

### Fixed

- README's status banner still said "not yet released to PyPI" / "release
  polish is what's left" after 0.1.0 was already published — leftover
  pre-publish language the 0.1.1 fix didn't catch because that pass only
  addressed the one thing reported (badge URLs), not a full read-through.
  Reworded to be version-agnostic (`"Status: published on PyPI"` instead
  of hardcoding a version number that goes stale every release) so this
  class of staleness is less likely to recur.

### Changed

- `pyproject.toml`'s `description` now mentions streaming and retries
  (it only listed HITL before, from when 0.1 was the only milestone
  written up). `Development Status` classifier bumped `3 - Alpha` →
  `4 - Beta` — the full planned feature set is implemented, tested at
  100% coverage, and has now been published twice.

## [0.1.1]

### Fixed

- README's PyPI/Python-version/license badges rendered as "package or
  version not found" on the PyPI project page — written before the
  package existed on PyPI (days before the actual first publish), and
  PyPI freezes each version's README at upload time, so the 0.1.0 upload
  carried the stale badge URLs regardless of shields.io itself already
  being correct. Same class of issue as `errand`'s own 0.1.0→0.1.1 fix
  (that one was relative image paths; this one is upload-time freshness),
  same fix shape: re-upload via a patch release.

## [0.1.0]

Initial release. Built through internal milestones 0.1–0.4 (see DESIGN.md,
not published) in one pass before the first PyPI publish, so everything
below ships together rather than across separate releases.

### Added

- **`GraphRunner`.** Runs a LangGraph graph (compiled or not) as a
  background `errand` job: `submit`/`status`, `resume` after an
  `interrupt()`, `thread_state`/`thread_history`, `stream_events`.
  Registers one internal task on an `errand_jobs.Errand` (a fresh one by
  default, or share yours via `errand=`) with `max_retries=0` — retries
  are this package's own loop, not errand's native one, since errand's
  can't classify exceptions or resume from a checkpoint.
- **`mount_graph`.** Auto-generated FastAPI router: `POST /runs`,
  `GET /runs/{job_id}`, `POST /runs/{job_id}/resume`,
  `GET /threads/{id}/state`, `GET /threads/{id}/history`,
  `GET /runs/{job_id}/events` (SSE). Request-body validation derived from
  the graph's own `state_schema` when introspectable (falls back to
  `dict[str, Any]`, with a warning, otherwise).
- **Human-in-the-loop.** Interrupt detection via `graph.aget_state(...).next`
  (falls back to the `__interrupt__` chunk key without a checkpointer).
  `resume()` creates a **new** job on the same `thread_id` — job history
  stays immutable.
- **SSE streaming.** `stream_events`/`GET .../events` stream the graph's
  own `astream(stream_mode="values")` output as it runs, via an in-memory,
  per-job pubsub with a bounded, drop-oldest replay buffer. In-process
  only — documented as a real constraint, not hidden.
- **Smart retries.** `RetryPolicy`: exponential backoff with full jitter,
  and a pluggable `is_retryable` predicate (default: builtin
  `TimeoutError`/`ConnectionError` plus a duck-typed HTTP 429/5xx
  `status_code` check — no provider SDK dependency). With a checkpointer,
  a retry resumes from the last completed node instead of re-running the
  graph from scratch; without one, `GraphRunner` warns at construction
  time and retries restart from scratch instead.

### Notes

- **Requires Python 3.11+.** `interrupt()` raises
  `RuntimeError: Called get_config outside of a runnable context` under
  Python 3.10 with recent `langgraph` releases — reproduced in plain
  `langgraph`, unrelated to this package. Since HITL is the flagship
  feature, 3.10 isn't supported rather than partially working.
- **`state_schema` TypedDicts must come from `typing_extensions`, not
  `typing`.** Pydantic v2 can't introspect `typing.TypedDict` on
  Python < 3.12 (`PydanticUserError`); `mount_graph`'s schema derivation
  warns and falls back to unvalidated `dict[str, Any]` if you get this
  wrong.
- The engine (`errand_langgraph`, minus `errand_langgraph.fastapi`) works
  without FastAPI installed; FastAPI is an optional extra
  (`pip install errand-langgraph[fastapi]`) needed only for `mount_graph`.
- 100% test coverage on every module; the non-FastAPI test suite is
  verified to pass in an environment where FastAPI is not installed, in CI
  and locally.
