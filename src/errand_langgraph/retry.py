"""Retry classification + backoff for :class:`~errand_langgraph.runner.GraphRunner`.

DESIGN.md sec 7: verified against errand-jobs 0.2.1 that its own ``Runner``
retries *any* exception up to ``max_retries`` with no classification hook --
the wrong tool here, since a validation error and a rate limit need
different treatment. ``GraphRunner``'s internal task registers with
``max_retries=0`` (see runner.py) and the retry loop lives in
:meth:`~errand_langgraph.runner.GraphRunner._run_graph` instead, using
:class:`RetryPolicy` from this module to decide whether/how long to wait,
and resuming from the last checkpoint (``payload=None``) rather than
re-running the graph from scratch when a checkpointer is configured.

Classification is a predicate, not a fixed list of exception types tied to
any LLM provider's SDK: errand-langgraph has no opinion on which provider
you use and isn't going to depend on openai/anthropic/etc. just to catch
their error types. :func:`default_is_retryable` is a generic heuristic that
covers the common case (timeouts, connection errors, HTTP 429/5xx) without
importing any of them; pass your own via ``GraphRunner(..., retry=...)``
for anything more specific.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass


def _status_code_of(exc: BaseException) -> int | None:
    for candidate in (exc, getattr(exc, "response", None)):
        code = getattr(candidate, "status_code", None)
        if isinstance(code, int):
            return code
    return None


def default_is_retryable(exc: BaseException) -> bool:
    """Timeouts, connection errors, and HTTP 429/5xx -- everything else isn't.

    The HTTP check is duck-typed against a ``status_code`` attribute (on
    the exception itself, or its ``.response``) -- the convention httpx,
    requests, and most HTTP-based provider SDKs already follow, without
    importing any of them (same reasoning as ``errand_jobs.di``'s
    duck-typed dependency detection).

    Everything else -- state validation errors, ``GraphRecursionError``,
    auth errors, a tool's own exceptions -- falls through to ``False`` with
    no special-casing needed: none of them are a ``TimeoutError``/
    ``ConnectionError`` subclass or carry an HTTP status code, so the
    default heuristic already gets DESIGN.md sec 7's "not retryable" list
    right without hardcoding any of it.
    """
    if isinstance(exc, TimeoutError | ConnectionError):
        return True
    status = _status_code_of(exc)
    if status is None:
        return False
    return status == 429 or status >= 500


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to retry a failed run, how long to wait, and on what.

    ``delay_for(attempt)`` uses full jitter (`AWS's recommended approach
    <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>`_,
    not reinvented here): a uniform random delay between 0 and the
    exponential cap, so many concurrent retries don't all wake up and hit
    the same rate-limited provider at once.

    Example::

        policy = RetryPolicy(max_attempts=3, base_delay=1.0)
        0 <= policy.delay_for(1) <= 1.0
        0 <= policy.delay_for(2) <= 2.0
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    is_retryable: Callable[[BaseException], bool] = default_is_retryable

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait before the given (1-indexed) retry attempt."""
        exponential = self.base_delay * (2.0 ** (attempt - 1))
        capped = min(exponential, self.max_delay)
        return random.uniform(0, capped)
