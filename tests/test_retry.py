from __future__ import annotations

from errand_langgraph.retry import RetryPolicy, default_is_retryable


def test_default_is_retryable_timeout_and_connection_errors() -> None:
    assert default_is_retryable(TimeoutError("slow"))
    assert default_is_retryable(ConnectionError("reset"))
    assert default_is_retryable(ConnectionResetError("reset"))  # a ConnectionError


def test_default_is_retryable_status_code_on_exception_itself() -> None:
    class RateLimited(Exception):
        status_code = 429

    class ServerError(Exception):
        status_code = 503

    class BadRequest(Exception):
        status_code = 400

    assert default_is_retryable(RateLimited())
    assert default_is_retryable(ServerError())
    assert not default_is_retryable(BadRequest())


def test_default_is_retryable_status_code_on_response_attribute() -> None:
    class FakeResponse:
        status_code = 500

    class SdkError(Exception):
        response = FakeResponse()

    assert default_is_retryable(SdkError())


def test_default_is_retryable_plain_exceptions_are_not_retryable() -> None:
    assert not default_is_retryable(ValueError("bad state"))
    assert not default_is_retryable(RuntimeError("boom"))


def test_delay_for_is_bounded_by_exponential_cap() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0)
    for attempt in range(1, 6):
        cap = min(1.0 * 2.0 ** (attempt - 1), 10.0)
        delay = policy.delay_for(attempt)
        assert 0.0 <= delay <= cap


def test_delay_for_respects_max_delay_cap() -> None:
    policy = RetryPolicy(base_delay=100.0, max_delay=5.0)
    assert 0.0 <= policy.delay_for(1) <= 5.0
    assert 0.0 <= policy.delay_for(10) <= 5.0
