"""Unit tests for retry logic and circuit breaker."""

import time

import pytest

from src.common.retry import (
    CircuitBreaker,
    CircuitBreakerOpenException,
    CircuitState,
    calculate_backoff_delay,
    retry_with_backoff_async,
)


def test_calculate_backoff_delay():
    # Attempt 0: base 100ms * 1 = 100ms + jitter [0, 100]ms -> [0.100, 0.200]s
    delay_0 = calculate_backoff_delay(0, base_ms=100.0, max_ms=30000.0)
    assert 0.100 <= delay_0 <= 0.205

    # Max cap check
    delay_high = calculate_backoff_delay(10, base_ms=100.0, max_ms=500.0)
    assert delay_high == 0.5


def test_circuit_breaker_closed_to_open_transitions():
    cb = CircuitBreaker("test_service", failure_threshold=3, recovery_timeout_seconds=0.2)
    assert cb.state == CircuitState.CLOSED

    # 2 failures - still closed
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.CLOSED

    # 3rd failure reaches threshold -> OPEN
    cb.record_failure()
    assert cb.state == CircuitState.OPEN
    assert cb.is_call_permitted() is False

    # Attempting call while OPEN raises CircuitBreakerOpenException
    with pytest.raises(CircuitBreakerOpenException):
        cb.call_sync(lambda: "ok")


def test_circuit_breaker_recovery_to_half_open_and_closed():
    cb = CircuitBreaker("test_service", failure_threshold=2, recovery_timeout_seconds=0.1)
    cb.record_failure()
    cb.record_failure()
    assert cb.state == CircuitState.OPEN

    # Wait for timeout to expire
    time.sleep(0.15)
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.is_call_permitted() is True

    # Successful call recovers to CLOSED
    result = cb.call_sync(lambda: "recovered")
    assert result == "recovered"
    assert cb.state == CircuitState.CLOSED


def test_circuit_breaker_half_open_failure_reopens():
    cb = CircuitBreaker("test_service", failure_threshold=2, recovery_timeout_seconds=0.1)
    cb.record_failure()
    cb.record_failure()
    time.sleep(0.15)
    assert cb.state == CircuitState.HALF_OPEN

    # Failure in HALF_OPEN returns to OPEN immediately
    with pytest.raises(ValueError):

        def failing_call():
            raise ValueError("boom")

        cb.call_sync(failing_call)

    assert cb.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_retry_with_backoff_async_success():
    attempts = 0

    async def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient network blip")
        return "success"

    result = await retry_with_backoff_async(flaky, max_retries=4, base_ms=1.0, max_ms=10.0)
    assert result == "success"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_with_backoff_async_exhausted():
    async def always_fails():
        raise RuntimeError("fatal error")

    with pytest.raises(RuntimeError, match="fatal error"):
        await retry_with_backoff_async(always_fails, max_retries=3, base_ms=1.0, max_ms=5.0)
