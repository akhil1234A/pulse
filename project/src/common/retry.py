"""Exponential backoff with jitter and circuit breaker pattern implementation."""

import asyncio
import logging
import random
import time
from enum import IntEnum
from typing import Any, Callable, Coroutine, Optional, TypeVar

from src.common.metrics import CIRCUIT_BREAKER_STATE

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(IntEnum):
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2


class CircuitBreakerOpenException(Exception):
    """Raised when an operation is attempted while the circuit breaker is OPEN."""

    pass


def calculate_backoff_delay(
    attempt: int,
    base_ms: float = 100.0,
    max_ms: float = 30000.0,
) -> float:
    """Calculates exponential backoff with full jitter in seconds.

    Formula: min(base_ms * 2^attempt + uniform(0, 100ms), max_ms)
    """
    exponent_delay = base_ms * (2**attempt)
    jitter = random.uniform(0, 100.0)
    delay_ms = min(exponent_delay + jitter, max_ms)
    return delay_ms / 1000.0


class CircuitBreaker:
    """Three-state circuit breaker pattern for external dependencies (Redis, Postgres)."""

    def __init__(
        self,
        component_name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
    ) -> None:
        self.component_name = component_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds

        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures: int = 0
        self._last_state_change: float = time.monotonic()
        self._sync_metric()

    @property
    def state(self) -> CircuitState:
        """Returns the current state, checking for timeout transitions to HALF_OPEN."""
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_state_change
            if elapsed >= self.recovery_timeout_seconds:
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    def _sync_metric(self) -> None:
        try:
            CIRCUIT_BREAKER_STATE.labels(component=self.component_name).set(int(self._state))
        except Exception:
            pass

    def _transition_to(self, new_state: CircuitState) -> None:
        prev_state = self._state
        self._state = new_state
        self._last_state_change = time.monotonic()
        self._sync_metric()
        logger.info(
            "Circuit breaker for %s changed state: %s -> %s",
            self.component_name,
            prev_state.name,
            new_state.name,
        )

    def record_success(self) -> None:
        """Records a successful op, transitioning HALF_OPEN -> CLOSED or resetting failures."""
        if self._state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.CLOSED)
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        """Records a failed operation, transitioning to OPEN if threshold reached."""
        self._consecutive_failures += 1
        if self._state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.OPEN)
        elif (
            self._state == CircuitState.CLOSED
            and self._consecutive_failures >= self.failure_threshold
        ):
            self._transition_to(CircuitState.OPEN)

    def is_call_permitted(self) -> bool:
        """Checks if a call is allowed to execute."""
        return self.state != CircuitState.OPEN

    async def call_async(
        self, coro_func: Callable[..., Coroutine[Any, Any, T]], *args: Any, **kwargs: Any
    ) -> T:
        """Executes an async callable protected by the circuit breaker."""
        if not self.is_call_permitted():
            raise CircuitBreakerOpenException(
                f"Circuit breaker for {self.component_name} is OPEN; call rejected."
            )
        try:
            result = await coro_func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as exc:
            self.record_failure()
            raise exc

    def call_sync(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Executes a synchronous callable protected by the circuit breaker."""
        if not self.is_call_permitted():
            raise CircuitBreakerOpenException(
                f"Circuit breaker for {self.component_name} is OPEN; call rejected."
            )
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as exc:
            self.record_failure()
            raise exc


async def retry_with_backoff_async(
    coro_func: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    max_retries: int = 5,
    base_ms: float = 100.0,
    max_ms: float = 30000.0,
    **kwargs: Any,
) -> T:
    """Executes an async function with exponential backoff and jitter."""
    last_exception: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return await coro_func(*args, **kwargs)
        except Exception as exc:
            last_exception = exc
            if attempt < max_retries - 1:
                delay = calculate_backoff_delay(attempt, base_ms=base_ms, max_ms=max_ms)
                logger.warning(
                    "Operation failed (attempt %d/%d), retrying in %.3fs: %s",
                    attempt + 1,
                    max_retries,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
    assert last_exception is not None
    raise last_exception
