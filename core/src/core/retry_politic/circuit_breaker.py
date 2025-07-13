from collections.abc import Callable
from time import time
from typing import Any

from core.retry_politic.error import CircuitBreakerOpenError
from core.retry_politic.interfaces import (
    CircuitBreakerConfig,
    CircuitBreakerState,
)


class CircuitBreaker:
    """Circuit Breaker для предотвращения каскадных сбоев."""

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0

    async def execute(self, operation: Callable[[], Any]) -> Any:
        """Выполняет операцию через Circuit Breaker."""
        if self.state == CircuitBreakerState.OPEN:
            if time() - self.last_failure_time > self.config.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
            else:
                msg = "Circuit breaker is open"
                raise CircuitBreakerOpenError(msg)

        try:
            result = await operation()
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise e

    async def _on_success(self) -> None:
        """Обработка успешного выполнения."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
        else:
            self.failure_count = 0

    async def _on_failure(self) -> None:
        """Обработка неудачного выполнения."""
        self.failure_count += 1
        self.last_failure_time = time()

        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
