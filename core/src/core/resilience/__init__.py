import asyncio
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class RetryPolicy(Enum):
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"
    FIBONACCI_BACKOFF = "fibonacci_backoff"


@dataclass
class RetryConfig:
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    policy: RetryPolicy = RetryPolicy.EXPONENTIAL_BACKOFF
    jitter: bool = True
    backoff_multiplier: float = 2.0


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: int = 60
    success_threshold: int = 3  # For half-open state


class RetryManager:
    """Управление повторными попытками с различными стратегиями"""

    def __init__(self, config: RetryConfig) -> None:
        self.config = config

    def calculate_delay(self, attempt: int) -> float:
        """Вычисляет задержку для конкретной попытки"""
        if self.config.policy == RetryPolicy.EXPONENTIAL_BACKOFF:
            delay = self.config.initial_delay * (
                self.config.backoff_multiplier ** (attempt - 1)
            )
        elif self.config.policy == RetryPolicy.LINEAR_BACKOFF:
            delay = self.config.initial_delay * attempt
        elif self.config.policy == RetryPolicy.FIBONACCI_BACKOFF:
            delay = self.config.initial_delay * self._fibonacci(attempt)
        else:  # FIXED_INTERVAL
            delay = self.config.initial_delay

        delay = min(delay, self.config.max_delay)

        if self.config.jitter:
            # Добавляем случайное отклонение ±10%
            jitter = delay * 0.1 * (2 * secrets.SystemRandom().random() - 1)
            delay += jitter

        return max(0, delay)

    def _fibonacci(self, n: int) -> int:
        """Вычисляет n-е число Фибоначчи"""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b

    async def execute_with_retry(
        self,
        operation: Callable[[], Any],
        should_retry: Callable[[Exception], bool] | None = None,
    ) -> Any:
        """Выполняет операцию с повторными попытками"""
        last_exception = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                return await operation()
            except Exception as e:
                last_exception = e

                # Проверяем, нужно ли повторить операцию
                if should_retry and not should_retry(e):
                    raise e

                if attempt == self.config.max_attempts:
                    raise e

                delay = self.calculate_delay(attempt)
                await asyncio.sleep(delay)

        # Этот код не должен выполняться, но добавлен для безопасности
        raise last_exception or Exception("Unknown error in retry manager")


class CircuitBreaker:
    """Circuit Breaker для предотвращения каскадных сбоев"""

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0

    async def execute(self, operation: Callable[[], Any]) -> Any:
        """Выполняет операцию через Circuit Breaker"""
        if self.state == CircuitBreakerState.OPEN:
            if (
                time.time() - self.last_failure_time
                > self.config.recovery_timeout
            ):
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
        """Обработка успешного выполнения"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
        else:
            self.failure_count = 0

    async def _on_failure(self) -> None:
        """Обработка неудачного выполнения"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN


class CircuitBreakerOpenError(Exception):
    """Исключение, выбрасываемое когда Circuit Breaker открыт"""

    pass
