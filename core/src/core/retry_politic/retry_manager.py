import asyncio
import secrets
from collections.abc import Callable
from typing import Any

from core.retry_politic.interfaces import RetryConfig, RetryPolicy


class RetryManager:
    """Управление повторными попытками с различными стратегиями."""

    def __init__(self, config: RetryConfig) -> None:
        self.config = config

    def calculate_delay(self, attempt: int) -> float:
        """Вычисляет задержку для конкретной попытки."""
        match self.config.policy:
            case RetryPolicy.EXPONENTIAL_BACKOFF:
                delay = self.config.initial_delay * (
                    self.config.backoff_multiplier ** (attempt - 1)
                )
            case RetryPolicy.LINEAR_BACKOFF:
                delay = self.config.initial_delay * attempt
            case RetryPolicy.FIBONACCI_BACKOFF:
                delay = self.config.initial_delay * self._fibonacci(attempt)
            case _:
                delay = self.config.initial_delay

        delay = min(delay, self.config.max_delay)

        if self.config.jitter:
            jitter = delay * 0.1 * (2 * secrets.SystemRandom().random() - 1)
            delay += jitter

        return max(0.0, delay)

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Вычисляет n-е число Фибоначчи."""
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
        """Выполняет операцию с повторными попытками."""
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

        raise last_exception or Exception("Unknown error in retry manager")
