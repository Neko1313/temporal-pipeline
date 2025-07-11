from typing import Literal

from pydantic import BaseModel, Field


class ResilienceConfig(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=100)
    initial_delay: float = Field(default=1.0, ge=0.1)
    max_delay: float = Field(default=60.0, ge=1.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=5.0)
    retry_policy: Literal[
        "exponential_backoff",
        "linear_backoff",
        "fixed_interval",
        "fibonacci_backoff",
    ] = "exponential_backoff"
    jitter: bool = True

    circuit_breaker_enabled: bool = Field(default=True)
    failure_threshold: int = Field(default=5, ge=1)
    recovery_timeout: int = Field(default=60, ge=10)
    success_threshold: int = Field(default=3, ge=1)

    execution_timeout: int | None = Field(default=None, ge=1)

    retryable_exceptions: list[str] = Field(
        default_factory=lambda: [
            "ConnectionError",
            "TimeoutError",
            "TemporaryFailure",
        ],
    )
