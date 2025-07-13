from enum import StrEnum

from pydantic import BaseModel, Field


class RetryPolicy(StrEnum):
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_INTERVAL = "fixed_interval"
    FIBONACCI_BACKOFF = "fibonacci_backoff"


class RetryConfig(BaseModel):
    max_attempts: int = Field(3)
    initial_delay: float = Field(1.0)
    max_delay: float = Field(60.0)
    policy: RetryPolicy = Field(RetryPolicy.EXPONENTIAL_BACKOFF)
    jitter: bool = Field(True)
    backoff_multiplier: float = Field(2.0)


class CircuitBreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerConfig(BaseModel):
    failure_threshold: int = Field(5)
    recovery_timeout: int = Field(60)
    success_threshold: int = Field(3)
