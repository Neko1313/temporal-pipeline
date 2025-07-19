"""Общие енумы для проекта."""

from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Статусы выполнения."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class ComponentType(StrEnum):
    """Типы компонентов."""

    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"


class RetryPolicy(StrEnum):
    """Политики повторных попыток."""

    EXPONENTIAL = "exponential_backoff"
    LINEAR = "linear_backoff"
    FIXED = "fixed_interval"
    FIBONACCI = "fibonacci_backoff"


class ErrorType(StrEnum):
    """Типы ошибок, не подлежащие повторным попыткам."""

    CONFIGURATION = "ConfigurationError"
    AUTHENTICATION = "AuthenticationError"
    PERMISSION = "PermissionError"
