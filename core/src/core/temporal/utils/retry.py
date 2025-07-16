"""Управление политиками повторных попыток."""

from datetime import timedelta
from typing import NamedTuple

from temporalio.common import RetryPolicy

from core.temporal.constants import (
    FIBONACCI_COEFFICIENT,
    LINEAR_COEFFICIENT,
    ErrorType,
)
from core.temporal.constants import (
    RetryPolicy as RetryPolicyType,
)
from core.yaml_loader.interfaces import ResilienceConfig


class RetryIntervals(NamedTuple):
    """Интервалы для политики повторных попыток."""

    initial: timedelta
    maximum: timedelta


def create_retry_policy(resilience_config: ResilienceConfig) -> RetryPolicy:
    """
    Создает политику повторных попыток на основе конфигурации.

    Args:
        resilience_config: Конфигурация устойчивости

    Returns:
        Настроенная политика повторных попыток
    """
    intervals = _calculate_intervals(resilience_config)
    coefficient = _get_backoff_coefficient(resilience_config)

    return RetryPolicy(
        initial_interval=intervals.initial,
        maximum_interval=intervals.maximum,
        maximum_attempts=resilience_config.max_attempts,
        backoff_coefficient=coefficient,
        non_retryable_error_types=_get_non_retryable_errors(),
    )


def _calculate_intervals(
    resilience_config: ResilienceConfig,
) -> RetryIntervals:
    """Вычисляет интервалы для повторных попыток."""
    initial = timedelta(seconds=resilience_config.initial_delay)
    maximum = timedelta(seconds=resilience_config.max_delay)

    # Для фиксированного интервала максимум равен начальному
    if resilience_config.retry_policy == RetryPolicyType.FIXED:
        maximum = initial

    return RetryIntervals(initial, maximum)


def _get_backoff_coefficient(resilience_config: ResilienceConfig) -> float:
    """Определяет коэффициент для увеличения задержки."""
    policy_coefficients = {
        RetryPolicyType.EXPONENTIAL: resilience_config.backoff_multiplier,
        RetryPolicyType.LINEAR: LINEAR_COEFFICIENT,
        RetryPolicyType.FIXED: LINEAR_COEFFICIENT,
        RetryPolicyType.FIBONACCI: FIBONACCI_COEFFICIENT,
    }

    return policy_coefficients.get(
        resilience_config.retry_policy,
        resilience_config.backoff_multiplier,
    )


def _get_non_retryable_errors() -> list[str]:
    """Возвращает список ошибок, не подлежащих повторным попыткам."""
    return [
        ErrorType.CONFIGURATION,
        ErrorType.AUTHENTICATION,
        ErrorType.PERMISSION,
    ]
