"""Утилиты для Temporal workflows."""

# Функции для управления ошибками
from core.temporal.utils.error_stage import should_continue_on_failure

# Функции для управления выполнением
from core.temporal.utils.execution import (
    build_execution_order,
    execute_stage_batch,
    init_execution_metadata,
)

# Функции для построения результатов
from core.temporal.utils.results import (
    build_error_result,
    build_success_result,
)

__all__ = [
    # Result builders
    "build_error_result",
    # Execution management
    "build_execution_order",
    "build_success_result",
    "execute_stage_batch",
    "init_execution_metadata",
    # Error handling
    "should_continue_on_failure",
]
