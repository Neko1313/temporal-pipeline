"""Вспомогательные функции для Temporal workflows."""

from typing import Any

from core.temporal.interfaces import StageExecutionResult
from core.yaml_loader.interfaces import StageConfig


def prepare_input_data(
    stage_config: StageConfig,
    stage_data: dict[str, StageExecutionResult],
) -> dict[str, Any] | None:
    """
    Подготавливает входные данные для этапа на основе зависимостей.

    Args:
        stage_config: Конфигурация этапа
        stage_data: Данные от предыдущих этапов

    Returns:
        Словарь с входными данными или None
    """
    if not stage_config.depends_on:
        return None

    input_data = {"dependencies": {}, "records": []}

    for dependency in stage_config.depends_on:
        if dependency not in stage_data:
            continue

        dependency_result = stage_data[dependency]
        input_data["dependencies"][dependency] = dependency_result

        # Извлекаем записи из метаданных зависимости
        if _has_records_in_metadata(dependency_result):
            input_data["records"] = dependency_result.metadata["records"]

    return input_data if input_data["dependencies"] else None


def _has_records_in_metadata(result: StageExecutionResult) -> bool:
    """Проверяет наличие записей в метаданных результата."""
    return (
        hasattr(result, "metadata")
        and bool(result.metadata)
        and "records" in result.metadata
    )
