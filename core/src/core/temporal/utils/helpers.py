"""Вспомогательные функции для Temporal workflows."""
from logging import getLogger
from typing import Any

from core.temporal.interfaces import StageExecutionResult
from core.yaml_loader.interfaces import StageConfig

logger = getLogger(__name__)

def prepare_input_data(
    stage_config: StageConfig,
    stage_data: dict[str, StageExecutionResult],
) -> dict[str, Any] | None:
    if not stage_config.depends_on:
        return None

    input_data = {"dependencies": {}, "records": []}

    for dependency in stage_config.depends_on:
        if dependency not in stage_data:
            logger.warning(
                f"Dependency '{dependency}' not found in stage_data"
            )
            continue

        dependency_result = stage_data[dependency]
        input_data["dependencies"][dependency] = dependency_result

        if _has_records_in_metadata(dependency_result):
            input_data["records"] = dependency_result.metadata["records"]

    return input_data if input_data["dependencies"] else None


def _has_records_in_metadata(result: StageExecutionResult) -> bool:
    return (
        hasattr(result, "metadata")
        and bool(result.metadata)
        and result.metadata is not None
        and "records" in result.metadata
    )
