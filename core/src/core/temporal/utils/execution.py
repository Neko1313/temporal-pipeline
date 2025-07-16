"""Управление выполнением этапов пайплайна."""

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError

from core.enums import ExecutionStatus
from core.temporal.activities import stage_activity
from core.temporal.interfaces import StageActivity, StageExecutionResult
from core.temporal.utils.helpers import prepare_input_data
from core.temporal.utils.retry import create_retry_policy
from core.yaml_loader.interfaces import PipelineConfig, StageConfig


def build_execution_order(stages: dict[str, StageConfig]) -> list[list[str]]:
    """
    Определяет порядок выполнения этапов с учетом зависимостей.

    Args:
        stages: Словарь конфигураций этапов

    Returns:
        Список батчей для параллельного выполнения

    Raises:
        ValueError: При обнаружении циклических зависимостей
    """
    execution_order = []
    remaining_stages = set(stages.keys())

    while remaining_stages:
        ready_stages = _find_ready_stages(stages, remaining_stages)

        if not ready_stages:
            msg = "Circular dependency detected in stages"
            raise ValueError(msg)

        execution_order.append(ready_stages)
        remaining_stages -= set(ready_stages)

    return execution_order


def init_execution_metadata() -> dict[str, Any]:
    """Инициализирует метаданные выполнения."""
    workflow_info = workflow.info()

    return {
        "workflow_id": workflow_info.workflow_id,
        "run_number": workflow_info.run_id,
        "scheduled": False,
    }


async def execute_stage_batch(
    stage_batch: list[str],
    pipeline_config: PipelineConfig,
    run_id: str,
    stage_data: dict[str, StageExecutionResult],
) -> list[StageExecutionResult]:
    """
    Выполняет батч этапов параллельно.

    Args:
        stage_batch: Список имен этапов для выполнения
        pipeline_config: Конфигурация пайплайна
        run_id: Идентификатор запуска
        stage_data: Данные от предыдущих этапов

    Returns:
        Список результатов выполнения
    """
    tasks = _create_stage_tasks(
        stage_batch, pipeline_config, run_id, stage_data
    )

    return await _execute_tasks_with_error_handling(tasks)


def _find_ready_stages(
    stages: dict[str, StageConfig],
    remaining_stages: set[str],
) -> list[str]:
    """Находит этапы, готовые к выполнению."""
    ready_stages = []

    for stage_name in remaining_stages:
        stage = stages[stage_name]
        unresolved_deps = set(stage.depends_on) & remaining_stages

        if not unresolved_deps:
            ready_stages.append(stage_name)

    return ready_stages


def _create_stage_tasks(
    stage_batch: list[str],
    pipeline_config: PipelineConfig,
    run_id: str,
    stage_data: dict[str, StageExecutionResult],
) -> list[tuple[str, Any]]:
    """Создает задачи для выполнения этапов."""
    tasks = []

    for stage_name in stage_batch:
        task = _create_single_stage_task(
            stage_name, pipeline_config, run_id, stage_data
        )
        tasks.append((stage_name, task))

    return tasks


def _create_single_stage_task(
    stage_name: str,
    pipeline_config: PipelineConfig,
    run_id: str,
    stage_data: dict[str, StageExecutionResult],
) -> Any:
    """Создает задачу для выполнения одного этапа."""
    stage_config = pipeline_config.stages[stage_name]
    resilience_config = pipeline_config.get_effective_resilience(stage_name)

    return workflow.execute_activity(
        stage_activity,
        args=[
            StageActivity(
                stage_name=stage_name,
                stage_config=stage_config,
                run_id=run_id,
                pipeline_name=pipeline_config.name,
                input_data=prepare_input_data(stage_config, stage_data),
                resilience_config=resilience_config,
            ),
        ],
        start_to_close_timeout=timedelta(
            seconds=resilience_config.execution_timeout
            or pipeline_config.default_timeout
        ),
        retry_policy=create_retry_policy(resilience_config),
    )


async def _execute_tasks_with_error_handling(
    tasks: list[tuple[str, Any]],
) -> list[StageExecutionResult]:
    """Выполняет задачи с обработкой ошибок."""
    results = []

    for stage_name, task in tasks:
        try:
            result = await task
            workflow.logger.info(
                "Stage %s completed: %s records",
                stage_name,
                result.records_processed,
            )
            results.append(result)

        except ActivityError as e:
            failed_result = _create_activity_error_result(stage_name, e)
            workflow.logger.error("Stage %s failed: %s", stage_name, str(e))
            results.append(failed_result)

    return results


def _create_activity_error_result(
    stage_name: str,
    error: ActivityError,
) -> StageExecutionResult:
    """Создает результат для ошибки activity."""
    return StageExecutionResult(
        stage_name=stage_name,
        status=ExecutionStatus.FAILED,
        records_processed=0,
        execution_time=0.0,
        error_message=str(error),
        metadata={"error_type": "ActivityError"},
    )
