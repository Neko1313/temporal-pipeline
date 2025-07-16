"""Построение результатов выполнения пайплайна."""

from datetime import datetime
from typing import Any

from temporalio import workflow

from core.enums import ExecutionStatus
from core.temporal.interfaces import (
    ErrorMetadata,
    PipelineExecutionResult,
    StageExecutionResult,
)
from core.yaml_loader.interfaces import PipelineConfig


def build_success_result(
    pipeline_config: PipelineConfig,
    run_id: str,
    start_time: datetime,
    stage_results: list[StageExecutionResult],
    execution_metadata: dict[str, Any],
) -> PipelineExecutionResult:
    """
    Создает результат успешного выполнения пайплайна.

    Args:
        pipeline_config: Конфигурация пайплайна
        run_id: Идентификатор запуска
        start_time: Время начала выполнения
        stage_results: Результаты выполнения этапов
        execution_metadata: Метаданные выполнения

    Returns:
        Результат выполнения пайплайна
    """
    stats = _calculate_execution_stats(stage_results)
    execution_time = _calculate_total_time(start_time)

    workflow.logger.info(
        "Pipeline %s completed successfully", pipeline_config.name
    )

    return PipelineExecutionResult(
        pipeline_name=pipeline_config.name,
        run_id=run_id,
        status=ExecutionStatus.SUCCESS,
        total_records_processed=stats["total_records"],
        total_execution_time=execution_time,
        stage_results=stage_results,
        execution_metadata=_enrich_success_metadata(
            execution_metadata, stats, execution_time
        ),
    )


def build_error_result(
    error_metadata: ErrorMetadata,
) -> PipelineExecutionResult:
    """
    Создает результат при ошибке выполнения пайплайна.

    Args:
        error_metadata: Исключение, вызвавшее ошибку

    Returns:
        Результат выполнения пайплайна с ошибкой
    """

    workflow.logger.error(
        "Pipeline %s failed: %s",
        error_metadata.pipeline_config.name,
        str(error_metadata.error),
    )

    return PipelineExecutionResult(
        pipeline_name=error_metadata.pipeline_config.name,
        run_id=error_metadata.run_id,
        status=ExecutionStatus.FAILED,
        total_execution_time=_calculate_total_time(error_metadata.start_time),
        stage_results=error_metadata.stage_results,
        error_message=str(error_metadata.error),
        execution_metadata=_enrich_error_metadata(
            error_metadata.metadata,
            _calculate_execution_stats(error_metadata.stage_results),
            _calculate_total_time(error_metadata.start_time),
            error_metadata.error,
        ),
    )


def _calculate_execution_stats(
    stage_results: list[StageExecutionResult],
) -> dict[str, int]:
    """Вычисляет статистику выполнения."""
    total_records = sum(r.records_processed for r in stage_results)
    successful_stages = len([r for r in stage_results if r.is_success])
    failed_stages = len([r for r in stage_results if r.is_failed])

    return {
        "total_records": total_records,
        "successful_stages": successful_stages,
        "failed_stages": failed_stages,
    }


def _calculate_total_time(start_time: datetime) -> float:
    """Вычисляет общее время выполнения в секундах."""
    return (workflow.now() - start_time).total_seconds()


def _enrich_success_metadata(
    metadata: dict[str, Any],
    stats: dict[str, int],
    execution_time: float,
) -> dict[str, Any]:
    """Обогащает метаданные для успешного выполнения."""
    metadata.update(
        {
            "actual_execution_time": execution_time,
            "stages_executed": stats["successful_stages"],
            "stages_failed": stats["failed_stages"],
            "total_records": stats["total_records"],
        }
    )

    return metadata


def _enrich_error_metadata(
    metadata: dict[str, Any],
    stats: dict[str, int],
    execution_time: float,
    error: Exception,
) -> dict[str, Any]:
    """Обогащает метаданные для ошибочного выполнения."""
    metadata.update(
        {
            "actual_execution_time": execution_time,
            "failure_reason": type(error).__name__,
            "stages_executed": stats["successful_stages"],
            "stages_failed": stats["failed_stages"],
        }
    )

    return metadata
