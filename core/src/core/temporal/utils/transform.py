from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from core.temporal.activities import execute_stage_activity
from core.temporal.interfaces import (
    PipelineExecutionResult,
    StageExecutionResult,
)
from core.yaml_loader.interfaces import (
    PipelineConfig,
    ResilienceConfig,
    StageConfig,
)


def _create_retry_policy(resilience_config: ResilienceConfig) -> RetryPolicy:
    initial_interval = timedelta(seconds=resilience_config.initial_delay)
    maximum_interval = timedelta(seconds=resilience_config.max_delay)

    if resilience_config.retry_policy == "exponential_backoff":
        backoff_coefficient = resilience_config.backoff_multiplier
    elif resilience_config.retry_policy == "linear_backoff":
        backoff_coefficient = 1.0
    elif resilience_config.retry_policy == "fixed_interval":
        backoff_coefficient = 1.0
        maximum_interval = initial_interval
    elif resilience_config.retry_policy == "fibonacci_backoff":
        backoff_coefficient = 1.618
    else:
        backoff_coefficient = resilience_config.backoff_multiplier

    return RetryPolicy(
        initial_interval=initial_interval,
        maximum_interval=maximum_interval,
        maximum_attempts=resilience_config.max_attempts,
        backoff_coefficient=backoff_coefficient,
        non_retryable_error_types=[
            "ConfigurationError",
            "AuthenticationError",
            "PermissionError",
        ],
    )


def _prepare_input_data(
    stage_config: StageConfig,
    stage_data: dict[str, Any],  # Изменено: any -> Any
) -> dict[str, Any] | None:  # Изменено: any -> Any
    if not stage_config.depends_on:
        return None

    input_data = {"dependencies": {}, "records": []}

    for dependency in stage_config.depends_on:
        if dependency in stage_data:
            input_data["dependencies"][dependency] = stage_data[dependency]
            dependency_result = stage_data[dependency]
            if (
                hasattr(dependency_result, "metadata")
                and dependency_result.metadata
            ) and "records" in dependency_result.metadata:
                input_data["records"] = dependency_result.metadata["records"]

    return input_data if input_data["dependencies"] else None


def build_execution_order(stages: dict[str, StageConfig]) -> list[list[str]]:
    execution_order = []
    remaining_stages = set(stages.keys())

    while remaining_stages:
        ready_stages = []

        for stage_name in remaining_stages:
            stage = stages[stage_name]
            unresolved_deps = set(stage.depends_on) & remaining_stages

            if not unresolved_deps:
                ready_stages.append(stage_name)

        if not ready_stages:
            msg = "Circular dependency detected in stages"
            raise ValueError(msg)

        execution_order.append(ready_stages)
        remaining_stages -= set(ready_stages)

    return execution_order


def init_execution_metadata() -> dict[str, str | bool | int]:
    return {
        "workflow_id": workflow.info().workflow_id,
        "run_number": workflow.info().run_id,
        "scheduled": False,
    }


async def execute_stage_batch(
    stage_batch: list[str],
    pipeline_config: PipelineConfig,
    run_id: str,
    stage_data: dict[str, StageExecutionResult],
) -> list[StageExecutionResult]:
    batch_tasks = []
    for stage_name in stage_batch:
        task = _create_stage_task(
            stage_name, pipeline_config, run_id, stage_data
        )
        batch_tasks.append((stage_name, task))

    results = []
    for stage_name, task in batch_tasks:
        try:
            result = await task
            workflow.logger.info(
                f"Stage {stage_name} completed: {result.records_processed} records"
            )
            results.append(result)

        except ActivityError as e:
            failed_result = _create_failed_result(
                stage_name, pipeline_config, e
            )
            workflow.logger.error(f"Stage {stage_name} failed: {e!s}")
            results.append(failed_result)

    return results


def _create_stage_task(
    stage_name: str,
    pipeline_config: PipelineConfig,
    run_id: str,
    stage_data: dict[str, StageExecutionResult],
):
    stage_config = pipeline_config.stages[stage_name]
    resilience_config = pipeline_config.get_effective_resilience(stage_name)

    return workflow.execute_activity(
        execute_stage_activity,
        args=[
            stage_name,
            stage_config.model_dump(),
            run_id,
            pipeline_config.name,
            _prepare_input_data(stage_config, stage_data),
            resilience_config.model_dump(),
        ],
        start_to_close_timeout=timedelta(
            seconds=resilience_config.execution_timeout
            or pipeline_config.default_timeout
        ),
        retry_policy=_create_retry_policy(resilience_config),
    )


def _create_failed_result(
    stage_name: str, pipeline_config: PipelineConfig, error: Exception
) -> StageExecutionResult:
    resilience_config = pipeline_config.get_effective_resilience(stage_name)

    return StageExecutionResult(
        stage_name=stage_name,
        status="failed",
        records_processed=0,
        execution_time=0.0,
        error_message=str(error),
        metadata={"retry_attempts": resilience_config.max_attempts},
    )


def _calculate_execution_stats(
    stage_results: list[StageExecutionResult],
) -> tuple[int, int, int]:
    """Вычисление статистики выполнения"""
    total_records = sum(r.records_processed for r in stage_results)
    successful_stages = len(
        [r for r in stage_results if r.status == "success"]
    )
    failed_stages = len([r for r in stage_results if r.status == "failed"])

    return total_records, successful_stages, failed_stages


def build_success_result(
    pipeline_config: PipelineConfig,
    run_id: str,
    start_time,
    stage_results: list[StageExecutionResult],
    execution_metadata: dict[str, Any],
) -> PipelineExecutionResult:
    """Построение результата успешного выполнения"""
    execution_time = (workflow.now() - start_time).total_seconds()
    total_records, successful_stages, failed_stages = (
        _calculate_execution_stats(stage_results)
    )

    execution_metadata.update(
        {
            "actual_execution_time": execution_time,
            "stages_executed": successful_stages,
            "stages_failed": failed_stages,
        }
    )

    workflow.logger.info(
        f"Pipeline {pipeline_config.name} completed successfully"
    )

    return PipelineExecutionResult(
        pipeline_name=pipeline_config.name,
        run_id=run_id,
        status="success",
        total_records_processed=total_records,
        total_execution_time=execution_time,
        stage_results=stage_results,
        execution_metadata=execution_metadata,
    )


def build_error_result(
    pipeline_config: PipelineConfig,
    run_id: str,
    start_time,
    stage_results: list[StageExecutionResult],
    execution_metadata: dict[str, Any],  # Изменено: any -> Any
    error: Exception,
) -> PipelineExecutionResult:
    """Построение результата при ошибке выполнения"""
    execution_time = (workflow.now() - start_time).total_seconds()
    _, successful_stages, _ = _calculate_execution_stats(stage_results)

    execution_metadata.update(
        {
            "actual_execution_time": execution_time,
            "failure_reason": type(error).__name__,
            "stages_executed": successful_stages,
        }
    )

    workflow.logger.error(f"Pipeline {pipeline_config.name} failed: {error!s}")

    return PipelineExecutionResult(
        pipeline_name=pipeline_config.name,
        run_id=run_id,
        status="failed",
        total_execution_time=execution_time,
        stage_results=stage_results,
        error_message=str(error),
        execution_metadata=execution_metadata,
    )
