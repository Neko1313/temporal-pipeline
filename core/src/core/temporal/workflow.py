"""Основной workflow для выполнения пайплайна."""

from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError

from core.temporal.constants import ExecutionStatus
from core.temporal.interfaces import ErrorMetadata, PipelineExecutionResult
from core.temporal.utils import (
    build_error_result,
    build_execution_order,
    build_success_result,
    execute_stage_batch,
    init_execution_metadata,
    should_continue_on_failure,
)
from core.yaml_loader.interfaces import PipelineConfig


@workflow.defn
class DataPipelineWorkflow:
    """Workflow для выполнения пайплайна обработки данных."""

    @workflow.run
    async def run(
        self,
        pipeline_config: PipelineConfig,
        run_id: str,
    ) -> PipelineExecutionResult:
        """Выполняет пайплайн обработки данных."""
        workflow.logger.info(
            "Starting pipeline: %s, run_id: %s",
            pipeline_config.name,
            run_id,
        )

        execution_context = _create_execution_context(
            pipeline_config, workflow.now()
        )

        try:
            stage_results = await _execute_pipeline_stages(
                pipeline_config, run_id, execution_context
            )

            return build_success_result(
                pipeline_config,
                run_id,
                execution_context["start_time"],
                stage_results,
                execution_context["metadata"],
            )

        except Exception as e:
            return build_error_result(
                ErrorMetadata(
                    pipeline_config=pipeline_config,
                    run_id=run_id,
                    start_time=execution_context["start_time"],
                    stage_results=execution_context.get("stage_results", []),
                    metadata=execution_context["metadata"],
                    error=e,
                )
            )


def _create_execution_context(
    pipeline_config: PipelineConfig,
    start_time: Any,
) -> dict[str, Any]:
    """Создает контекст выполнения пайплайна."""
    execution_order = build_execution_order(pipeline_config.stages)
    metadata = init_execution_metadata()

    metadata.update(
        {
            "total_stages": len(pipeline_config.stages),
            "execution_batches": len(execution_order),
            "max_parallel_stages": pipeline_config.max_parallel_stages,
        }
    )

    return {
        "start_time": start_time,
        "execution_order": execution_order,
        "metadata": metadata,
        "stage_results": [],
    }


async def _execute_pipeline_stages(
    pipeline_config: PipelineConfig,
    run_id: str,
    execution_context: dict[str, Any],
) -> list[Any]:
    """Выполняет все этапы пайплайна."""
    stage_results = []
    stage_data = {}
    execution_order = execution_context["execution_order"]

    for batch_index, stage_batch in enumerate(execution_order):
        workflow.logger.info(
            "Executing batch %s/%s: %s",
            batch_index + 1,
            len(execution_order),
            stage_batch,
        )

        batch_results = await execute_stage_batch(
            stage_batch, pipeline_config, run_id, stage_data
        )

        await _process_batch_results(
            batch_results,
            stage_results,
            stage_data,
            pipeline_config,
        )

    execution_context["stage_results"] = stage_results
    return stage_results


async def _process_batch_results(
    batch_results: list[Any],
    stage_results: list[Any],
    stage_data: dict[str, Any],
    pipeline_config: PipelineConfig,
) -> None:
    """Обрабатывает результаты выполнения батча."""
    for result in batch_results:
        stage_results.append(result)

        if result.status == ExecutionStatus.SUCCESS:
            stage_data[result.stage_name] = result
        elif not should_continue_on_failure(
            result.stage_name, pipeline_config
        ):
            msg = (
                f"Critical stage {result.stage_name} "
                f"failed: {result.error_message}"
            )
            raise ApplicationError(msg)
