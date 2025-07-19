"""Workflow для запуска пайплайна по расписанию."""

import uuid
from datetime import datetime

from temporalio import workflow

from core.temporal.interfaces import PipelineExecutionResult
from core.temporal.workflow import DataPipelineWorkflow
from core.yaml_loader.interfaces import PipelineConfig


@workflow.defn
class ScheduledPipelineWorkflow:
    """Workflow для выполнения пайплайна по расписанию."""

    @workflow.run
    async def run(
        self,
        pipeline_config: PipelineConfig,
        schedule_id: str,
        scheduled_time: str,
    ) -> PipelineExecutionResult:
        """Запускает пайплайн по расписанию."""
        run_id = _generate_scheduled_run_id(schedule_id, scheduled_time)

        workflow.logger.info(
            "Starting scheduled pipeline execution: %s", run_id
        )

        _update_execution_metadata(
            pipeline_config, schedule_id, scheduled_time
        )

        result = await workflow.execute_child_workflow(
            DataPipelineWorkflow.run,
            args=[pipeline_config, run_id],
            id=f"pipeline-{run_id}",
        )

        _enrich_result_metadata(result, schedule_id, scheduled_time)

        return result


def _generate_scheduled_run_id(
    schedule_id: str,
    scheduled_time: str,
) -> str:
    """Генерирует уникальный идентификатор для запуска по расписанию."""
    timestamp = datetime.fromisoformat(scheduled_time.replace("Z", "+00:00"))

    return (
        f"scheduled_{schedule_id}_"
        f"{timestamp.strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )


def _update_execution_metadata(
    pipeline_config: PipelineConfig,
    schedule_id: str,
    scheduled_time: str,
) -> None:
    """Обновляет метаданные выполнения для запуска по расписанию."""
    if not hasattr(pipeline_config, "execution_metadata"):
        pipeline_config.execution_metadata = {}

    pipeline_config.execution_metadata.update(
        {
            "scheduled": True,
            "schedule_id": schedule_id,
            "scheduled_time": scheduled_time,
            "execution_type": "scheduled",
        }
    )


def _enrich_result_metadata(
    result: PipelineExecutionResult,
    schedule_id: str,
    scheduled_time: str,
) -> None:
    """Обогащает метаданные результата информацией о расписании."""
    if result.execution_metadata:
        result.execution_metadata.update(
            {
                "scheduled": True,
                "schedule_id": schedule_id,
                "scheduled_time": scheduled_time,
            }
        )
