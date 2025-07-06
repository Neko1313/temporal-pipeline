import uuid
from datetime import datetime

from temporalio import workflow

from core.temporal.interfaces import PipelineExecutionResult
from core.temporal.workflow import DataPipelineWorkflow
from core.yaml_loader.interfaces import PipelineConfig


@workflow.defn
class ScheduledPipelineWorkflow:
    @workflow.run
    async def run(
        self,
        pipeline_config: PipelineConfig,
        schedule_id: str,
        scheduled_time: str,
    ) -> PipelineExecutionResult:
        timestamp = datetime.fromisoformat(
            scheduled_time.replace("Z", "+00:00")
        )
        run_id = f"scheduled_{schedule_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"  # noqa: E501

        workflow.logger.info(
            f"Starting scheduled pipeline execution: {run_id}"
        )

        if "execution_metadata" not in pipeline_config:
            pipeline_config["execution_metadata"] = {}

        pipeline_config["execution_metadata"].update(
            {
                "scheduled": True,
                "schedule_id": schedule_id,
                "scheduled_time": scheduled_time,
                "execution_type": "scheduled",
            }
        )

        result = await workflow.execute_child_workflow(
            DataPipelineWorkflow.run,
            args=[pipeline_config, run_id],
            id=f"pipeline-{run_id}",
        )

        if result.execution_metadata:
            result.execution_metadata.update(
                {
                    "scheduled": True,
                    "schedule_id": schedule_id,
                    "scheduled_time": scheduled_time,
                }
            )

        return result
