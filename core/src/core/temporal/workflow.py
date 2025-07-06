from temporalio import workflow
from temporalio.exceptions import ApplicationError

from core.temporal.interfaces import PipelineExecutionResult
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
    @workflow.run
    async def run(
        self, pipeline_config: PipelineConfig, run_id: str
    ) -> PipelineExecutionResult:
        workflow.logger.info(
            f"Starting pipeline: {pipeline_config.name}, run_id: {run_id}"
        )

        start_time = workflow.now()
        stage_results = []
        execution_metadata = init_execution_metadata()

        try:
            execution_order = build_execution_order(pipeline_config.stages)
            execution_metadata.update(
                {
                    "total_stages": len(pipeline_config.stages),
                    "execution_batches": len(execution_order),
                    "max_parallel_stages": pipeline_config.max_parallel_stages,
                }
            )

            stage_data = {}

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

                # Обработка результатов батча
                for result in batch_results:
                    stage_results.append(result)
                    if result.status == "success":
                        stage_data[result.stage_name] = result
                    elif not should_continue_on_failure(
                        result.stage_name, pipeline_config
                    ):
                        msg = (
                            f"Critical stage {result.stage_name} "
                            f"failed: {result.error_message}"
                        )
                        raise ApplicationError(msg)

            return build_success_result(
                pipeline_config,
                run_id,
                start_time,
                stage_results,
                execution_metadata,
            )

        except Exception as e:
            return build_error_result(
                pipeline_config,
                run_id,
                start_time,
                stage_results,
                execution_metadata,
                e,
            )
