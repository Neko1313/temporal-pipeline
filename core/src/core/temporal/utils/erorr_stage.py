from temporalio import workflow

from core.yaml_loader.interfaces import PipelineConfig


def should_continue_on_failure(
    failed_stage: str, config: PipelineConfig
) -> bool:
    stage_config = config.stages.get(failed_stage)
    if not stage_config:
        return False

    dependent_stages = [
        name
        for name, stage in config.stages.items()
        if failed_stage in stage.depends_on
    ]

    if dependent_stages:
        workflow.logger.info(
            "Stopping pipeline: stages %s depend on failed stage %s",
            dependent_stages,
            failed_stage,
        )
        return False

    return False
