from temporalio import workflow


def should_continue_on_failure(failed_stage: str, config) -> bool:
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
            f"Stopping pipeline: stages {dependent_stages} depend on failed stage {failed_stage}"
        )
        return False

    return False
