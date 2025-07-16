"""Обработка ошибок в этапах пайплайна."""

from temporalio import workflow

from core.yaml_loader.interfaces import PipelineConfig


def should_continue_on_failure(
    failed_stage: str,
    config: PipelineConfig,
) -> bool:
    stage_config = config.stages.get(failed_stage)
    if not stage_config:
        return False

    dependent_stages = _find_dependent_stages(failed_stage, config)

    if dependent_stages:
        workflow.logger.info(
            "Stopping pipeline: stages %s depend on failed stage %s",
            dependent_stages,
            failed_stage,
        )
        return False

    return False


def _find_dependent_stages(
    stage_name: str,
    config: PipelineConfig,
) -> list[str]:
    """Находит этапы, зависящие от указанного."""
    return [
        name
        for name, stage in config.stages.items()
        if stage_name in stage.depends_on
    ]
