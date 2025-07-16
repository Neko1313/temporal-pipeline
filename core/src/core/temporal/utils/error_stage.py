"""Обработка ошибок в этапах пайплайна."""

from core.yaml_loader.interfaces import PipelineConfig


def should_continue_on_failure(
    failed_stage: str,
    config: PipelineConfig,
) -> bool:
    stage_config = config.stages.get(failed_stage)
    if not stage_config:
        return False

    return False
