from core.temporal.utils.error_stage import should_continue_on_failure
from tests.fixtures.yaml_loader.interfaces import FactoryPipelineConfig


def test_should_continue_on_failure(
    factory_pipeline_config: FactoryPipelineConfig,
) -> None:
    pipeline_config = factory_pipeline_config.build()
    result_should_continue_on_failure = should_continue_on_failure(
        next(iter(pipeline_config.stages)), pipeline_config
    )
    assert result_should_continue_on_failure is False


def test_should_continue_on_failure_stage_not_found(
    factory_pipeline_config: FactoryPipelineConfig,
) -> None:
    pipeline_config = factory_pipeline_config.build()
    result_should_continue_on_failure = should_continue_on_failure(
        "stage_not_found", pipeline_config
    )
    assert result_should_continue_on_failure is False
