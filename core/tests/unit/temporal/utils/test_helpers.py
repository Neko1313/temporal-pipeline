import pytest

from core.temporal import StageExecutionResult
from core.temporal.utils.helpers import prepare_input_data
from tests.fixtures.yaml_loader.interfaces import FactoryStageConfig, FactoryStageExecutionResult

def test_prepare_input_data(factory_stage_config: FactoryStageConfig, factory_stage_execution_result: FactoryStageExecutionResult) -> None:
    stage_name = "test_name"
    stage_config = factory_stage_config.build(depends_on=[stage_name])
    stage_execution_result = factory_stage_execution_result.build()
    result_prepare_input_data = prepare_input_data(stage_config, {stage_name: stage_execution_result})
    assert result_prepare_input_data is not None

def test_prepare_input_data_not_depends_on(factory_stage_config: FactoryStageConfig, factory_stage_execution_result: FactoryStageExecutionResult) -> None:
    stage_name = "test_name"
    stage_config = factory_stage_config.build()
    stage_execution_result = factory_stage_execution_result.build()
    result_prepare_input_data = prepare_input_data(stage_config, {stage_name: stage_execution_result})
    assert result_prepare_input_data is None

def test_prepare_input_data_not_depends(factory_stage_config: FactoryStageConfig) -> None:
    stage_config = factory_stage_config.build()
    result_prepare_input_data = prepare_input_data(stage_config, {})
    assert result_prepare_input_data is None