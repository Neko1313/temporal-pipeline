import pytest

from core.temporal.utils.execution import build_execution_order
from tests.fixtures.yaml_loader.interfaces import FactoryStageConfig


@pytest.mark.asyncio
async def test_build_execution_order(
    factory_stage_config: FactoryStageConfig,
) -> None:
    stage_config = factory_stage_config.build()
    name_stage = "test_stage"

    result_build_execution_order = build_execution_order(
        {name_stage: stage_config}
    )
    assert result_build_execution_order == [[name_stage]]


@pytest.mark.asyncio
async def test_build_execution_order_circular_dependency(
    factory_stage_config: FactoryStageConfig,
) -> None:
    stage_config_1_name = "test_stage_1"
    stage_config_2_name = "test_stage_2"
    stage_config_1 = factory_stage_config.build()
    stage_config_2 = factory_stage_config.build()

    stage_config_1.depends_on = [stage_config_2_name]
    stage_config_2.depends_on = [stage_config_1_name]

    with pytest.raises(
        ValueError, match="Circular dependency detected in stages"
    ) as ve:
        build_execution_order(
            {
                stage_config_1_name: stage_config_1,
                stage_config_2_name: stage_config_2,
            }
        )

    assert str(ve.value) == "Circular dependency detected in stages"
