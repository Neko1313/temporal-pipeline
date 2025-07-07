import pytest

from core.component import BaseProcessClass, ComponentConfig
from core.component.base.const import BASE_INFO, ERROR_MESSAGE


def test_base_process_config() -> None:
    config = ComponentConfig()
    base_process = BaseProcessClass(config)

    assert base_process.config == config


async def test_raise_fin_process() -> None:
    base_process = BaseProcessClass(ComponentConfig())
    with pytest.raises(NotImplementedError) as nie:
        await base_process.process()

    assert ERROR_MESSAGE in str(nie.value)


def test_info_process() -> None:
    base_process = BaseProcessClass(ComponentConfig())
    assert base_process.info() == BASE_INFO
