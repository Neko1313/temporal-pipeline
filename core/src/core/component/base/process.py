from typing import Protocol

from core.component.interfaces import ComponentConfig, Info, Result


class BaseProcess(Protocol):
    async def process(self) -> Result: ...

    @property
    def info(self) -> Info: ...


class BaseProcessClass:
    config: ComponentConfig

    def __init__(self, config: ComponentConfig) -> None:
        self.config = config

    async def process(self) -> Result | None:
        raise NotImplementedError()

    @classmethod
    def info(cls) -> Info:
        return Info(
            name="base",
            version="0.1.0",
            description=None,
            type_class=cls.__class__,
            type_module="core",
            config_class=ComponentConfig,
        )
