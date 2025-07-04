from typing import Protocol
from core.component.interfaces import ComponentConfig, Info, Result


class BaseProcess(Protocol):
    async def process(self) -> Result: ...

    @property
    def info(self) -> Info: ...


class BaseProcessClass:
    config: ComponentConfig

    async def process(self) -> Result:
        raise NotImplementedError()

    @property
    def info(self) -> Info:
        return Info(
            name="base",
            version="0.1.0",
            description=None,
            type_class=self.__class__,
            type_module="core",
            config_class=self.config,
        )
