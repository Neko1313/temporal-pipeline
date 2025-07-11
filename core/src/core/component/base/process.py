from typing import Protocol, TypeVar

from core.component.base.const import BASE_INFO, ERROR_MESSAGE
from core.component.interfaces import ComponentConfig, Info, Result

T_Config = TypeVar("T_Config", bound=ComponentConfig)


class BaseProcess(Protocol):
    async def process(self) -> Result: ...  # pragma: no cover

    @classmethod
    def info(cls) -> Info: ...  # pragma: no cover


class BaseProcessClass[T_Config: ComponentConfig]:
    config: T_Config

    def __init__(self, config: T_Config) -> None:
        self.config = config

    async def process(self) -> Result | None:
        raise NotImplementedError(ERROR_MESSAGE)

    @classmethod
    def info(cls) -> Info:
        return BASE_INFO
