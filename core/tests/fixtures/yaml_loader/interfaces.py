from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.pytest_plugin import register_fixture

from core.yaml_loader.interfaces import PipelineConfig, StageConfig
from core.yaml_loader.interfaces.schedule import ScheduleConfig


class FactoryScheduleConfig(ModelFactory[ScheduleConfig]):
    @classmethod
    def interval(cls) -> None:
        return None

    @classmethod
    def cron(cls) -> str:
        return cls.__random__.choice(["0 0 0 0 0", "0 0 0 1 0", "0 0 0 0 1"])


class FactoryStageConfig(ModelFactory[StageConfig]):
    @classmethod
    def depends_on(cls) -> list[str]:
        return []


@register_fixture(name="factory_pipeline_config")
class FactoryPipelineConfig(ModelFactory[PipelineConfig]):
    __model__ = PipelineConfig

    @classmethod
    def schedule(cls) -> ScheduleConfig:
        return FactoryScheduleConfig.build()

    @classmethod
    def stages(cls) -> dict[str, StageConfig]:
        return {"test": FactoryStageConfig.build()}

    @classmethod
    def required_env_vars(cls) -> list[str]:
        return []
