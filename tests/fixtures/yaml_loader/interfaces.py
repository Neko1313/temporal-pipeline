from os import environ

from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.pytest_plugin import register_fixture

from core.temporal import StageExecutionResult
from core.yaml_loader.interfaces import PipelineConfig, StageConfig
from core.yaml_loader.interfaces.schedule import ScheduleConfig


@register_fixture(name="factory_stage_execution_result")
class FactoryStageExecutionResult(ModelFactory[StageExecutionResult]): ...


class FactoryScheduleConfig(ModelFactory[ScheduleConfig]):
    @classmethod
    def interval(cls) -> None:
        return None

    @classmethod
    def cron(cls) -> str:
        return cls.__random__.choice(["0 0 0 0 0", "0 0 0 1 0", "0 0 0 0 1"])


@register_fixture(name="factory_stage_config")
class FactoryStageConfig(ModelFactory[StageConfig]):
    @classmethod
    def depends_on(cls) -> list[str]:
        return []

    @classmethod
    def component_config(cls) -> dict:
        return {"test": "${TEST_ENV}"}


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
        environ["TEST_ENV"] = "test"
        return ["TEST_ENV"]
