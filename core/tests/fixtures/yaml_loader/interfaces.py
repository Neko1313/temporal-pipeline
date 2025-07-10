from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.pytest_plugin import register_fixture

from core.yaml_loader.interfaces import PipelineConfig


@register_fixture(name="factory_pipeline_config")
class FactoryPipelineConfig(ModelFactory[PipelineConfig]): ...
