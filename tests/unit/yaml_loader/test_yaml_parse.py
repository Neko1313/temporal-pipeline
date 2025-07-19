import logging
import os
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest
import yaml

from core.yaml_loader import YAMLConfigParser
from tests.fixtures.yaml_loader.interfaces import FactoryPipelineConfig


@pytest.fixture
def get_config_yml_parse_path_file(
    factory_pipeline_config: FactoryPipelineConfig,
) -> Generator[str]:
    model_pipeline_config = factory_pipeline_config.build()

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False, encoding="utf-8"
    ) as temp_file:
        yaml.dump(
            model_pipeline_config.model_dump(mode="json"),
            temp_file,
            indent=2,
        )
        temp_file_path = temp_file.name

    try:
        yield temp_file_path
    finally:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def test_yaml_loader_parse_file(get_config_yml_parse_path_file: str) -> None:
    yaml_config_parser_result = YAMLConfigParser.parse_file(
        Path(get_config_yml_parse_path_file)
    )
    logging.info(yaml_config_parser_result)
