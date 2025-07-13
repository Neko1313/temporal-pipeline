import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from core.yaml_loader import YAMLConfigParser
from core.yaml_loader.interfaces import PipelineConfig
from tests.fixtures.yaml_loader.interfaces import FactoryStageConfig

SEC_30 = 30
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def test_parse_file_yaml_error() -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    ) as temp_file:
        temp_file.write("invalid: yaml: content:\n  - missing bracket")
        temp_file_path = temp_file.name

    try:
        with pytest.raises(ValueError, match="Invalid YAML syntax"):
            YAMLConfigParser.parse_file(Path(temp_file_path))
    finally:
        os.unlink(temp_file_path)


def test_parse_file_general_exception() -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    ) as temp_file:
        temp_file.write("invalid_structure: true")
        temp_file_path = temp_file.name

    try:
        with pytest.raises(ValueError, match="Error parsing config"):
            YAMLConfigParser.parse_file(Path(temp_file_path))
    finally:
        os.unlink(temp_file_path)


def test_substitute_env_vars_missing_required_var() -> None:
    test_data = {"required_var": "${REQUIRED_VAR}"}

    with (
        patch.dict(os.environ, {}, clear=True),
        pytest.raises(
            ValueError,
            match="Required environment variable not set: REQUIRED_VAR",
        ),
    ):
        YAMLConfigParser._substitute_env_vars(test_data)


def test_substitute_env_vars_with_default_value() -> None:
    test_data = {"host": "${DB_HOST:localhost}", "port": "${DB_PORT:5432}"}

    with patch.dict(os.environ, {}, clear=True):
        result = YAMLConfigParser._substitute_env_vars(test_data)
        assert result["host"] == "localhost"
        assert result["port"] == "5432"


def test_validate_env_vars_missing_variables(
    factory_stage_config: FactoryStageConfig,
) -> None:
    mock_config = PipelineConfig(
        name="error_test",
        stages={"test": factory_stage_config.build()},
        required_env_vars=[
            "MISSING_VAR1",
            "MISSING_VAR2",
        ],
    )

    with (
        patch.dict(os.environ, {}, clear=True),
        pytest.raises(
            ValueError,
            match="Missing required environment "
            "variables: \\['MISSING_VAR1', 'MISSING_VAR2'\\]",
        ),
    ):
        YAMLConfigParser._validate_env_vars(mock_config)


def test_get_env_vars_from_config() -> None:
    config_content = """
    database:
      host: ${DB_HOST:localhost}
      port: ${DB_PORT}
      password: ${DB_PASSWORD:secret}

    api:
      key: ${API_KEY}
    """

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    ) as temp_file:
        temp_file.write(config_content)
        temp_file_path = temp_file.name

    try:
        env_vars = YAMLConfigParser.get_env_vars_from_config(
            Path(temp_file_path)
        )
        expected_vars = {"DB_HOST", "DB_PORT", "DB_PASSWORD", "API_KEY"}
        assert env_vars == expected_vars
    finally:
        os.unlink(temp_file_path)


def test_parse_interval_to_seconds_valid() -> None:
    assert YAMLConfigParser.parse_interval_to_seconds("30s") == SEC_30
    assert YAMLConfigParser.parse_interval_to_seconds("5m") == MINUTE * 5
    assert YAMLConfigParser.parse_interval_to_seconds("2h") == 2 * HOUR
    assert YAMLConfigParser.parse_interval_to_seconds("1d") == DAY


def test_parse_interval_to_seconds_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid interval format: invalid"):
        YAMLConfigParser.parse_interval_to_seconds("invalid")


def test_error_load_file() -> None:
    path = Path(__file__).parent / "error" / "error.yml"
    with pytest.raises(FileNotFoundError) as ex_info:
        YAMLConfigParser.parse_file(path)

    assert str(ex_info.value) == f"Config file not found: {path}"
