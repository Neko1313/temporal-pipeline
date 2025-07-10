import os
import re
from pathlib import Path
from typing import Any

import yaml

from core.yaml_loader.interfaces import PipelineConfig

ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


class YAMLConfigParser:
    @classmethod
    def parse_file(cls, config_path: Path) -> PipelineConfig:
        if not config_path.exists():
            msg = f"Config file not found: {config_path}"
            raise FileNotFoundError(msg)

        try:
            with open(config_path, encoding="utf-8") as file:
                raw_config = yaml.safe_load(file)

            processed_config = cls._substitute_env_vars(raw_config)

            pipeline_config = PipelineConfig(**processed_config)

            cls._validate_env_vars(pipeline_config)

            return pipeline_config

        except yaml.YAMLError as ye:
            msg = f"Invalid YAML syntax in {config_path}: {ye}"
            raise ValueError(msg) from ye
        except Exception as ex:
            msg = f"Error parsing config {config_path}: {ex}"
            raise ValueError(msg) from ex

    @classmethod
    def _substitute_env_vars(cls, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: cls._substitute_env_vars(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cls._substitute_env_vars(item) for item in obj]
        if isinstance(obj, str):
            return cls._substitute_string_env_vars(obj)
        return obj

    @staticmethod
    def _substitute_string_env_vars(text: str) -> str:
        def replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1)

            if ":" in var_name:
                var_name, default_value = var_name.split(":", 1)
                return os.environ.get(var_name, default_value)
            value = os.environ.get(var_name)
            if value is None:
                msg = f"Required environment variable not set: {var_name}"
                raise ValueError(msg)
            return value

        return ENV_VAR_PATTERN.sub(replace_var, text)

    @staticmethod
    def _validate_env_vars(config: PipelineConfig) -> None:
        missing_vars = []

        for var_name in config.required_env_vars:
            if var_name not in os.environ:
                missing_vars.append(var_name)

        if missing_vars:
            msg = f"Missing required environment variables: {missing_vars}"
            raise ValueError(msg)

    @staticmethod
    def get_env_vars_from_config(config_path: Path) -> set[str]:
        with open(config_path, encoding="utf-8") as f:
            content = f.read()

        env_vars = set()
        for match in ENV_VAR_PATTERN.finditer(content):
            var_name = match.group(1)
            if ":" in var_name:
                var_name = var_name.split(":", 1)[0]
            env_vars.add(var_name)

        return env_vars

    @staticmethod
    def parse_interval_to_seconds(interval: str) -> int:
        pattern = r"^(\d+)([smhd])$"
        match = re.match(pattern, interval)

        if not match:
            msg = f"Invalid interval format: {interval}"
            raise ValueError(msg)

        value, unit = match.groups()
        value = int(value)

        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}

        return value * multipliers[unit]
