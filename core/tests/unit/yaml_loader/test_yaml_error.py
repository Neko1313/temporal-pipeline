from pathlib import Path

import pytest
from core.yaml_loader import YAMLConfigParser


def test_error_load_file() -> None:
    with pytest.raises(FileNotFoundError) as excinfo:
        path = Path(__file__).parent / "error" / "error.yml"
        YAMLConfigParser.parse_file(path)

    assert str(excinfo.value) == f"Config file not found: {path}"

