from importlib.metadata import entry_points
from unittest.mock import Mock

import pytest

from core.component import PluginRegistry, BaseProcessClass, ComponentConfig, Info


@pytest.fixture
def empty_registry() -> PluginRegistry:
    """Создает пустой реестр без инициализации"""
    return PluginRegistry()

@pytest.fixture
async def base_registry() -> PluginRegistry:
    plugin_registry = PluginRegistry()
    await plugin_registry.initialize()
    return plugin_registry


@pytest.fixture
def count_entry_points_plugins() -> dict[str, int]:
    return {
        "extract": len(entry_points(group="extract")),
        "transform": len(entry_points(group="transform")),
        "load": len(entry_points(group="load")),
    }


async def test_registry_discover_all_plugins(
    base_registry: PluginRegistry, count_entry_points_plugins: dict[str, int]
) -> None:
    registered_plugins = await base_registry.discover_all_plugins()
    assert registered_plugins == count_entry_points_plugins


async def test_registry_discover_plugin_group(
    base_registry: PluginRegistry,
) -> None: ...
