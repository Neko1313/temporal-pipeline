from collections import defaultdict
from importlib.metadata import entry_points

from core.component.interfaces import Info
from core.component.base import BaseProcessClass


class PluginRegistry:
    def __init__(self):
        self._plugins: dict[str, dict[str, Info]] = defaultdict(dict)
        self._plugin_groups = {
            "extract": BaseProcessClass,
            "transform": BaseProcessClass,
            "load": BaseProcessClass,
        }

    async def initialize(self):
        await self.discover_all_plugins()

    async def discover_all_plugins(self) -> dict[str, int]:
        discovered_count = {}

        for group_name, expected_interface in self._plugin_groups.items():
            count = await self._discover_plugin_group(group_name)
            discovered_count[group_name] = count

        return discovered_count

    async def _discover_plugin_group(self, group_name: str) -> int:
        loaded_count = 0
        group_entries = entry_points(group=group_name)
        try:
            for entry_point in group_entries:
                plugin_class: type[BaseProcessClass] = entry_point.load()
                object_plugin_class = plugin_class()

                if group_name != object_plugin_class.info.type_module:
                    raise Exception("Plugin info error")

                self._plugins[group_name][entry_point.name] = object_plugin_class.info
                loaded_count += 1

        except Exception as e:
            print(e)

        return loaded_count
