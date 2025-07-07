from collections import defaultdict
from importlib.metadata import entry_points
from logging import getLogger
from typing import Any

from core.component.base import BaseProcessClass
from core.component.interfaces import ComponentConfig, Info
from core.component.registry.error import RegistryError

logger = getLogger(__name__)


class PluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, dict[str, Info]] = defaultdict(dict)
        self._plugin_classes: dict[str, dict[str, type[BaseProcessClass]]] = (
            defaultdict(dict)
        )
        self._plugin_groups = {
            "extract": BaseProcessClass,
            "transform": BaseProcessClass,
            "load": BaseProcessClass,
        }

    async def initialize(self) -> None:
        await self.discover_all_plugins()

    async def discover_all_plugins(self) -> dict[str, int]:
        discovered_count = {}

        for group_name, _expected_interface in self._plugin_groups.items():
            count = await self._discover_plugin_group(group_name)
            discovered_count[group_name] = count

        return discovered_count

    async def _discover_plugin_group(self, group_name: str) -> int:
        loaded_count = 0
        group_entries = entry_points(group=group_name)
        try:
            for entry_point in group_entries:
                plugin_class: type[BaseProcessClass] = entry_point.load()
                plugin_class_info = plugin_class.info()

                if group_name != plugin_class_info.type_module:
                    msg = "Plugin info error"
                    raise RegistryError(msg)

                self._plugins[group_name][entry_point.name] = plugin_class_info
                self._plugin_classes[group_name][entry_point.name] = (
                    plugin_class
                )

                loaded_count += 1

        except Exception as err:
            logger.debug(err)

        return loaded_count

    def get_plugin(
        self, plugin_type: str, plugin_name: str
    ) -> type[BaseProcessClass] | None:
        """
        Получает класс плагина по типу и имени.

        Args:
            plugin_type: Тип плагина (extract, transform, load)
            plugin_name: Имя плагина

        Returns:
            Класс плагина или None если не найден

        """
        return self._plugin_classes.get(plugin_type, {}).get(plugin_name)

    def get_plugin_info(
        self, plugin_type: str, plugin_name: str
    ) -> Info | None:
        """
        Получает информацию о плагине.

        Args:
            plugin_type: Тип плагина (extract, transform, load)
            plugin_name: Имя плагина

        Returns:
            Информация о плагине или None если не найден

        """
        return self._plugins.get(plugin_type, {}).get(plugin_name)

    def list_plugins(
        self, plugin_type: str | None = None
    ) -> dict[str, list[str]]:
        """
        Возвращает список доступных плагинов.

        Args:
            plugin_type: Тип плагина для фильтрации (optional)

        Returns:
            Словарь с типами плагинов и их именами

        """
        if plugin_type:
            return {
                plugin_type: list(self._plugins.get(plugin_type, {}).keys())
            }

        return {
            ptype: list(plugins.keys())
            for ptype, plugins in self._plugins.items()
        }

    async def validate_component_config(
        self, plugin_type: str, plugin_name: str, config_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Валидирует конфигурацию компонента.

        Args:
            plugin_type: Тип плагина
            plugin_name: Имя плагина
            config_data: Данные конфигурации

        Returns:
            Результат валидации с ошибками (если есть)

        """
        plugin_class = self.get_plugin(plugin_type, plugin_name)
        if not plugin_class:
            return {
                "valid": False,
                "errors": [f"Plugin {plugin_type}.{plugin_name} not found"],
                "component_info": None,
            }

        try:
            plugin_instance = plugin_class(ComponentConfig(**config_data))

            return {
                "valid": True,
                "errors": [],
                "component_info": plugin_instance.info().model_dump()
                if hasattr(plugin_instance.info, "model_dump")
                else None,
            }

        except Exception as e:
            return {"valid": False, "errors": [str(e)], "component_info": None}

    def get_all_plugins(self) -> dict[str, dict[str, Info]]:
        """
        Возвращает всю информацию о плагинах.

        Returns:
            Полный каталог плагинов с их информацией

        """
        return dict(self._plugins)

    def register_plugin(
        self,
        plugin_type: str,
        plugin_name: str,
        plugin_class: type[BaseProcessClass],
    ) -> None:
        """
        Регистрирует плагин вручную (для тестирования).

        Args:
            plugin_type: Тип плагина
            plugin_name: Имя плагина
            plugin_class: Класс плагина

        """
        if plugin_type not in self._plugin_groups:
            msg = f"Unknown plugin type: {plugin_type}"
            raise ValueError(msg)

        if not issubclass(plugin_class, BaseProcessClass):
            msg = "Plugin class must inherit from BaseProcessClass"
            raise ValueError(msg)

        plugin_info = plugin_class.info()

        self._plugins[plugin_type][plugin_name] = plugin_info
        self._plugin_classes[plugin_type][plugin_name] = plugin_class
