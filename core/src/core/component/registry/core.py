"""
Обновленный PluginRegistry с дополнительными методами для универсальной архитектуры
"""

from collections import defaultdict
from importlib.metadata import entry_points
from typing import Dict, Optional, Type, List, Any

from core.component.interfaces import Info, ComponentConfig
from core.component.base import BaseProcessClass


class PluginRegistry:
    def __init__(self):
        self._plugins: dict[str, dict[str, Info]] = defaultdict(dict)
        self._plugin_classes: dict[str, dict[str, Type[BaseProcessClass]]] = (
            defaultdict(dict)
        )  # ДОБАВЛЕНО
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
                self._plugin_classes[group_name][entry_point.name] = (
                    plugin_class  # ДОБАВЛЕНО
                )

                loaded_count += 1

        except Exception as e:
            print(e)

        return loaded_count

    def get_plugin(
        self, plugin_type: str, plugin_name: str
    ) -> Optional[Type[BaseProcessClass]]:
        """
        Получает класс плагина по типу и имени

        Args:
            plugin_type: Тип плагина (extract, transform, load)
            plugin_name: Имя плагина

        Returns:
            Класс плагина или None если не найден
        """
        return self._plugin_classes.get(plugin_type, {}).get(plugin_name)

    def get_plugin_info(self, plugin_type: str, plugin_name: str) -> Optional[Info]:
        """
        Получает информацию о плагине

        Args:
            plugin_type: Тип плагина (extract, transform, load)
            plugin_name: Имя плагина

        Returns:
            Информация о плагине или None если не найден
        """
        return self._plugins.get(plugin_type, {}).get(plugin_name)

    def list_plugins(self, plugin_type: Optional[str] = None) -> Dict[str, List[str]]:
        """
        Возвращает список доступных плагинов

        Args:
            plugin_type: Тип плагина для фильтрации (optional)

        Returns:
            Словарь с типами плагинов и их именами
        """
        if plugin_type:
            return {plugin_type: list(self._plugins.get(plugin_type, {}).keys())}

        return {ptype: list(plugins.keys()) for ptype, plugins in self._plugins.items()}

    async def validate_component_config(
        self, plugin_type: str, plugin_name: str, config_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Валидирует конфигурацию компонента

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
            # Создаем экземпляр для получения info
            plugin_instance = plugin_class()

            # Пытаемся создать конфигурацию
            config = ComponentConfig(**config_data)
            plugin_instance.config = config

            return {
                "valid": True,
                "errors": [],
                "component_info": plugin_instance.info.model_dump()
                if hasattr(plugin_instance.info, "model_dump")
                else None,
            }

        except Exception as e:
            return {"valid": False, "errors": [str(e)], "component_info": None}

    def get_all_plugins(self) -> Dict[str, Dict[str, Info]]:
        """
        Возвращает всю информацию о плагинах

        Returns:
            Полный каталог плагинов с их информацией
        """
        return dict(self._plugins)

    def register_plugin(
        self, plugin_type: str, plugin_name: str, plugin_class: Type[BaseProcessClass]
    ):
        """
        Регистрирует плагин вручную (для тестирования)

        Args:
            plugin_type: Тип плагина
            plugin_name: Имя плагина
            plugin_class: Класс плагина
        """
        if plugin_type not in self._plugin_groups:
            raise ValueError(f"Unknown plugin type: {plugin_type}")

        if not issubclass(plugin_class, BaseProcessClass):
            raise ValueError("Plugin class must inherit from BaseProcessClass")

        # Создаем экземпляр для получения info
        plugin_instance = plugin_class()
        plugin_info = plugin_instance.info

        self._plugins[plugin_type][plugin_name] = plugin_info
        self._plugin_classes[plugin_type][plugin_name] = plugin_class
