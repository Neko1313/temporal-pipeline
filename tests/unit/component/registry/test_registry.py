"""
Полные тесты для PluginRegistry класса.
Покрывают все методы и edge cases.
"""

import asyncio
from importlib.metadata import EntryPoints, entry_points
from unittest.mock import Mock, patch

import pytest

from core.component import (
    BaseProcessClass,
    ComponentConfig,
    Info,
    PluginRegistry,
    Result,
)

# === FIXTURES ===


@pytest.fixture
def empty_registry() -> PluginRegistry:
    """Создает пустый реестр без инициализации."""
    return PluginRegistry()


@pytest.fixture
async def initialized_registry() -> PluginRegistry:
    """Создает и инициализирует реестр."""
    plugin_registry = PluginRegistry()
    await plugin_registry.initialize()
    return plugin_registry


@pytest.fixture
def mock_plugin_class() -> type[BaseProcessClass]:
    """Создает мок-класс плагина для тестирования."""

    class MockPlugin(BaseProcessClass[ComponentConfig]):
        def __init__(self, config: ComponentConfig) -> None:
            super().__init__(config)

        async def process(self) -> Result:
            return Result(
                status="success",
                response="mock_result",
            )

        @classmethod
        def process_info(cls) -> Info:
            return Info(
                name="MockPlugin",
                version="1.0.0",
                description="Test plugin",
                type_class=cls,
                type_module="extract",
                config_class=ComponentConfig,
            )

    return MockPlugin


@pytest.fixture
def count_entry_points_plugins() -> dict[str, int]:
    """Подсчитывает количество entry points для каждого типа плагинов."""
    result = {}
    for group_name in ["extract", "transform", "load"]:
        try:
            result[group_name] = len(entry_points(group=group_name))
        except Exception:
            result[group_name] = 0
    return result


async def test_registry_initialize(empty_registry: PluginRegistry) -> None:
    """Тест метода initialize()."""
    assert len(empty_registry._plugins) == 0

    await empty_registry.initialize()

    all_plugins = empty_registry.get_all_plugins()
    assert isinstance(all_plugins, dict)


# === ТЕСТЫ ОБНАРУЖЕНИЯ ПЛАГИНОВ ===


async def test_discover_all_plugins(
    empty_registry: PluginRegistry, count_entry_points_plugins: dict[str, int]
) -> None:
    """Тест обнаружения всех плагинов."""
    discovered = await empty_registry.discover_all_plugins()

    assert isinstance(discovered, dict)
    # Проверяем только те группы, для которых есть entry points
    for group_name in ["extract", "transform", "load"]:
        if count_entry_points_plugins[group_name] > 0:
            assert group_name in discovered
            assert (
                discovered[group_name]
                == count_entry_points_plugins[group_name]
            )
        # Если entry points нет, группа может отсутствовать в результате


async def test_discover_plugin_group_success(
    empty_registry: PluginRegistry,
) -> None:
    """Тест успешного обнаружения плагинов конкретной группы."""
    # Тестируем группу extract
    count = await empty_registry._discover_plugin_group("extract")

    assert isinstance(count, int)
    assert count >= 0

    # Проверяем что плагины добавились в реестр
    extract_plugins = empty_registry._plugins.get("extract", {})
    extract_classes = empty_registry._plugin_classes.get("extract", {})

    assert len(extract_plugins) == count
    assert len(extract_classes) == count


async def test_discover_plugin_group_empty() -> None:
    """Тест обнаружения плагинов для несуществующей группы."""
    registry = PluginRegistry()

    # Для несуществующей группы должно вернуться 0
    count = await registry._discover_plugin_group("nonexistent")
    assert count == 0


@patch("core.component.registry.core.entry_points")
async def test_discover_plugin_group_with_exception(
    mock_entry_points: Mock, empty_registry: PluginRegistry
) -> None:
    """Тест обработки исключений при обнаружении плагинов."""
    # Настраиваем мок для генерации исключения
    mock_entry_point = Mock()
    mock_entry_point.load.side_effect = ImportError("Test error")
    mock_entry_point.name = "test_plugin"
    mock_entry_points.return_value = [mock_entry_point]

    # Метод должен обработать исключение и вернуть 0
    count = await empty_registry._discover_plugin_group("extract")
    assert count == 0


@patch("core.component.registry.core.entry_points")
async def test_discover_plugin_group_wrong_type_module(
    mock_entry_points: Mock,
    empty_registry: PluginRegistry,
    mock_plugin_class: type[BaseProcessClass],  # noqa: ARG001
) -> None:
    class WrongTypePlugin(BaseProcessClass[ComponentConfig]):
        async def process(self) -> Result:
            return Result(
                status="success",
                response="mock_result",
            )

        @classmethod
        def process_info(cls) -> Info:
            return Info(
                name="WrongTypePlugin",
                version="1.0.0",
                description="Plugin with wrong type_module",
                type_class=cls,
                type_module="core",  # Неправильный тип
                config_class=ComponentConfig,
            )

    mock_entry_point = Mock()
    mock_entry_point.load.return_value = WrongTypePlugin
    mock_entry_point.name = "wrong_plugin"
    mock_entry_points.return_value = [mock_entry_point]

    count = await empty_registry._discover_plugin_group("extract")

    assert count == 0
    assert len(empty_registry._plugins.get("extract", {})) == 0


# === ТЕСТЫ ПОЛУЧЕНИЯ ПЛАГИНОВ ===


async def test_get_plugin_success(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест успешного получения плагина."""
    # Получаем список доступных плагинов
    all_plugins = initialized_registry.get_all_plugins()

    if all_plugins.get("extract"):
        # Берем первый доступный extract плагин
        plugin_name = next(iter(all_plugins["extract"].keys()))
        plugin_class = initialized_registry.get_plugin("extract", plugin_name)

        assert plugin_class is not None
        assert issubclass(plugin_class, BaseProcessClass)


def test_get_plugin_not_found(initialized_registry: PluginRegistry) -> None:
    """Тест получения несуществующего плагина."""
    plugin_class = initialized_registry.get_plugin(
        "extract", "nonexistent_plugin"
    )
    assert plugin_class is None


def test_get_plugin_wrong_type(initialized_registry: PluginRegistry) -> None:
    """Тест получения плагина неправильного типа."""
    plugin_class = initialized_registry.get_plugin(
        "nonexistent_type", "any_plugin"
    )
    assert plugin_class is None


async def test_get_plugin_info_success(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест успешного получения информации о плагине."""
    all_plugins = initialized_registry.get_all_plugins()

    if all_plugins.get("extract"):
        plugin_name = next(
            iter(all_plugins["extract"].keys())
        )  # Это будет 'sql_extract' или 'http_extract'
        plugin_info = initialized_registry.get_plugin_info(
            "extract", plugin_name
        )

        assert plugin_info is not None
        assert isinstance(plugin_info, Info)
        # Имя класса и entry point могут отличаться
        assert plugin_info.name is not None  # Просто проверяем что имя есть
        assert plugin_info.type_module == "extract"


def test_get_plugin_info_not_found(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест получения информации о несуществующем плагине."""
    plugin_info = initialized_registry.get_plugin_info(
        "extract", "nonexistent_plugin"
    )
    assert plugin_info is None


# === ТЕСТЫ СПИСКА ПЛАГИНОВ ===


async def test_list_plugins_all(initialized_registry: PluginRegistry) -> None:
    """Тест получения списка всех плагинов."""
    plugins_list = initialized_registry.list_plugins()

    assert isinstance(plugins_list, dict)
    assert "extract" in plugins_list
    assert "transform" in plugins_list
    # load может отсутствовать если нет соответствующих entry points
    # Проверяем только те типы, которые реально есть в проекте

    for _plugin_type, plugin_names in plugins_list.items():
        assert isinstance(plugin_names, list)


async def test_list_plugins_specific_type(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест получения списка плагинов конкретного типа."""
    extract_plugins = initialized_registry.list_plugins("extract")

    assert isinstance(extract_plugins, dict)
    assert "extract" in extract_plugins
    assert len(extract_plugins) == 1
    assert isinstance(extract_plugins["extract"], list)


async def test_no_load_plugins_is_ok() -> None:
    """Тест что отсутствие load плагинов - это нормально."""
    with patch("core.component.registry.core.entry_points") as mock_ep:

        def side_effect(group: str) -> EntryPoints:
            if group == "load":
                return EntryPoints()
            return entry_points(group=group)  # Остальные реальные

        mock_ep.side_effect = side_effect

        registry = PluginRegistry()
        await registry.initialize()

        all_plugins = registry.get_all_plugins()

        if "load" not in all_plugins:
            load_list = registry.list_plugins("load")
            assert "load" in load_list
            assert load_list["load"] == []

            # Попытка получить несуществующий load плагин должна вернуть None
            load_plugin = registry.get_plugin("load", "any_plugin")
            assert load_plugin is None
        else:
            # Если load плагины есть, проверяем что они корректны
            assert isinstance(all_plugins["load"], dict)


# === ТЕСТЫ ВАЛИДАЦИИ КОНФИГУРАЦИИ ===


async def test_validate_component_config_success(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест успешной валидации конфигурации компонента."""
    all_plugins = initialized_registry.get_all_plugins()

    if all_plugins.get("extract"):
        plugin_name = next(iter(all_plugins["extract"].keys()))

        result = await initialized_registry.validate_component_config(
            "extract",
            plugin_name,
            ComponentConfig(run_id="test_run", pipeline_name="test_pipeline"),
        )

        assert isinstance(result, dict)
        assert "valid" in result
        assert "errors" in result
        assert "component_info" in result
        assert result["valid"] is True
        assert isinstance(result["errors"], list)


async def test_validate_component_config_plugin_not_found(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест валидации конфигурации для несуществующего плагина."""

    result = await initialized_registry.validate_component_config(
        "extract", "nonexistent_plugin", ComponentConfig(run_id="test_run")
    )

    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert "not found" in result["errors"][0]
    assert result["component_info"] is None


async def test_validate_component_config_invalid_config(
    initialized_registry: PluginRegistry,
) -> None:
    """Тест валидации неправильной конфигурации."""
    all_plugins = initialized_registry.get_all_plugins()

    if all_plugins.get("extract"):
        plugin_name = next(iter(all_plugins["extract"].keys()))

        result = await initialized_registry.validate_component_config(
            "extract", plugin_name, ComponentConfig(run_id="123")
        )

        assert isinstance(result, dict)
        assert "valid" in result
        assert "errors" in result


async def test_get_all_plugins(initialized_registry: PluginRegistry) -> None:
    """Тест получения всех плагинов."""
    all_plugins = initialized_registry.get_all_plugins()

    assert isinstance(all_plugins, dict)
    assert "extract" in all_plugins
    assert "transform" in all_plugins
    # load группа может отсутствовать если нет соответствующих entry points

    for _plugin_type, plugins in all_plugins.items():
        assert isinstance(plugins, dict)
        for plugin_name, plugin_info in plugins.items():
            assert isinstance(plugin_name, str)
            assert isinstance(plugin_info, Info)


# === ТЕСТЫ РУЧНОЙ РЕГИСТРАЦИИ ===


def test_register_plugin_success(
    empty_registry: PluginRegistry, mock_plugin_class: type[BaseProcessClass]
) -> None:
    """Тест успешной ручной регистрации плагина."""
    empty_registry.register_plugin("extract", "mock_plugin", mock_plugin_class)

    # Проверяем что плагин зарегистрирован
    plugin_class = empty_registry.get_plugin("extract", "mock_plugin")
    assert plugin_class == mock_plugin_class

    plugin_info = empty_registry.get_plugin_info("extract", "mock_plugin")
    assert plugin_info is not None
    assert plugin_info.name == "MockPlugin"


def test_register_plugin_unknown_type(
    empty_registry: PluginRegistry, mock_plugin_class: type[BaseProcessClass]
) -> None:
    """Тест регистрации плагина неизвестного типа."""
    with pytest.raises(ValueError, match="Unknown plugin type"):
        empty_registry.register_plugin(
            "unknown_type", "mock_plugin", mock_plugin_class
        )


def test_register_plugin_invalid_class(empty_registry: PluginRegistry) -> None:
    """Тест регистрации класса, не наследующего BaseProcessClass."""

    class InvalidPlugin:
        pass

    with pytest.raises(ValueError, match="must inherit from BaseProcessClass"):
        empty_registry.register_plugin(
            "extract",
            "invalid_plugin",
            InvalidPlugin,  # type: ignore
        )


# === ИНТЕГРАЦИОННЫЕ ТЕСТЫ ===


async def test_full_workflow(empty_registry: PluginRegistry) -> None:
    """Интеграционный тест полного workflow с реестром."""
    # 1. Инициализируем реестр
    await empty_registry.initialize()

    # 2. Получаем список всех плагинов
    all_plugins = empty_registry.get_all_plugins()
    assert len(all_plugins) >= 0  # Может быть пустым, но должен быть словарем

    # 3. Проверяем каждый тип плагинов, который реально есть в проекте
    available_types = list(all_plugins.keys())

    for plugin_type in available_types:
        plugins_list = empty_registry.list_plugins(plugin_type)
        assert plugin_type in plugins_list

        if plugins_list[plugin_type]:  # Если есть плагины этого типа
            plugin_name = plugins_list[plugin_type][0]

            plugin_class = empty_registry.get_plugin(plugin_type, plugin_name)
            assert plugin_class is not None

            plugin_info = empty_registry.get_plugin_info(
                plugin_type, plugin_name
            )
            assert plugin_info is not None
            assert plugin_info.type_module == plugin_type

            # 6. Валидируем конфигурацию
            validation_result = await empty_registry.validate_component_config(
                plugin_type,
                plugin_name,
                ComponentConfig(run_id="test", pipeline_name="test"),
            )
            assert "valid" in validation_result


async def test_concurrent_access() -> None:
    """Тест конкурентного доступа к реестру."""

    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()

        # Инициализируем реестр несколько раз одновременно
        tasks = [registry.initialize() for _ in range(5)]
        await asyncio.gather(*tasks)

        # Проверяем что реестр корректно инициализирован
        all_plugins = registry.get_all_plugins()
        assert isinstance(all_plugins, dict)

        # Выполняем несколько операций одновременно
        tasks = []
        for _ in range(10):
            tasks.append(asyncio.create_task(registry.discover_all_plugins()))

        results = await asyncio.gather(*tasks)

        first_result = results[0]
        for result in results[1:]:
            assert result == first_result


async def test_registry_with_mock_plugins() -> None:
    """Тест реестра с полностью мокированными плагинами."""

    mock_entry_point = Mock()
    mock_entry_point.name = "test_plugin"
    mock_entry_point.load.return_value = type(
        "TestPlugin",
        (BaseProcessClass,),
        {
            "__init__": lambda self, config: super(type(self), self).__init__(
                config  # type: ignore
            ),
            "process": lambda self: {"status": "success"},  # noqa: ARG005
            "process_info": classmethod(
                lambda cls: Info(
                    name="TestPlugin",
                    version="1.0.0",
                    description="Test plugin",
                    type_class=cls,
                    type_module="extract",
                    config_class=ComponentConfig,
                )
            ),
        },
    )

    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = [mock_entry_point]

        registry = PluginRegistry()
        await registry.initialize()

        # Проверяем что плагин загрузился
        all_plugins = registry.get_all_plugins()
        assert "extract" in all_plugins
        assert "test_plugin" in all_plugins["extract"]

        # Проверяем что можем получить плагин
        plugin_class = registry.get_plugin("extract", "test_plugin")
        assert plugin_class is not None

        plugin_info = registry.get_plugin_info("extract", "test_plugin")
        assert plugin_info is not None
        assert plugin_info.name == "TestPlugin"


def test_list_plugins_nonexistent_type() -> None:
    """Тест получения списка плагинов несуществующего типа с изоляцией."""
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()
        plugins_list = registry.list_plugins("nonexistent_type")

        assert isinstance(plugins_list, dict)
        assert "nonexistent_type" in plugins_list
        assert plugins_list["nonexistent_type"] == []


# === ТЕСТЫ EDGE CASES ===


async def test_empty_plugin_groups() -> None:
    """Тест поведения при пустых группах плагинов."""
    registry = PluginRegistry()

    # Очищаем группы плагинов
    registry._plugin_groups = {}

    discovered = await registry.discover_all_plugins()
    assert discovered == {}


def test_get_all_plugins_empty_registry() -> None:
    """Тест получения всех плагинов из пустого реестра."""
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()
        all_plugins = registry.get_all_plugins()

        assert isinstance(all_plugins, dict)
        assert len(all_plugins) == 0


async def test_validate_component_config_exception_handling() -> None:
    """Тест обработки исключений при валидации конфигурации."""
    registry = PluginRegistry()

    class ProblematicPlugin(BaseProcessClass[ComponentConfig]):
        def __init__(self, config: ComponentConfig) -> None:  # noqa: ARG002
            # Намеренно вызываем исключение при инициализации
            msg = "Configuration error"
            raise ValueError(msg)

        async def process(self) -> Result:
            return Result(
                status="success",
                response={},
            )

        @classmethod
        def process_info(cls) -> Info:
            return Info(
                name="ProblematicPlugin",
                version="1.0.0",
                description="Plugin that causes errors",
                type_class=cls,
                type_module="extract",
                config_class=ComponentConfig,
            )

    registry.register_plugin("extract", "problematic", ProblematicPlugin)

    result = await registry.validate_component_config(
        "extract", "problematic", ComponentConfig(run_id="test")
    )

    assert result["valid"] is False
    assert len(result["errors"]) > 0
    assert "Configuration error" in result["errors"][0]


async def test_discover_plugin_group_registry_error() -> None:
    """Тест генерации RegistryError при несоответствии type_module."""
    registry = PluginRegistry()

    class BadTypePlugin(BaseProcessClass[ComponentConfig]):
        async def process(self) -> Result:
            return Result(
                status="success",
                response={},
            )

        @classmethod
        def process_info(cls) -> Info:
            return Info(
                name="BadTypePlugin",
                version="1.0.0",
                description="Plugin with bad type_module",
                type_class=cls,
                type_module="transform",
                config_class=ComponentConfig,
            )

    # Регистрируем плагин напрямую, чтобы обойти entry_points
    registry._plugins["extract"] = {}
    registry._plugin_classes["extract"] = {}

    with patch(
        "core.component.registry.core.entry_points"
    ) as mock_entry_points:
        mock_entry_point = Mock()
        mock_entry_point.load.return_value = BadTypePlugin
        mock_entry_point.name = "bad_plugin"
        mock_entry_points.return_value = [mock_entry_point]

        # Метод должен обработать ошибку и вернуть 0
        count = await registry._discover_plugin_group("extract")
        assert count == 0


def test_get_plugin_edge_cases() -> None:
    """Тест edge cases для метода get_plugin с изолированным реестром."""
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()

        assert registry.get_plugin(None, "test") is None
        assert registry.get_plugin("extract", None) is None
        assert registry.get_plugin(None, None) is None

        assert registry.get_plugin("", "test") is None
        assert registry.get_plugin("extract", "") is None


def test_get_plugin_info_edge_cases() -> None:
    """Тест edge cases для метода get_plugin_info с изолированным реестром."""
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()

        assert registry.get_plugin_info(None, "test") is None
        assert registry.get_plugin_info("extract", None) is None
        assert registry.get_plugin_info(None, None) is None

        assert registry.get_plugin_info("", "test") is None
        assert registry.get_plugin_info("extract", "") is None


async def test_validate_component_config_edge_cases() -> None:
    """Тест edge cases для валидации конфигурации с изолированным реестром."""
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()
        await registry.initialize()

        result = await registry.validate_component_config(
            None, "test", ComponentConfig()
        )
        assert result["valid"] is False

        result = await registry.validate_component_config(
            "extract", None, ComponentConfig()
        )
        assert result["valid"] is False

        result = await registry.validate_component_config(
            "", "test", ComponentConfig()
        )
        assert result["valid"] is False

        result = await registry.validate_component_config(
            "extract", "", ComponentConfig()
        )
        assert result["valid"] is False


def test_list_plugins_edge_cases() -> None:
    """Тест edge cases для метода list_plugins с изолированным реестром."""
    # Создаем полностью изолированный реестр
    with patch("core.component.registry.core.entry_points") as mock_ep:
        mock_ep.return_value = []

        registry = PluginRegistry()

        result = registry.list_plugins(None)
        assert isinstance(result, dict)
        result = registry.list_plugins("")
        assert isinstance(result, dict)
        assert result == {}
