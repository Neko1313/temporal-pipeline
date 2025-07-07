#!/usr/bin/env python3
"""
Temporal Pipeline CLI - Современный интерфейс командной строки на Typer
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer
import yaml
from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.prompt import Confirm
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree

from core.component import PluginRegistry
from core.temporal.activities import (
    cleanup_pipeline_data_activity,
    execute_stage_activity,
    validate_pipeline_activity,
)
from core.temporal.interfaces import PipelineExecutionResult
from core.temporal.scheduled_workflow import ScheduledPipelineWorkflow
from core.temporal.workflow import DataPipelineWorkflow
from core.yaml_loader import YAMLConfigParser
from core.yaml_loader.interfaces import PipelineConfig

console = Console()
app = typer.Typer(
    name="temporal-pipeline",
    help="🚀 Мощный ETL фреймворк с Temporal.io и plugin архитектурой",
    add_completion=False,
    rich_markup_mode="rich",
)

# Подкоманды
plugin_app = typer.Typer(help="🔌 Управление плагинами")
pipeline_app = typer.Typer(help="⚙️ Управление пайплайнами")
worker_app = typer.Typer(help="👷 Управление Temporal Workers")

app.add_typer(plugin_app, name="plugins")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(worker_app, name="worker")


@app.command()
def version() -> None:
    """📋 Показать версию приложения"""
    rprint("[bold blue]Temporal Pipeline[/bold blue] [green]v1.0.0[/green]")
    rprint("🏗️  ETL фреймворк нового поколения")


@pipeline_app.command("run")
def run_pipeline(
    config_path: Annotated[
        Path, typer.Argument(help="Путь к YAML конфигурации", exists=True)
    ],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Только валидация без запуска")
    ] = False,
    env_file: Annotated[
        Path | None, typer.Option("--env-file", help="Путь к .env файлу")
    ] = None,
    temporal_host: Annotated[
        str, typer.Option("--host", help="Адрес Temporal сервера")
    ] = "localhost:7233",
    namespace: Annotated[
        str, typer.Option("--namespace", help="Temporal namespace")
    ] = "default",
    run_id: Annotated[
        str | None, typer.Option("--run-id", help="Кастомный run ID")
    ] = None,
    wait: Annotated[
        bool,
        typer.Option("--wait/--no-wait", help="Ждать завершения пайплайна"),
    ] = True,
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Подробный вывод")
    ] = False,
) -> None:
    """🎯 Запустить ETL пайплайн"""
    asyncio.run(
        _run_pipeline_async(
            config_path,
            dry_run,
            env_file,
            temporal_host,
            namespace,
            run_id,
            wait,
            verbose,
        )
    )


async def _run_pipeline_async(
    config_path: Path,
    dry_run: bool,
    env_file: Path | None,
    temporal_host: str,
    namespace: str,
    run_id: str | None,
    wait: bool,
    verbose: bool,
) -> None:
    """Асинхронное выполнение пайплайна"""
    from temporalio.client import Client
    from temporalio.contrib.pydantic import pydantic_data_converter

    # Загружаем переменные окружения
    if env_file:
        _load_env_file(env_file)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            # Парсинг конфигурации
            task1 = progress.add_task(
                "📋 Загрузка конфигурации...", total=None
            )
            parser = YAMLConfigParser()
            pipeline_config = parser.parse_file(config_path)
            progress.advance(task1)
            progress.update(task1, description="✅ Конфигурация загружена")

            # Валидация плагинов
            task2 = progress.add_task(
                "🔌 Проверка плагинов...", total=len(pipeline_config.stages)
            )
            registry = PluginRegistry()
            await registry.initialize()

            validation_errors = []
            for stage_name, stage_config in pipeline_config.stages.items():
                component_info = registry.get_plugin_info(
                    stage_config.stage, stage_config.component
                )
                if not component_info:
                    available = registry.list_plugins(stage_config.stage)
                    error_msg = (
                        f"Стадия '{stage_name}': "
                        f"компонент '{stage_config.component}' "
                        f"типа '{stage_config.stage}' не найден. "
                        f"Доступные: {available.get(stage_config.stage, [])}"
                    )
                    validation_errors.append(error_msg)
                progress.advance(task2)

            if validation_errors:
                progress.update(
                    task2, description="❌ Найдены ошибки валидации"
                )
                for error in validation_errors:
                    rprint(f"[red]❌ {error}[/red]")
                raise typer.Exit(1)

            progress.update(task2, description="✅ Все плагины найдены")

            _display_pipeline_info(pipeline_config)

            if dry_run:
                rprint(
                    "\n🔍 [bold green]Dry run завершен успешно![/bold green]"
                )
                _display_pipeline_stats(pipeline_config)
                return

            # Подключение к Temporal
            task3 = progress.add_task(
                "🔗 Подключение к Temporal...", total=None
            )
            client = None
            try:
                client = await Client.connect(
                    temporal_host,
                    namespace=namespace,
                    data_converter=pydantic_data_converter,
                )
                progress.update(
                    task3, description="✅ Подключение установлено"
                )
            except Exception as ex:
                progress.update(task3, description="❌ Ошибка подключения")
                rprint(
                    f"[red]❌ Не удалось подключиться к Temporal: {ex}[/red]"
                )
                rprint(
                    f"[yellow]💡 Убедитесь что "
                    f"Temporal Server запущен "
                    f"на {temporal_host}[/yellow]"
                )
                raise typer.Exit(1) from ex

            # Запуск пайплайна
            task4 = progress.add_task("🚀 Запуск пайплайна...", total=None)

            from core.temporal.workflow import (
                DataPipelineWorkflow,
            )

            actual_run_id = (
                run_id
                or f"cli_{datetime.now(tz=UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"  # noqa: E501
            )

            if verbose:
                rprint(f"[dim]Run ID: {actual_run_id}[/dim]")

            if wait:
                if client is None:
                    raise typer.Exit(1)

                result = await client.execute_workflow(
                    DataPipelineWorkflow.run,
                    args=[pipeline_config, actual_run_id],
                    id=f"pipeline-{actual_run_id}",
                    task_queue="pipeline-tasks",
                )

                progress.update(task4, description="✅ Пайплайн завершен")
                _display_execution_results(result)

            else:
                if client is None:
                    raise typer.Exit(1)

                handle = await client.start_workflow(
                    DataPipelineWorkflow.run,
                    args=[pipeline_config, actual_run_id],
                    id=f"pipeline-{actual_run_id}",
                    task_queue="pipeline-tasks",
                )

                progress.update(task4, description="✅ Пайплайн запущен")
                rprint("[green]✅ Пайплайн запущен асинхронно[/green]")
                rprint(f"[blue]🆔 Workflow ID: {handle.id}[/blue]")
                rprint(f"[blue]🏃 Run ID: {actual_run_id}[/blue]")

    except Exception as ex:
        rprint(f"[red]❌ Ошибка: {ex}[/red]")
        if verbose:
            import traceback

            rprint(f"[dim]{traceback.format_exc()}[/dim]")
        raise typer.Exit(1) from ex


@pipeline_app.command("validate")
def validate_pipeline(
    config_path: Annotated[
        Path, typer.Argument(help="Путь к YAML конфигурации", exists=True)
    ],
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Подробный вывод")
    ] = False,
    output_format: Annotated[
        str, typer.Option("--format", help="Формат вывода")
    ] = "table",
) -> None:
    """✅ Валидировать конфигурацию пайплайна"""
    asyncio.run(_validate_config_async(config_path, verbose, output_format))


async def _validate_config_async(
    config_path: Path, verbose: bool, output_format: str
) -> None:
    """Валидация конфигурации"""
    try:
        parser = YAMLConfigParser()
        pipeline_config = parser.parse_file(config_path)

        registry = PluginRegistry()
        await registry.initialize()

        rprint("🔍 [bold blue]Результаты валидации:[/bold blue]")

        # Базовая информация
        if output_format == "table":
            table = Table(title="Информация о пайплайне", show_header=True)
            table.add_column("Параметр", style="cyan", width=20)
            table.add_column("Значение", style="magenta")

            table.add_row("Название", pipeline_config.name)
            table.add_row("Версия", pipeline_config.version)
            table.add_row(
                "Описание", pipeline_config.description or "Не указано"
            )
            table.add_row(
                "Количество стадий", str(len(pipeline_config.stages))
            )
            table.add_row(
                "Макс. параллельность",
                str(pipeline_config.max_parallel_stages),
            )
            table.add_row(
                "Таймаут по умолчанию", f"{pipeline_config.default_timeout}с"
            )

            if pipeline_config.schedule.enabled:
                schedule_info = (
                    pipeline_config.schedule.cron
                    if pipeline_config.schedule.cron
                    else pipeline_config.schedule.interval
                )
                table.add_row("Расписание", f"✅ {schedule_info}")
            else:
                table.add_row("Расписание", "❌ Отключено")

            console.print(table)

        # Проверка стадий
        stages_table = Table(title="Стадии пайплайна", show_header=True)
        stages_table.add_column("Стадия", style="cyan", width=15)
        stages_table.add_column("Тип", style="yellow", width=10)
        stages_table.add_column("Компонент", style="green", width=15)
        stages_table.add_column("Зависимости", style="blue", width=15)
        stages_table.add_column("Статус", style="red", width=12)

        if verbose:
            stages_table.add_column("Версия", style="dim", width=8)
            stages_table.add_column("Описание", style="dim")

        all_valid = True
        for stage_name, stage_config in pipeline_config.stages.items():
            component_info = registry.get_plugin_info(
                stage_config.stage, stage_config.component
            )
            status = "✅ OK" if component_info else "❌ НЕ НАЙДЕН"
            if not component_info:
                all_valid = False

            dependencies = (
                ", ".join(stage_config.depends_on)
                if stage_config.depends_on
                else "Нет"
            )

            row = [
                stage_name,
                stage_config.stage,
                stage_config.component,
                dependencies,
                status,
            ]

            if verbose and component_info:
                row.extend(
                    [
                        component_info.version or "Н/Д",
                        component_info.description or "Нет описания",
                    ]
                )
            elif verbose:
                row.extend(["Н/Д", "Компонент не найден"])

            stages_table.add_row(*row)

        console.print(stages_table)

        # Анализ зависимостей
        if verbose:
            _display_dependency_analysis(pipeline_config)

        # Результат
        if all_valid:
            rprint("\n✅ [bold green]Конфигурация валидна![/bold green]")
        else:
            rprint("\n❌ [bold red]Найдены ошибки в конфигурации![/bold red]")
            raise typer.Exit(1)

    except Exception as ex:
        rprint(f"[red]❌ Ошибка валидации: {ex}[/red]")
        raise typer.Exit(1) from ex


@plugin_app.command("list")
def list_plugins(
    plugin_type: Annotated[
        str | None, typer.Option("--type", help="Тип плагинов")
    ] = None,
    output_format: Annotated[
        str, typer.Option("--format", help="Формат вывода")
    ] = "table",
    detailed: Annotated[
        bool, typer.Option("--detailed", "-d", help="Подробная информация")
    ] = False,
) -> None:
    """🔌 Показать доступные плагины"""
    asyncio.run(_list_plugins_async(plugin_type, output_format, detailed))


async def _list_plugins_async(
    plugin_type: str | None, output_format: str, detailed: bool
) -> None:
    """Отображение списка плагинов"""
    registry = PluginRegistry()
    await registry.initialize()

    all_plugins = registry.get_all_plugins()

    if plugin_type:
        if plugin_type not in all_plugins:
            rprint(f"[red]❌ Неизвестный тип плагина: {plugin_type}[/red]")
            rprint(
                f"[yellow]Доступные типы: "
                f"{', '.join(all_plugins.keys())}[/yellow]"
            )
            raise typer.Exit(1)
        all_plugins = {plugin_type: all_plugins[plugin_type]}

    if output_format == "json":
        json_output = {}
        for ptype, plugins in all_plugins.items():
            json_output[ptype] = {
                name: {
                    "name": info.name,
                    "version": info.version,
                    "description": info.description,
                    "type_module": info.type_module,
                }
                for name, info in plugins.items()
            }
        rprint(json.dumps(json_output, indent=2, ensure_ascii=False))

    elif output_format == "tree":
        tree = Tree("🔌 [bold blue]Доступные плагины[/bold blue]")
        for ptype, plugins in all_plugins.items():
            type_branch = tree.add(
                f"📂 [yellow]{ptype.upper()}[/yellow] "
                f"([dim]{len(plugins)} плагинов[/dim])"
            )
            for name, info in plugins.items():
                plugin_info = f"[green]{name}[/green]"
                if info.version:
                    plugin_info += f" [dim]v{info.version}[/dim]"
                if info.description and detailed:
                    plugin_info += f"\n   [italic]{info.description}[/italic]"
                type_branch.add(plugin_info)
        console.print(tree)

    else:
        # Table формат
        total_plugins = sum(len(plugins) for plugins in all_plugins.values())
        rprint(f"[blue]📊 Всего плагинов: {total_plugins}[/blue]\n")

        for ptype, plugins in all_plugins.items():
            if plugins:
                table = Table(title=f"🔌 Плагины типа: {ptype.upper()}")
                table.add_column("Название", style="cyan", width=20)
                table.add_column("Версия", style="yellow", width=10)

                if detailed:
                    table.add_column("Описание", style="green")
                    table.add_column("Модуль", style="blue", width=12)
                else:
                    table.add_column("Описание", style="green", max_width=50)

                for name, info in plugins.items():
                    row = [
                        name,
                        info.version or "Н/Д",
                        info.description or "Описание отсутствует",
                    ]

                    if detailed:
                        row.append(info.type_module)

                    table.add_row(*row)

                console.print(table)
                console.print()


@pipeline_app.command("init")
def init_pipeline(
    name: Annotated[str, typer.Argument(help="Название пайплайна")],
    template: Annotated[
        str, typer.Option("--template", "-t", help="Тип шаблона")
    ] = "simple",
    output: Annotated[
        Path | None,
        typer.Option("--output", "-o", help="Путь для сохранения"),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", "-f", help="Перезаписать существующий файл"),
    ] = False,
    edit: Annotated[
        bool, typer.Option("--edit", help="Открыть в редакторе после создания")
    ] = False,
) -> None:
    """📝 Создать новый пайплайн из шаблона"""
    _create_pipeline_template(name, template, output, force, edit)


@worker_app.command("start")
def start_worker(
    host: Annotated[
        str, typer.Option("--host", help="Temporal server address")
    ] = "localhost:7233",
    namespace: Annotated[
        str, typer.Option("--namespace", help="Temporal namespace")
    ] = "default",
    task_queue: Annotated[
        str, typer.Option("--task-queue", help="Task queue name")
    ] = "pipeline-tasks",
    max_concurrent_activities: Annotated[
        int, typer.Option("--max-activities", help="Макс. активностей")
    ] = 10,
    max_concurrent_workflows: Annotated[
        int, typer.Option("--max-workflows", help="Макс. воркфлоу")
    ] = 5,
) -> None:
    """⚙️ Запустить Temporal Worker"""
    asyncio.run(
        _start_worker_async(
            host,
            namespace,
            task_queue,
            max_concurrent_activities,
            max_concurrent_workflows,
        )
    )


# Вспомогательные функции


def _load_env_file(env_file: Path) -> None:
    """Загрузка переменных окружения из файла"""
    import os

    try:
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith("#") and "=" in line:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value.strip("\"'")
        rprint(f"[green]✅ Загружены переменные из {env_file}[/green]")
    except Exception as e:
        rprint(f"[red]❌ Ошибка загрузки env файла: {e}[/red]")


def _display_pipeline_info(pipeline_config: PipelineConfig) -> None:
    """Отображение информации о пайплайне"""
    panel_content = """
[bold blue]Название:[/bold blue] {}
[bold blue]Версия:[/bold blue] {}
[bold blue]Описание:[/bold blue] {}
[bold blue]Стадий:[/bold blue] {}
[bold blue]Макс. параллельность:[/bold blue] {}
[bold blue]Таймаут:[/bold blue] {}с
""".format(
        pipeline_config.name,
        pipeline_config.version,
        pipeline_config.description or "Не указано",
        len(pipeline_config.stages),
        pipeline_config.max_parallel_stages,
        pipeline_config.default_timeout,
    )

    if pipeline_config.schedule.enabled:
        schedule_info = (
            pipeline_config.schedule.cron
            if pipeline_config.schedule.cron
            else pipeline_config.schedule.interval
        )
        panel_content += (
            f"[bold blue]Расписание:[/bold blue] {schedule_info}\n"
        )

    console.print(
        Panel(
            panel_content,
            title="🚀 Информация о пайплайне",
            border_style="green",
        )
    )


def _display_pipeline_stats(pipeline_config: PipelineConfig) -> None:
    """Статистика пайплайна"""
    from collections import Counter

    stage_types = Counter(
        stage.stage for stage in pipeline_config.stages.values()
    )

    stats_table = Table(title="📊 Статистика пайплайна")
    stats_table.add_column("Метрика", style="cyan")
    stats_table.add_column("Значение", style="magenta")

    for stage_type, count in stage_types.items():
        stats_table.add_row(f"Стадий типа {stage_type}", str(count))

    deps_count = sum(
        len(stage.depends_on) for stage in pipeline_config.stages.values()
    )
    stats_table.add_row("Всего зависимостей", str(deps_count))
    stats_table.add_row(
        "Переменных окружения", str(len(pipeline_config.required_env_vars))
    )

    console.print(stats_table)


def _display_dependency_analysis(pipeline_config: PipelineConfig) -> None:
    """Анализ зависимостей"""
    rprint("\n[bold blue]📊 Анализ зависимостей:[/bold blue]")

    # Строим граф зависимостей
    from core.temporal.utils.transform import (
        build_execution_order,
    )

    try:
        execution_order = build_execution_order(pipeline_config.stages)

        dep_table = Table(title="Порядок выполнения", show_header=True)
        dep_table.add_column("Батч", style="cyan", width=8)
        dep_table.add_column("Стадии", style="green")
        dep_table.add_column("Параллельность", style="yellow", width=15)

        for i, batch in enumerate(execution_order, 1):
            dep_table.add_row(str(i), ", ".join(batch), f"{len(batch)} стадий")

        console.print(dep_table)

    except ValueError as e:
        rprint(f"[red]❌ Ошибка в зависимостях: {e}[/red]")


def _display_execution_results(result: PipelineExecutionResult) -> None:
    """Отображение результатов выполнения"""

    if result.status == "success":
        rprint("\n🎉 [bold green]Пайплайн выполнен успешно![/bold green]")

        results_table = Table(title="📊 Результаты выполнения")
        results_table.add_column("Метрика", style="cyan")
        results_table.add_column("Значение", style="magenta")

        results_table.add_row(
            "Обработано записей", str(result.total_records_processed)
        )
        results_table.add_row(
            "Время выполнения", f"{result.total_execution_time:.2f}с"
        )
        if result.stage_results:
            results_table.add_row(
                "Успешных стадий",
                str(
                    len(
                        [
                            s
                            for s in result.stage_results
                            if s.status == "success"
                        ]
                    )
                ),
            )
        results_table.add_row("Run ID", result.run_id)

        console.print(results_table)

        # Детали по стадиям
        if result.stage_results:
            stages_table = Table(title="Детали по стадиям")
            stages_table.add_column("Стадия", style="cyan")
            stages_table.add_column("Статус", style="green")
            stages_table.add_column("Записей", style="yellow")
            stages_table.add_column("Время", style="blue")

            for stage_result in result.stage_results:
                status_icon = (
                    "✅" if stage_result.status == "success" else "❌"
                )
                stages_table.add_row(
                    stage_result.stage_name,
                    f"{status_icon} {stage_result.status}",
                    str(stage_result.records_processed),
                    f"{stage_result.execution_time:.2f}с",
                )

            console.print(stages_table)
    else:
        rprint("\n❌ [bold red]Пайплайн завершился с ошибкой![/bold red]")
        if result.error_message:
            rprint(f"💥 [red]Ошибка: {result.error_message}[/red]")


def _create_pipeline_template(
    name: str, template: str, output: Path | None, force: bool, edit: bool
) -> None:
    """Создание шаблона пайплайна"""
    templates = {
        "simple": {
            "name": name,
            "description": f"Простой ETL пайплайн: {name}",
            "version": "1.0.0",
            "author": "Generated by temporal-pipeline CLI",
            "tags": ["etl", "simple"],
            "schedule": {"enabled": False},
            "default_resilience": {
                "max_attempts": 3,
                "initial_delay": 1.0,
                "backoff_multiplier": 2.0,
                "retry_policy": "exponential_backoff",
            },
            "stages": {
                "extract_data": {
                    "stage": "extract",
                    "component": "csv_extract",
                    "component_config": {
                        "source_config": {
                            "path": "${SOURCE_CSV_PATH}",
                            "encoding": "utf-8",
                        },
                        "delimiter": ",",
                        "has_header": True,
                    },
                },
                "load_data": {
                    "stage": "load",
                    "component": "postgres_load",
                    "depends_on": ["extract_data"],
                    "component_config": {
                        "connection_config": {"uri": "${TARGET_DATABASE_URL}"},
                        "target_table": "target_table",
                        "target_schema": "public",
                        "if_exists": "replace",
                    },
                },
            },
            "required_env_vars": ["SOURCE_CSV_PATH", "TARGET_DATABASE_URL"],
            "max_parallel_stages": 2,
        },
        "api_to_db": {
            "name": name,
            "description": f"ETL пайплайн API -> "
            f"Transform -> "
            f"Database: {name}",
            "version": "1.0.0",
            "schedule": {
                "enabled": True,
                "cron": "0 */6 * * *",  # Каждые 6 часов
                "timezone": "UTC",
            },
            "stages": {
                "extract_api_data": {
                    "stage": "extract",
                    "component": "http_extract",
                    "component_config": {
                        "url": "${API_ENDPOINT}",
                        "method": "GET",
                        "auth_config": {
                            "auth_type": "bearer",
                            "bearer_token": "${API_TOKEN}",
                        },
                        "response_format": "json",
                        "json_data_path": "data",
                    },
                },
                "transform_data": {
                    "stage": "transform",
                    "component": "json_transform",
                    "depends_on": ["extract_api_data"],
                    "component_config": {
                        "filter_conditions": ['pl.col("status") == "active"'],
                        "add_metadata": True,
                    },
                },
                "load_to_database": {
                    "stage": "load",
                    "component": "postgres_load",
                    "depends_on": ["transform_data"],
                    "component_config": {
                        "connection_config": {"uri": "${DATABASE_URL}"},
                        "target_table": f"{name}_data",
                        "target_schema": "public",
                        "if_exists": "upsert",
                        "upsert_config": {"conflict_columns": ["id"]},
                    },
                },
            },
            "required_env_vars": ["API_ENDPOINT", "API_TOKEN", "DATABASE_URL"],
        },
        "complex": {
            "name": name,
            "description": f"Комплексный multi-source ETL пайплайн: {name}",
            "version": "1.0.0",
            "schedule": {
                "enabled": True,
                "cron": "0 2 * * *",
                "timezone": "UTC",
            },
            "default_resilience": {
                "max_attempts": 5,
                "initial_delay": 2.0,
                "backoff_multiplier": 2.0,
                "retry_policy": "exponential_backoff",
                "circuit_breaker_enabled": True,
            },
            "stages": {
                "extract_users": {
                    "stage": "extract",
                    "component": "http_extract",
                    "component_config": {
                        "url": "${USERS_API_URL}",
                        "auth_config": {
                            "auth_type": "bearer",
                            "bearer_token": "${API_TOKEN}",
                        },
                        "pagination_config": {
                            "pagination_type": "page",
                            "max_pages": 100,
                        },
                    },
                },
                "extract_orders": {
                    "stage": "extract",
                    "component": "sql_extract",
                    "component_config": {
                        "query": "SELECT * FROM orders "
                        "WHERE "
                        "created_at > ("
                        'CURRENT_DATE - INTERVAL "7 days"'
                        ")",
                        "source_config": {"uri": "${ORDERS_DB_URL}"},
                    },
                },
                "extract_products": {
                    "stage": "extract",
                    "component": "csv_extract",
                    "component_config": {
                        "source_config": {"path": "${PRODUCTS_CSV_PATH}"}
                    },
                },
                "transform_join_data": {
                    "stage": "transform",
                    "component": "json_transform",
                    "depends_on": ["extract_users", "extract_orders"],
                    "component_config": {
                        "join_config": {
                            "join_type": "inner",
                            "left_on": "user_id",
                            "right_on": "user_id",
                        },
                        "aggregation": {
                            "group_by": ["user_id"],
                            "aggregations": {
                                "order_value": "sum",
                                "order_count": "count",
                            },
                        },
                    },
                },
                "load_analytics": {
                    "stage": "load",
                    "component": "postgres_load",
                    "depends_on": ["transform_join_data"],
                    "component_config": {
                        "connection_config": {"uri": "${DWH_DATABASE_URL}"},
                        "target_table": "user_analytics",
                        "target_schema": "analytics",
                        "if_exists": "append",
                    },
                },
            },
            "required_env_vars": [
                "USERS_API_URL",
                "API_TOKEN",
                "ORDERS_DB_URL",
                "PRODUCTS_CSV_PATH",
                "DWH_DATABASE_URL",
            ],
            "max_parallel_stages": 3,
        },
    }

    template_config = templates.get(template, templates["simple"])
    output_path = output or Path(f"{name}.yml")

    # Проверяем существование файла
    if (
        output_path.exists()
        and not force
        and not Confirm.ask(
            f"Файл {output_path} уже существует. Перезаписать?"
        )
    ):
        rprint("[yellow]Создание отменено[/yellow]")
        return

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                template_config,
                f,
                default_flow_style=False,
                allow_unicode=True,
                indent=2,
            )

        rprint(
            f"✅ Создан шаблон пайплайна:"
            f"[bold green]{output_path}[/bold green]"
        )
        rprint(f"📝 Тип: [yellow]{template}[/yellow]")

        # Показываем содержимое
        with open(output_path, encoding="utf-8") as f:
            content = f.read()

        syntax = Syntax(content, "yaml", theme="monokai", line_numbers=True)
        console.print(
            Panel(
                syntax, title=f"Содержимое {output_path}", border_style="blue"
            )
        )

        # Показываем следующие шаги
        settings = chr(10).join(
            f'   export {var}="your_value"'
            for var in template_config.get("required_env_vars", [])
        )
        next_steps = f"""
[bold blue]Следующие шаги:[/bold blue]

1. Настройте переменные окружения:
   {settings}

2. Валидируйте конфигурацию:
   [cyan]temporal-pipeline pipeline validate {output_path}[/cyan]

3. Запустите пайплайн:
   [cyan]temporal-pipeline pipeline run {output_path}[/cyan]

4. Или запустите в dry-run режиме:
   [cyan]temporal-pipeline pipeline run {output_path} --dry-run[/cyan]
"""

        console.print(
            Panel(next_steps, title="🚀 Начало работы", border_style="green")
        )

        # Открываем в редакторе если нужно
        if edit:
            import os
            import shutil
            import subprocess

            editor = os.environ.get("EDITOR", "nano")
            try:
                if shutil.which(editor):
                    subprocess.run([editor, str(output_path)], check=True)
            except Exception as ex:
                rprint(f"[yellow]Не удалось открыть редактор: {ex}[/yellow]")

    except Exception as ex:
        rprint(f"[red]❌ Ошибка создания шаблона: {ex}[/red]")
        raise typer.Exit(1) from ex


async def _start_worker_async(
    host: str,
    namespace: str,
    task_queue: str,
    max_activities: int,
    max_workflows: int,
) -> None:
    """Запуск Temporal Worker"""
    try:
        rprint(f"🔗 Подключение к Temporal: [bold blue]{host}[/bold blue]")
        rprint(f"📍 Namespace: [bold yellow]{namespace}[/bold yellow]")
        rprint(f"📋 Task Queue: [bold green]{task_queue}[/bold green]")

        from temporalio.client import Client
        from temporalio.contrib.pydantic import pydantic_data_converter

        client = await Client.connect(
            host, namespace=namespace, data_converter=pydantic_data_converter
        )

        rprint("✅ [bold green]Подключение к Temporal успешно![/bold green]")

        from core.component import PluginRegistry

        registry = PluginRegistry()
        await registry.initialize()

        # Импортируем все необходимые компоненты
        rprint("📦 Импорт workflow и activities...")

        try:
            rprint("✅ Все компоненты импортированы")
        except ImportError as ex:
            rprint(f"[red]❌ Ошибка импорта: {ex}[/red]")
            raise typer.Exit(1) from ex

        # Создаем и настраиваем Worker
        from temporalio.worker import Worker

        rprint("⚙️ Создание Worker...")
        rprint(f"   • Макс. activities: {max_activities}")
        rprint(f"   • Макс. workflows: {max_workflows}")

        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[DataPipelineWorkflow, ScheduledPipelineWorkflow],
            activities=[
                execute_stage_activity,
                validate_pipeline_activity,
                cleanup_pipeline_data_activity,
            ],
            max_concurrent_activities=max_activities,
            max_concurrent_workflow_tasks=max_workflows,
        )

        rprint("\n🎉 [bold green]Worker готов к работе![/bold green]")

        worker_info = f"""
[bold blue]Информация о Worker:[/bold blue]

🖥️  Host: {host}
📍 Namespace: {namespace}
📋 Task Queue: {task_queue}
⚡ Макс. activities: {max_activities}
🔄 Макс. workflows: {max_workflows}

[bold green]Worker ожидает задач...[/bold green]
[dim]Нажмите Ctrl+C для остановки[/dim]
"""

        console.print(
            Panel(worker_info, title="⚙️ Temporal Worker", border_style="blue")
        )

        # Запускаем Worker
        await worker.run()

    except KeyboardInterrupt:
        rprint("\n🛑 [yellow]Worker остановлен пользователем[/yellow]")
    except Exception as ex:
        rprint(f"\n❌ [bold red]Ошибка Worker:[/bold red] {ex}")
        import traceback

        rprint(f"[dim]{traceback.format_exc()}[/dim]")
        raise typer.Exit(1) from ex


if __name__ == "__main__":
    app()
