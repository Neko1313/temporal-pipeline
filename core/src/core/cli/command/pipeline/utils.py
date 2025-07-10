import os
from pathlib import Path

from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from typer import Exit

from core.cli.command.pipeline.const import MESSAGE_INFO_PIPELINE
from core.component import PluginRegistry
from core.yaml_loader.interfaces import PipelineConfig

console = Console()


def load_env_file(env_file: Path) -> None:
    try:
        with open(env_file) as f:
            for line in f:
                if line.strip() and not line.startswith("#") and "=" in line:
                    key, value = line.strip().split("=", 1)
                    os.environ[key] = value.strip("\"'")
        rprint(f"[green]✅ Loaded variables from {env_file}[/green]")
    except Exception as e:
        rprint(f"[red]❌ Error loading env file: {e}[/red]")
        raise Exit(1) from e


def display_pipeline_info(pipeline_config: PipelineConfig) -> None:
    panel_content = MESSAGE_INFO_PIPELINE.format(
        pipeline_config.name,
        pipeline_config.version,
        pipeline_config.description or "Not specified",
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
        panel_content += f"[bold blue]Schedule:[/bold blue] {schedule_info}\n"

    console.print(
        Panel(
            panel_content,
            title="🚀 Pipeline information",
            border_style="green",
        )
    )


async def get_temporal_client(
    temporal_host: str,
    namespace: str,
) -> Client:
    try:
        return await Client.connect(
            temporal_host,
            namespace=namespace,
            data_converter=pydantic_data_converter,
        )
    except Exception as ex:
        rprint(f"[red]❌ Failed to connect to Temporal: {ex}[/red]")
        rprint(
            f"[yellow]💡 Make sure"
            f"Temporal Server is up and running "
            f"on {temporal_host}[/yellow]"
        )
        raise Exit(1) from ex


def plugin_validation(
    pipeline_config: PipelineConfig,
    registry: PluginRegistry,
) -> None:
    validation_errors = []
    for _stage_name, stage_config in pipeline_config.stages.items():
        component_info = registry.get_plugin_info(
            stage_config.stage, stage_config.component
        )
        if component_info:
            continue

        error_not_found = f"Not found {stage_config.component}"
        validation_errors.append(error_not_found)

    if validation_errors:
        for error in validation_errors:
            rprint(f"[red]❌ {error}[/red]")

        raise Exit(1)
