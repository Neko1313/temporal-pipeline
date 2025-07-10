from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from rich import print as rprint
from rich.console import Console

from cli.command.pipeline.utils import (
    display_pipeline_info,
    get_temporal_client,
    load_env_file,
    plugin_validation,
)
from core.component import PluginRegistry
from core.temporal.workflow import (
    DataPipelineWorkflow,
)
from core.yaml_loader import YAMLConfigParser

console = Console()


async def run_pipeline_async(
    config_path: Path,
    env_file: Path | None,
    temporal_host: str,
    namespace: str,
    run_id: str | None,
) -> None:
    if env_file:
        load_env_file(env_file)

    pipeline_config = YAMLConfigParser.parse_file(config_path)
    registry = PluginRegistry()

    await registry.initialize()

    plugin_validation(pipeline_config, registry)

    display_pipeline_info(pipeline_config)

    client = await get_temporal_client(temporal_host, namespace)

    actual_run_id = (
        run_id
        or f"cli_{datetime.now(tz=UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"  # noqa: E501
    )

    handle = await client.start_workflow(
        DataPipelineWorkflow.run,
        args=[pipeline_config, actual_run_id],
        id=f"pipeline-{actual_run_id}",
        task_queue="pipeline-tasks",
    )

    rprint("[green]✅ Pipeline is running[/green]")
    rprint(f"[blue]🆔 Workflow ID: {handle.id}[/blue]")
    rprint(f"[blue]🏃 Run ID: {actual_run_id}[/blue]")


async def validate_config_async(
    config_path: Path,
    env_file: Path | None,
) -> None:
    if env_file:
        load_env_file(env_file)

    pipeline_config = YAMLConfigParser.parse_file(config_path)
    registry = PluginRegistry()

    await registry.initialize()

    plugin_validation(pipeline_config, registry)
    rprint("✅ Configuration is valid!")
