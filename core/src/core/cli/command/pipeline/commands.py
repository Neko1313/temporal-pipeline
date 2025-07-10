import traceback
from asyncio import run as asyncio_run
from pathlib import Path
from typing import Annotated

from rich import print as rprint
from typer import Argument, Exit, Option, Typer

from core.cli.command.pipeline.service import (
    run_pipeline_async,
    validate_config_async,
)

pipeline_app = Typer(help="⚙️ Pipeline management")


@pipeline_app.command("run")
def run_pipeline(  # noqa: PLR0913
    config_path: Annotated[
        Path, Argument(help="Path to YAML configuration", exists=True)
    ],
    env_file: Annotated[
        Path | None, Option("--env-file", help="Path to .env file")
    ] = None,
    temporal_host: Annotated[
        str, Option("--host", help="Temporal server address")
    ] = "localhost:7233",
    namespace: Annotated[
        str, Option("--namespace", help="Temporal namespace")
    ] = "default",
    run_id: Annotated[
        str | None, Option("--run-id", help="Custom run ID")
    ] = None,
    verbose: Annotated[
        bool, Option("--verbose", "-v", help="Detailed conclusion")
    ] = False,
) -> None:
    """Starting Pipeline."""
    try:
        asyncio_run(
            run_pipeline_async(
                config_path,
                env_file,
                temporal_host,
                namespace,
                run_id,
            )
        )

    except Exception as ex:
        rprint(f"[red]❌ Error: {ex}[/red]")
        if verbose:
            rprint(f"[dim]{traceback.format_exc()}[/dim]")
        raise Exit(1) from ex


@pipeline_app.command("validate")
def validate_pipeline(
    config_path: Annotated[
        Path, Argument(help="Path to YAML configuration", exists=True)
    ],
    env_file: Annotated[
        Path | None, Option("--env-file", help="Path to .env file")
    ] = None,
) -> None:
    """Starting Pipeline validation."""
    asyncio_run(validate_config_async(config_path, env_file))
