"""Temporal Pipeline CLI."""

import typer
from rich import print as rprint
from rich.console import Console

from cli.command import pipeline_app, plugin_app, worker_app

console = Console()
app = typer.Typer(
    name="temporal-pipeline",
    help="🚀 Powerful ETL framework with Temporal.io and plugin architecture",
    add_completion=False,
)

app.add_typer(plugin_app, name="plugins")
app.add_typer(pipeline_app, name="pipeline")
app.add_typer(worker_app, name="worker")


@app.command()
def version() -> None:
    """📋 Show the application version."""
    rprint("[bold blue]Temporal Pipeline[/bold blue] [green]v1.0.0[/green]")
    rprint("🏗️  Next generation ETL framework")


if __name__ == "__main__":
    app()
