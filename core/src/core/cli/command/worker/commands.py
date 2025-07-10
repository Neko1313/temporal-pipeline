import traceback
from asyncio import run as asyncio_run
from typing import Annotated

from rich import print as rprint
from typer import Exit, Option, Typer

from core.cli.command.worker.service import start_worker_async

worker_app = Typer(help="👷 Managing Temporal Workers")


@worker_app.command("start")
def start_worker(
    host: Annotated[
        str, Option("--host", help="Temporal server address")
    ] = "localhost:7233",
    namespace: Annotated[
        str, Option("--namespace", help="Temporal namespace")
    ] = "default",
    task_queue: Annotated[
        str, Option("--task-queue", help="Task queue name")
    ] = "pipeline-tasks",
) -> None:
    """⚙️ Start Temporal Worker."""
    try:
        asyncio_run(
            start_worker_async(
                host,
                namespace,
                task_queue,
            )
        )
    except KeyboardInterrupt:
        rprint("\n🛑 [yellow]Worker stopped by user[/yellow]")
    except Exception as ex:
        rprint(f"\n❌ [bold red]Error Worker:[/bold red] {ex}")
        rprint(f"[dim]{traceback.format_exc()}[/dim]")
        raise Exit(1) from ex
