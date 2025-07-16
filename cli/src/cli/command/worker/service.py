from rich import print as rprint
from rich.console import Console
from rich.panel import Panel
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from cli.command.worker.const import WORKER_INFO
from core.component import PluginRegistry
from core.temporal.activities import (
    activity_stage,
    cleanup_pipeline_data_activity,
    validate_pipeline_activity,
)
from core.temporal.scheduled_workflow import ScheduledPipelineWorkflow
from core.temporal.workflow import DataPipelineWorkflow

console = Console()


async def start_worker_async(
    host: str,
    namespace: str,
    task_queue: str,
) -> None:
    rprint(f"🔗 Connection to Temporal: [bold blue]{host}[/bold blue]")
    rprint(f"📍 Namespace: [bold yellow]{namespace}[/bold yellow]")
    rprint(f"📋 Task Queue: [bold green]{task_queue}[/bold green]")

    client = await Client.connect(
        host, namespace=namespace, data_converter=pydantic_data_converter
    )

    rprint(
        "✅ [bold green]The connection to Temporal is successful![/bold green]"
    )

    registry = PluginRegistry()
    await registry.initialize()

    rprint("📦 Import workflow и activities...")

    rprint("⚙️ Creating Worker...")

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[DataPipelineWorkflow, ScheduledPipelineWorkflow],
        activities=[
            activity_stage,
            validate_pipeline_activity,
            cleanup_pipeline_data_activity,
        ],
    )

    rprint("\n🎉 [bold green]The Worker is ready to go![/bold green]")

    worker_info = WORKER_INFO.format(
        host=host, namespace=namespace, task_queue=task_queue
    )

    console.print(
        Panel(worker_info, title="⚙️ Temporal Worker", border_style="blue")
    )

    await worker.run()
