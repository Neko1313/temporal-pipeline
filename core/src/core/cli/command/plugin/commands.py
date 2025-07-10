from asyncio import run as asyncio_run
from typing import Annotated

from typer import Option, Typer

from core.cli.command.plugin.service import list_plugins_async
from core.cli.command.plugin.type import OutputType

plugin_app = Typer(help="🔌 Plugin management")


@plugin_app.command("list")
def list_plugins(
    plugin_type: Annotated[
        str | None, Option("--type", help="Plug-in type")
    ] = None,
    output_format: Annotated[
        OutputType, Option("--format", help="Output format")
    ] = OutputType.TABLE,
    detailed: Annotated[
        bool, Option("--detailed", "-d", help="Detailed information")
    ] = False,
) -> None:
    """🔌 Show available plugins."""
    asyncio_run(list_plugins_async(plugin_type, output_format, detailed))
