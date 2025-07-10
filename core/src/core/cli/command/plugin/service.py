from json import dumps

from rich import print as rprint
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from typer import Exit

from core.cli.command.plugin.type import OutputType
from core.component import Info, PluginRegistry

console = Console()


async def list_plugins_async(
    plugin_type: str | None, output_format: str, detailed: bool
) -> None:
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
            raise Exit(1)
        all_plugins = {plugin_type: all_plugins[plugin_type]}

    match output_format:
        case OutputType.JSON:
            _output_json_format(all_plugins)
        case OutputType.TREE:
            _output_tree_format(all_plugins, detailed)
        case _:
            _output_table_format(all_plugins, detailed)


def _output_json_format(all_plugins: dict[str, dict[str, Info]]) -> None:
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
    rprint(dumps(json_output, indent=2, ensure_ascii=False))


def _output_tree_format(
    all_plugins: dict[str, dict[str, Info]], detailed: bool
) -> None:
    tree = Tree("🔌 [bold blue]Available plug-ins[/bold blue]")
    for ptype, plugins in all_plugins.items():
        type_branch = tree.add(
            f"📂 [yellow]{ptype.upper()}[/yellow] "
            f"([dim]{len(plugins)} plugins[/dim])"
        )
        for name, info in plugins.items():
            plugin_info = f"[green]{name}[/green]"
            if info.version:
                plugin_info += f" [dim]v{info.version}[/dim]"
            if info.description and detailed:
                plugin_info += f"\n   [italic]{info.description}[/italic]"
            type_branch.add(plugin_info)

    console.print(tree)


def _output_table_format(
    all_plugins: dict[str, dict[str, Info]], detailed: bool
) -> None:
    total_plugins = sum(len(plugins) for plugins in all_plugins.values())
    rprint(f"[blue]📊 Total plugins: {total_plugins}[/blue]\n")

    for ptype, plugins in all_plugins.items():
        if not plugins:
            continue

        table = Table(title=f"🔌 Plug-ins like: {ptype.upper()}")
        table.add_column("Name", style="cyan", width=20)
        table.add_column("Version", style="yellow", width=10)

        if detailed:
            table.add_column("Description", style="green")
            table.add_column("Module", style="blue", width=12)
        else:
            table.add_column("Description", style="green", max_width=50)

        for name, info in plugins.items():
            row = [
                name,
                info.version or "N/A",
                info.description or "No description",
            ]

            if detailed:
                row.append(info.type_module)

            table.add_row(*row)

        console.print(table)
