"""Utils parsing yaml file."""

from core.yaml_loader.interfaces.stage import StageConfig


def has_circular_dependencies(stages: dict[str, StageConfig]) -> bool:
    """
    Check if the given stages have a circular dependency.

    stages: Stages configuration

    return: is circular dependency
    """
    if not stages:
        return False

    if not isinstance(stages, dict):
        msg = "stages must be a dictionary"
        raise TypeError(msg)

    visited = set()
    rec_stack = set()

    def dfs(stage_name: str) -> bool:
        visited.add(stage_name)
        rec_stack.add(stage_name)

        stage = stages.get(stage_name)
        if stage:
            for dep in stage.depends_on:
                if dep not in visited:
                    if dfs(dep):
                        return True
                elif dep in rec_stack:
                    return True

        rec_stack.remove(stage_name)
        return False

    return any(
        stage_name not in visited and dfs(stage_name) for stage_name in stages
    )
