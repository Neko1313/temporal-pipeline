from core.yaml_loader.interfaces.stage import StageConfig


def has_circular_dependencies(stages: dict[str, StageConfig]) -> bool:
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

    for stage_name in stages:
        if stage_name not in visited:
            if dfs(stage_name):
                return True

    return False
