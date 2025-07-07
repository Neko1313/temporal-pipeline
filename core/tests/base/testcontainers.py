from typing import Any

from testcontainers.core.generic import DockerContainer


class TemporalContainer(DockerContainer):
    def __init__(
        self,
        image: str = "temporalio/auto-setup:latest",
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(image, **kwargs)
        self.with_exposed_ports(7233, 8080)  # Temporal gRPC и UI
        self.with_env("DB", "sqlite")
        self.with_env("SQLITE_PRAGMA_journal_mode", "WAL")

    def get_temporal_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(7233)
        return f"{host}:{port}"

    def get_temporal_ui_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(8080)
        return f"http://{host}:{port}"
