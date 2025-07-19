from typing import Any

from testcontainers.core.generic import DockerContainer
from tests.base.const import TEMPORAL_GRPC_PORT, TEMPORAL_UI_PORT


class TemporalContainer(DockerContainer):
    def __init__(
        self,
        image: str = "temporalio/auto-setup:latest",
        **kwargs: dict[str, Any],
    ) -> None:
        super().__init__(image, **kwargs)
        self.with_exposed_ports(TEMPORAL_GRPC_PORT, TEMPORAL_UI_PORT)
        self.with_env("DB", "sqlite")
        self.with_env("SQLITE_PRAGMA_journal_mode", "WAL")

    def get_temporal_url(self) -> str:
        host = self.get_container_host_ip()
        port = self.get_exposed_port(TEMPORAL_GRPC_PORT)
        return f"{host}:{port}"

    def get_temporal_ui_url(self) -> str:
        if not self._container:
            raise RuntimeError("No temporal container")

        host = self.get_container_host_ip()
        port = self.get_exposed_port(TEMPORAL_UI_PORT)
        if not port:
            raise RuntimeError("Temporal UI port not exposed")

        return f"http://{host}:{port}"
