from collections.abc import Generator
from typing import Literal

import pytest

from tests.base import TemporalContainer


@pytest.fixture(scope="session")
def anyio_backend() -> Literal["asyncio"]:
    return "asyncio"


@pytest.fixture(scope="session")
def temporal_container_service(
    anyio_backend: Literal["asyncio"],  # noqa: ARG001
) -> Generator[TemporalContainer]:
    with TemporalContainer() as temporal_container:
        yield temporal_container
