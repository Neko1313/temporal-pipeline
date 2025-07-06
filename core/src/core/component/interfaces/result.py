from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from polars import DataFrame


class Result(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    response: "DataFrame | dict | BaseModel | None"
    status: Literal["error", "success", "processing"]
