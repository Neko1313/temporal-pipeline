from pydantic import BaseModel, ConfigDict
from polars import DataFrame
from typing import Literal


class Result(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    response: DataFrame | dict | BaseModel | None
    status: Literal["error", "success", "processing"]
