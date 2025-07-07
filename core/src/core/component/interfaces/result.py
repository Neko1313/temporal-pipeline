from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class Result(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    response: Any
    status: Literal["error", "success", "processing"]
