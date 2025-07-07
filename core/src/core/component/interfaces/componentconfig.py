from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ComponentConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str | None = Field(default=None)
    pipeline_name: str | None = Field(default=None)
    stage_name: str | None = Field(default=None)
    attempt: int | None = Field(default=None)
    input_data: Any = Field(default=None)
