from typing import Any, Literal

from pydantic import BaseModel, Field

from core.yaml_loader.interfaces.resilience import ResilienceConfig


class StageConfig(BaseModel):
    stage: Literal["extract", "transform", "load"]
    component: str = Field(..., min_length=1)
    depends_on: list[str] = Field(default_factory=list)
    component_config: dict[str, Any] = Field(default_factory=dict)

    resilience: ResilienceConfig = Field(default_factory=ResilienceConfig)
