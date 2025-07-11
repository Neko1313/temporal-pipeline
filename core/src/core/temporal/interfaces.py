from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StageExecutionResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    stage_name: str
    status: str
    records_processed: int = 0
    execution_time: float = 0.0
    error_message: str | None = None
    metadata: dict[str, Any] | None = Field(default_factory=dict)
    resilience_info: dict[str, Any] = Field(default_factory=dict)


class PipelineExecutionResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    pipeline_name: str
    run_id: str
    status: str
    total_records_processed: int = 0
    total_execution_time: float = 0.0
    stage_results: list[StageExecutionResult] | None = Field(
        default_factory=list
    )
    error_message: str | None = None
    execution_metadata: dict[str, Any] = Field(default_factory=dict)
