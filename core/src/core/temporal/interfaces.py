"""Интерфейсы для Temporal workflows."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from core.temporal.constants import ExecutionStatus
from core.yaml_loader.interfaces.pipeline import PipelineConfig
from core.yaml_loader.interfaces.resilience import ResilienceConfig
from core.yaml_loader.interfaces.stage import StageConfig


class StageExecutionResult(BaseModel):
    """Результат выполнения отдельного этапа пайплайна."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    stage_name: str
    status: ExecutionStatus
    records_processed: int = 0
    execution_time_seconds: float = Field(0.0, alias="execution_time")
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    resilience_info: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Проверяет, успешно ли выполнен этап."""
        return self.status == "success"

    @property
    def is_failed(self) -> bool:
        """Проверяет, завершился ли этап с ошибкой."""
        return self.status == "failed"

    @property
    def has_error(self) -> bool:
        """Проверяет наличие ошибки."""
        return self.error_message is not None


class PipelineExecutionResult(BaseModel):
    """Результат выполнения всего пайплайна."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    pipeline_name: str
    run_id: str
    status: ExecutionStatus
    total_records_processed: int = 0
    total_execution_time_seconds: float = Field(
        0.0, alias="total_execution_time"
    )
    stage_results: list[StageExecutionResult] = Field(default_factory=list)
    error_message: str | None = None
    execution_metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Проверяет, успешно ли выполнен пайплайн."""
        return self.status == "success"

    @property
    def failed_stages(self) -> list[StageExecutionResult]:
        """Возвращает список этапов, завершившихся с ошибкой."""
        return [stage for stage in self.stage_results if stage.is_failed]

    @property
    def successful_stages(self) -> list[StageExecutionResult]:
        """Возвращает список успешно выполненных этапов."""
        return [stage for stage in self.stage_results if stage.is_success]


class ErrorMetadata(BaseModel):
    pipeline_config: PipelineConfig
    run_id: str
    start_time: datetime
    stage_results: list[StageExecutionResult]
    metadata: dict[str, Any]
    error: Exception


class StageActivity(BaseModel):
    stage_name: str
    stage_config: StageConfig
    run_id: str
    pipeline_name: str
    input_data: dict[str, Any] | None = None
    resilience_config: ResilienceConfig | None = None
