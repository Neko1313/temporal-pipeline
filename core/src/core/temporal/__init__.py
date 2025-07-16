"""Temporal workflow и activities для обработки данных."""

from core.temporal.activities import (
    activity_stage,
    cleanup_pipeline_data_activity,
    validate_pipeline_activity,
)
from core.temporal.interfaces import (
    PipelineExecutionResult,
    StageExecutionResult,
)
from core.temporal.scheduled_workflow import ScheduledPipelineWorkflow
from core.temporal.workflow import DataPipelineWorkflow

__all__ = [
    # Workflows
    "DataPipelineWorkflow",
    "PipelineExecutionResult",
    "ScheduledPipelineWorkflow",
    # Interfaces
    "StageExecutionResult",
    # Activities
    "activity_stage",
    "cleanup_pipeline_data_activity",
    "validate_pipeline_activity",
]
