"""Temporal Activities для выполнения этапов пайплайна."""

import asyncio
import io
import logging
from time import time
from typing import Any

from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarDataFrame
from pydantic import BaseModel
from temporalio import activity

from core.component import (
    BaseProcessClass,
    ComponentConfig,
    Info,
    PluginRegistry,
    Result,
)
from core.temporal.constants import ComponentType, ExecutionStatus
from core.temporal.interfaces import StageActivity, StageExecutionResult
from core.yaml_loader.interfaces import (
    PipelineConfig,
    ResilienceConfig,
    StageConfig,
)

activity_logger = logging.getLogger("temporal_activity")


@activity.defn
async def stage_activity(
    stage_activity: StageActivity,
) -> StageExecutionResult:
    """Выполняет отдельный этап пайплайна."""
    activity_logger.info(
        "Executing stage: %s (%s.%s)",
        stage_activity.stage_name,
        stage_activity.stage_config.stage,
        stage_activity.stage_config.component,
    )

    start_time = time()
    attempt_number = activity.info().attempt

    resilience_info = _build_resilience_info(
        attempt_number,
        stage_activity.resilience_config,
        stage_activity.stage_config,
    )

    try:
        component = await _initialize_component(
            stage_activity,
            attempt_number,
        )

        result = await _execute_component(
            component, stage_activity.resilience_config
        )

        return _process_success_result(
            stage_activity,
            result,
            attempt_number,
            start_time,
            resilience_info,
        )

    except Exception as e:
        return _process_error_result(
            stage_activity,
            e,
            attempt_number,
            start_time,
            resilience_info,
        )


@activity.defn
async def validate_pipeline_activity(
    pipeline_config: PipelineConfig,
) -> dict[str, Any]:
    """Валидирует конфигурацию пайплайна."""
    activity_logger.info("Validating pipeline: %s", pipeline_config.name)

    registry = PluginRegistry()
    await registry.initialize()

    validation_errors = []

    for stage_name, stage_config in pipeline_config.stages.items():
        error = _validate_stage_component(registry, stage_name, stage_config)
        if error:
            validation_errors.append(error)

    if validation_errors:
        return {
            "valid": False,
            "errors": validation_errors,
        }

    return {
        "valid": True,
        "stages_count": len(pipeline_config.stages),
    }


@activity.defn
async def cleanup_pipeline_data_activity(
    run_id: str,
    pipeline_name: str,
) -> dict[str, Any]:
    """Очищает временные данные пайплайна."""
    activity_logger.info(
        "Cleaning up data for pipeline: %s, run: %s",
        pipeline_name,
        run_id,
    )

    # Здесь может быть логика очистки
    # Например, удаление временных файлов, очистка кэша и т.д.

    return {
        "status": "cleaned",
        "run_id": run_id,
        "pipeline_name": pipeline_name,
    }


# === Приватные вспомогательные функции ===


def _build_resilience_info(
    attempt_number: int,
    resilience_config: ResilienceConfig | None,
    stage_config: StageConfig,
) -> dict[str, Any]:
    """Создает информацию о политике устойчивости."""
    return {
        "attempt": attempt_number,
        "max_attempts": resilience_config.max_attempts
        if resilience_config
        else 3,
        "component_type": stage_config.stage,
        "component_name": stage_config.component,
    }


async def _initialize_component(
    stage_activity: StageActivity,
    attempt_number: int,
) -> BaseProcessClass:
    """Инициализирует компонент для выполнения."""
    registry = PluginRegistry()
    await registry.initialize()

    component_class = _get_component_class(
        registry, stage_activity.stage_config
    )
    component_info = registry.get_plugin_info(
        stage_activity.stage_config.stage,
        stage_activity.stage_config.component,
    )

    config_data = _create_component_config(
        stage_activity,
        component_info,
        attempt_number,
    )

    if (
        stage_activity.input_data
        and stage_activity.stage_config.stage == ComponentType.TRANSFORM
    ):
        config_data.temporal_input_data = _deserialize_input_data(
            stage_activity.input_data
        )

    return component_class(config_data)


def _get_component_class(
    registry: PluginRegistry,
    stage_config: StageConfig,
) -> type[BaseProcessClass]:
    """Получает класс компонента из реестра."""
    component_class = registry.get_plugin(
        stage_config.stage, stage_config.component
    )

    if not component_class:
        available = registry.list_plugins(stage_config.stage)
        msg = (
            f"Component '{stage_config.component}' "
            f"of type '{stage_config.stage}' not found. "
            f"Available: {available.get(stage_config.stage, [])}"
        )
        raise ValueError(msg)

    return component_class


def _create_component_config(
    stage_activity: StageActivity,
    component_info: Info | None,
    attempt_number: int,
) -> ComponentConfig:
    """Создает конфигурацию для компонента."""
    if not component_info or not component_info.config_class:
        return ComponentConfig(
            run_id=stage_activity.run_id,
            pipeline_name=stage_activity.pipeline_name,
            stage_name=stage_activity.stage_name,
            attempt=attempt_number,
        )

    config_data = {
        **stage_activity.stage_config.component_config,
        "run_id": stage_activity.run_id,
        "pipeline_name": stage_activity.pipeline_name,
        "stage_name": stage_activity.stage_name,
        "attempt": attempt_number,
    }

    return component_info.config_class(**config_data)


async def _execute_component(
    component: BaseProcessClass,
    resilience_config: ResilienceConfig | None,
) -> Result | None:
    """Выполняет компонент с учетом политики повторных попыток."""
    if resilience_config and resilience_config.execution_timeout:
        return await asyncio.wait_for(
            component.process(),
            timeout=resilience_config.execution_timeout,
        )

    return await component.process()


def _process_success_result(
    stage_activity: StageActivity,
    result: Result | None,
    attempt_number: int,
    start_time: float,
    resilience_info: dict[str, Any],
) -> StageExecutionResult:
    """Обрабатывает успешный результат выполнения."""
    execution_time = time() - start_time

    if result is not None and result.status != ExecutionStatus.SUCCESS:
        return _create_failed_stage_result(
            stage_activity.stage_name,
            "Component returned error status",
            execution_time,
            {
                "run_id": stage_activity.run_id,
                "pipeline_name": stage_activity.pipeline_name,
                "stage_name": stage_activity.stage_name,
                "attempt": attempt_number,
                "component_type": stage_activity.stage_config.stage,
                "component_name": stage_activity.stage_config.component,
            },
            resilience_info,
        )

    records_processed = _count_records_from_result(result)
    metadata = _build_success_metadata(
        stage_activity,
        result,
        attempt_number,
        execution_time,
        records_processed,
    )

    activity_logger.info(
        "Stage %s completed successfully: %s records in %.2fs",
        stage_activity.stage_name,
        records_processed,
        execution_time,
    )

    return StageExecutionResult(
        stage_name=stage_activity.stage_name,
        status=ExecutionStatus.SUCCESS,
        records_processed=records_processed,
        execution_time=execution_time,
        metadata=metadata,
        resilience_info=resilience_info,
    )


def _process_error_result(
    stage_activity: StageActivity,
    error: Exception,
    attempt_number: int,
    start_time: float,
    resilience_info: dict[str, Any],
) -> StageExecutionResult:
    """Обрабатывает ошибку выполнения."""
    execution_time = time() - start_time
    error_message = str(error)
    error_type = type(error).__name__

    activity_logger.error(
        "Stage %s (%s.%s) failed on attempt %s: %s",
        stage_activity.stage_name,
        stage_activity.stage_config.stage,
        stage_activity.stage_config.component,
        attempt_number,
        error_message,
    )

    resilience_info.update(
        {
            "failed_attempt": attempt_number,
            "error_type": error_type,
            "total_execution_time": execution_time,
            "will_retry": attempt_number < resilience_info["max_attempts"],
        }
    )

    return StageExecutionResult(
        stage_name=stage_activity.stage_name,
        status=ExecutionStatus.FAILED,
        execution_time=execution_time,
        error_message=error_message,
        metadata={
            "error_type": error_type,
            "execution_context": {
                "run_id": stage_activity.run_id,
                "pipeline_name": stage_activity.pipeline_name,
                "stage_name": stage_activity.stage_name,
                "attempt": attempt_number,
            },
            "component_info": {
                "type": stage_activity.stage_config.stage,
                "name": stage_activity.stage_config.component,
            },
        },
        resilience_info=resilience_info,
    )


def _validate_stage_component(
    registry: PluginRegistry,
    stage_name: str,
    stage_config: StageConfig,
) -> dict[str, str] | None:
    """Валидирует компонент этапа."""
    component_info = registry.get_plugin_info(
        stage_config.stage, stage_config.component
    )

    if not component_info:
        return {
            "stage": stage_name,
            "error": f"Component '{stage_config.component}' "
            f"of type '{stage_config.stage}' not found",
        }

    return None


def _create_failed_stage_result(
    stage_name: str,
    error_message: str,
    execution_time: float,
    context: dict[str, Any],
    resilience_info: dict[str, Any],
) -> StageExecutionResult:
    """Создает результат для неуспешного этапа."""
    return StageExecutionResult(
        stage_name=stage_name,
        status=ExecutionStatus.FAILED,
        execution_time=execution_time,
        error_message=error_message,
        metadata={
            "execution_context": context,
            "component_info": {
                "type": context["component_type"],
                "name": context["component_name"],
            },
        },
        resilience_info=resilience_info,
    )


def _build_success_metadata(
    stage_activity: StageActivity,
    result: Result | None,
    attempt_number: int,
    execution_time: float,
    records_processed: int,
) -> dict[str, Any]:
    """Создает метаданные для успешного результата."""
    metadata = {
        "execution_context": {
            "run_id": stage_activity.run_id,
            "pipeline_name": stage_activity.pipeline_name,
            "stage_name": stage_activity.stage_name,
            "attempt": attempt_number,
        },
        "component_info": {
            "type": stage_activity.stage_config.stage,
            "name": stage_activity.stage_config.component,
        },
        "performance": {
            "execution_time": execution_time,
            "records_processed": records_processed,
        },
    }

    if result is None:
        metadata["result_data"] = _serialize_result_data(result)
        return metadata

    if result.response is not None:
        metadata["result_data"] = _serialize_result_data(result.response)

    return metadata


def _count_records_from_result(result: Result | None) -> int:
    """Подсчитывает количество обработанных записей."""
    if result is None:
        return 0

    if result.response is None:
        return 0

    if isinstance(result.response, list | tuple):
        return len(result.response)

    if hasattr(result.response, "__len__"):
        return len(result.response)

    return 1


def _polars_serialize(data: PolarDataFrame) -> dict[str, Any]:
    return {
        "type": "polars_dataframe",
        "shape": data.shape,
        "columns": data.columns,
        "dtypes": {
            col: str(dtype)
            for col, dtype in zip(data.columns, data.dtypes, strict=False)
        },
        "rows_sample": data.head(5).to_dicts() if len(data) > 0 else [],
        "row_count": len(data),
    }


def _pandas_serialize(data: PandasDataFrame) -> dict[str, Any]:
    return {
        "type": "pandas_dataframe",
        "shape": data.shape,
        "columns": data.columns.tolist(),
        "dtypes": {col: str(dtype) for col, dtype in data.dtypes.items()},
        "rows_sample": data.head(5).to_dict("records")
        if len(data) > 0
        else [],
        "row_count": len(data),
    }


def _serialize_result_data(data: Any) -> Any:  # noqa: PLR0911
    """Сериализует данные результата для сохранения."""
    if isinstance(data, PolarDataFrame):
        return _polars_serialize(data)

    if isinstance(data, PandasDataFrame):
        return _pandas_serialize(data)

    if isinstance(data, BaseModel):
        return data.model_dump()

    if isinstance(data, list | tuple):
        return [_serialize_result_data(item) for item in data]

    if isinstance(data, dict):
        return {
            key: _serialize_result_data(value) for key, value in data.items()
        }

    if isinstance(data, io.IOBase):
        return {"type": "io_stream", "class": type(data).__name__}

    # Для любых других неизвестных типов
    if hasattr(data, "__class__"):
        class_name = data.__class__.__name__
        module_name = getattr(data.__class__, "__module__", "unknown")
        return {
            "type": "unsupported_object",
            "class": class_name,
            "module": module_name,
            "repr": repr(data)[:100],
        }

    return data


def _deserialize_input_data(data: dict[str, Any]) -> dict[str, Any]:
    """Десериализует входные данные."""
    return data.get("dependencies", {})
