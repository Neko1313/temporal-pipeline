"""
Обновленные Temporal Activities с универсальной архитектурой BaseProcessClass
"""

import asyncio
import logging
from time import time
from typing import Any

from temporalio import activity

from core.component import (
    BaseProcessClass,
    ComponentConfig,
    PluginRegistry,
    Result,
)
from core.temporal.interfaces import StageExecutionResult
from core.yaml_loader.interfaces import StageConfig

activity_logger = logging.getLogger("temporal_activity")


@activity.defn
async def execute_stage_activity(  # noqa: PLR0913
    stage_name: str,
    stage_config: StageConfig,
    run_id: str,
    pipeline_name: str,
    input_data: dict[str, Any] | None = None,
    resilience_config: dict[str, Any] | None = None,
) -> StageExecutionResult:
    activity_logger.info(
        "Executing stage: %s (%s.%s)",
        stage_name,
        stage_config.stage,
        stage_config.component,
    )
    start_time = time()

    activity_info = activity.info()
    attempt_number = activity_info.attempt

    resilience_info = {
        "attempt": attempt_number,
        "max_attempts": resilience_config.get("max_attempts", 3)
        if resilience_config
        else 3,
        "component_type": stage_config.stage,
        "component_name": stage_config.component,
    }

    try:
        # ИЗМЕНЕНИЕ: Используем новый PluginRegistry
        registry = PluginRegistry()
        await registry.initialize()

        # ИЗМЕНЕНИЕ: Универсальное получение компонента (без group_map)
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

        component = component_class()

        config_data = (
            stage_config.component_config.model_dump()
            if stage_config.component_config
            else {}
        )

        config_data.update(
            {
                "run_id": run_id,
                "pipeline_name": pipeline_name,
                "stage_name": stage_name,
                "attempt": attempt_number,
            }
        )

        if input_data:
            deserialized_input = _deserialize_input_data(input_data)
            config_data["input_data"] = deserialized_input

        component.config = ComponentConfig(**config_data)

        result = await _execute_component_with_resilience(
            component, stage_name, resilience_config
        )

        execution_time = time() - start_time

        if result.status == "success":
            records_processed = _count_records_from_result(result)

            metadata = {
                "execution_context": {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "stage_name": stage_name,
                    "attempt": attempt_number,
                },
                "component_info": {
                    "type": stage_config.stage,
                    "name": stage_config.component,
                },
                "performance": {
                    "execution_time": execution_time,
                    "records_processed": records_processed,
                },
            }

            # Сериализуем результат для передачи следующим стадиям
            if result.response is not None:
                metadata["result_data"] = _serialize_result_data(
                    result.response
                )

            activity_logger.info(
                "Stage %s completed successfully: %s records in %s s",
                stage_name,
                records_processed,
                execution_time,
            )

            return StageExecutionResult(
                stage_name=stage_name,
                status="success",
                records_processed=records_processed,
                execution_time=execution_time,
                metadata=metadata,
                resilience_info=resilience_info,
            )
        error_msg = "Component returned error status"
        activity_logger.error(f"Stage {stage_name} failed: {error_msg}")

        return StageExecutionResult(
            stage_name=stage_name,
            status="failed",
            execution_time=execution_time,
            error_message=error_msg,
            metadata={
                "execution_context": {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "stage_name": stage_name,
                    "attempt": attempt_number,
                },
                "component_info": {
                    "type": stage_config.stage,
                    "name": stage_config.component,
                },
            },
            resilience_info=resilience_info,
        )

    except Exception as e:
        execution_time = time() - start_time
        error_msg = str(e)
        error_type = type(e).__name__

        activity_logger.error(
            f"Stage {stage_name} "
            f"({stage_config.stage}.{stage_config.component}) "
            f"failed on attempt {attempt_number}: {error_msg}"
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
            stage_name=stage_name,
            status="failed",
            execution_time=execution_time,
            error_message=error_msg,
            metadata={
                "error_type": error_type,
                "execution_context": {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "stage_name": stage_name,
                    "attempt": attempt_number,
                },
                "component_info": {
                    "type": stage_config.stage,
                    "name": stage_config.component,
                },
            },
            resilience_info=resilience_info,
        )


@activity.defn
async def validate_pipeline_activity(
    pipeline_config: dict[str, Any],
) -> dict[str, Any]:
    """
    ОБНОВЛЕННАЯ УНИВЕРСАЛЬНАЯ activity для валидации конфигурации pipeline
    """
    activity_logger.info("Validating pipeline configuration")

    try:
        from core.yaml_loader.interfaces import PipelineConfig  # noqa: PLC0415

        # Валидируем структуру конфигурации
        config = PipelineConfig(**pipeline_config)

        # ИЗМЕНЕНИЕ: Используем новый PluginRegistry
        registry = PluginRegistry()
        await registry.initialize()

        validation_errors = []

        for stage_name, stage_config in config.stages.items():
            component_info = registry.get_plugin_info(
                stage_config.stage, stage_config.component
            )
            if not component_info:
                available = registry.list_plugins(stage_config.stage)
                validation_errors.append(
                    f"Stage '{stage_name}': "
                    f"Component '{stage_config.component}' "
                    f"of type '{stage_config.stage}' not found. "
                    f"Available: {available.get(stage_config.stage, [])}"
                )
                continue

            # Валидируем конфигурацию компонента
            config_data = (
                stage_config.component_config.model_dump()
                if stage_config.component_config
                else {}
            )
            validation_result = await registry.validate_component_config(
                stage_config.stage, stage_config.component, config_data
            )

            if not validation_result["valid"]:
                validation_errors.extend(
                    [
                        f"Stage '{stage_name}': {error}"
                        for error in validation_result["errors"]
                    ]
                )

        return {
            "valid": len(validation_errors) == 0,
            "errors": validation_errors,
            "total_stages": len(config.stages),
            "available_components": registry.list_plugins(),
        }

    except Exception as e:
        activity_logger.error(f"Pipeline validation failed: {e!s}")
        return {
            "valid": False,
            "errors": [f"Configuration parsing failed: {e!s}"],
            "total_stages": 0,
            "available_components": {},
        }


@activity.defn
async def cleanup_pipeline_data_activity(
    run_id: str,
    pipeline_name: str,
    cleanup_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    СОХРАНЯЕМ КАК ЕСТЬ - activity для очистки данных после выполнения pipeline
    """
    activity_logger.info(f"Cleaning up pipeline data for run_id: {run_id}")

    try:
        cleanup_results = {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "cleaned_items": [],
            "errors": [],
        }

        if cleanup_config:
            if cleanup_config.get("clear_temp_files", False):
                cleanup_results["cleaned_items"].append("temp_files")

            if cleanup_config.get("clear_cache", False):
                cleanup_results["cleaned_items"].append("cache")

        activity_logger.info(
            "Cleanup completed for %s: %s items cleaned",
            run_id,
            len(cleanup_results["cleaned_items"]),
        )

        return cleanup_results

    except Exception as e:
        activity_logger.error(f"Cleanup failed for {run_id}: {e!s}")
        return {
            "run_id": run_id,
            "pipeline_name": pipeline_name,
            "cleaned_items": [],
            "errors": [str(e)],
        }


# === HELPER ФУНКЦИИ ===


def _deserialize_input_data(data: dict[str, Any]) -> Any:  # noqa: PLR0911
    """Десериализует входные данные из metadata предыдущих стадий"""
    if "polars_data" in data:
        try:
            import polars as pl  # noqa: PLC0415

            return pl.read_json(data["polars_data"])
        except ImportError:
            # Fallback если polars недоступен
            pass

    if "dict_data" in data:
        return data["dict_data"]
    if "pydantic_data" in data:
        return data["pydantic_data"]
    if "raw_data" in data:
        return data["raw_data"]
    if "records" in data:
        # Поддержка старого формата
        try:
            import polars as pl  # noqa: PLC0415

            return pl.DataFrame(data["records"])
        except ImportError:
            return data["records"]
    else:
        return data


def _serialize_result_data(data: Any) -> dict[str, Any]:
    """Сериализует результат для передачи между стадиями"""
    try:
        # Lazy import polars для избежания проблем в temporal sandbox
        import polars as pl  # noqa: PLC0415

        if isinstance(data, pl.DataFrame):
            return {
                "polars_data": data.write_json(),
                "records_count": len(data),
                "columns": data.columns,
            }
    except ImportError:
        pass

    if isinstance(data, dict):
        return {"dict_data": data}
    if hasattr(data, "model_dump"):
        return {"pydantic_data": data.model_dump()}
    return {"raw_data": str(data)}


def _count_records_from_result(result: Result) -> int:
    """Подсчитывает количество обработанных записей из результата"""
    if result.response is None:
        return 0

    try:
        import polars as pl  # noqa: PLC0415

        if isinstance(result.response, pl.DataFrame):
            return len(result.response)
    except ImportError:
        pass

    if isinstance(result.response, dict):
        if "records_count" in result.response:
            return result.response["records_count"]
        if "records_loaded" in result.response:
            return result.response["records_loaded"]
        if "records" in result.response:
            return len(result.response["records"])

    return 1


async def _execute_component_with_resilience(
    component: BaseProcessClass,
    stage_name: str,
    resilience_config: dict[str, Any] | None,
) -> Result:
    """Выполняет компонент с учетом resilience настроек"""
    if not resilience_config:
        return await component.process()

    execution_timeout = resilience_config.get("execution_timeout")

    async def execute_with_timeout() -> Result:
        return await component.process()

    if execution_timeout:
        try:
            return await asyncio.wait_for(
                execute_with_timeout(), timeout=execution_timeout
            )
        except TimeoutError as te:
            msg = f"Stage {stage_name} execution\
            exceeded timeout of {execution_timeout} seconds"
            raise TimeoutError(msg) from te
    else:
        return await execute_with_timeout()
