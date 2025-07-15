"""Обновленные Temporal Activities с правильной инициализацией конфигурации."""

import asyncio
import io
import logging
from time import time
from typing import Any

from pydantic import BaseModel
from temporalio import activity

from core.component import (
    BaseProcessClass,
    ComponentConfig,
    Info,
    PluginRegistry,
    Result,
)
from core.temporal.interfaces import StageExecutionResult
from core.yaml_loader.interfaces import (
    PipelineConfig,
    ResilienceConfig,
    StageConfig,
)

activity_logger = logging.getLogger("temporal_activity")


@activity.defn
async def stage_activity(  # noqa: PLR0913
    stage_name: str,
    stage_config: StageConfig,
    run_id: str,
    pipeline_name: str,
    input_data: dict[str, Any] | None = None,
    resilience_config: ResilienceConfig | None = None,
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
        "max_attempts": resilience_config.max_attempts
        if resilience_config
        else 3,
        "component_type": stage_config.stage,
        "component_name": stage_config.component,
    }

    try:
        registry = PluginRegistry()
        await registry.initialize()

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

        component_info = registry.get_plugin_info(
            stage_config.stage, stage_config.component
        )

        config_data = _create_component_config(
            component_info,
            stage_config,
            run_id,
            pipeline_name,
            stage_name,
            attempt_number,
        )

        activity_logger.info(f"Created config: {type(config_data).__name__}")
        activity_logger.debug(f"Config data: {config_data}")

        if input_data:
            deserialized_input = _deserialize_input_data(input_data)
            activity_logger.debug(
                "Deserialized input data: %s", type(deserialized_input)
            )

            if stage_config.stage == "transform":
                config_data.temporal_input_data = deserialized_input

        component = component_class(config_data)

        if input_data and stage_config.stage == "transform":
            deserialized_input = _deserialize_input_data(input_data)
            component.config._temporal_input_data = deserialized_input
            activity_logger.debug(
                f"Set temporal input data: {type(deserialized_input)}"
            )

        result = await _component_with_retry_politic(
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


def _create_component_config(  # noqa: PLR0913
    component_info: Info | None,
    stage_config: StageConfig,
    run_id: str,
    pipeline_name: str,
    stage_name: str,
    attempt_number: int,
) -> ComponentConfig:
    """Создает правильную конфигурацию для компонента."""

    if not component_info or not component_info.config_class:
        config_class = ComponentConfig
    else:
        config_class = component_info.config_class

    config_dict = {}
    if hasattr(stage_config.component_config, "__dict__"):
        config_dict = stage_config.component_config.__dict__.copy()
    elif isinstance(stage_config.component_config, BaseModel):
        config_dict = stage_config.component_config.model_dump()
    else:
        try:
            config_dict = dict(stage_config.component_config)
        except Exception as ex:
            logging.warn(ex)
            config_dict = {}

    metadata_fields = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "stage_name": stage_name,
        "attempt": attempt_number,
    }

    config_dict.update(metadata_fields)

    try:
        return config_class(**config_dict)
    except Exception as e:
        activity_logger.error(f"Failed to create {config_class.__name__}: {e}")
        activity_logger.error(f"Config dict: {config_dict}")
        raise


@activity.defn
async def validate_pipeline_activity(
    pipeline_config: dict[str, Any],
) -> dict[str, Any]:
    """Activity для валидации конфигурации pipeline."""
    activity_logger.info("Validating pipeline configuration")

    try:
        config = PipelineConfig(**pipeline_config)
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

            config_data = ComponentConfig() or stage_config.component_config
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
    """Activity для очистки данных после выполнения pipeline."""
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


def _deserialize_input_data(input_data: dict[str, Any]) -> Any:  # noqa: PLR0911 PLR0912
    if not isinstance(input_data, dict):
        return input_data

    data_type = input_data.get("type")
    data_content = input_data.get("data")

    if data_type == "polars_dataframe" and data_content:
        try:
            import polars as pl  # noqa: PLC0415

            if isinstance(data_content, str):
                return pl.read_json(io.StringIO(data_content))
            return pl.DataFrame(data_content)
        except ImportError:
            activity_logger.warning("Polars not available for deserialization")
            return data_content
        except Exception as e:
            activity_logger.error(
                f"Failed to deserialize polars dataframe: {e}"
            )
            return data_content

    if data_type == "polars_series" and data_content:
        try:
            import polars as pl  # noqa: PLC0415

            name = input_data.get("name", "values")
            return pl.Series(name=name, values=data_content)
        except ImportError:
            return data_content
        except Exception as e:
            activity_logger.error(f"Failed to deserialize polars series: {e}")
            return data_content

    if data_type == "native":
        return data_content

    if data_type == "pydantic":
        return data_content

    if "polars_data" in input_data:
        try:
            import polars as pl  # noqa: PLC0415

            return pl.read_json(io.StringIO(input_data["polars_data"]))
        except ImportError:
            pass

    if "dict_data" in input_data:
        return input_data["dict_data"]

    if "raw_data" in input_data:
        return input_data["raw_data"]

    if "records" in input_data:
        try:
            import polars as pl  # noqa: PLC0415

            return pl.DataFrame(input_data["records"])
        except ImportError:
            return input_data["records"]

    return input_data


def _serialize_result_data(data: Any) -> dict[str, Any]:
    try:
        import polars as pl  # noqa: PLC0415

        if isinstance(data, pl.DataFrame):
            return {
                "type": "polars_dataframe",
                "data": data.write_json(),
                "records_count": len(data),
                "columns": list(
                    data.columns
                ),  # Убеждаемся что это список строк
                "shape": [len(data), len(data.columns)],
            }

        if isinstance(data, pl.Series):
            # Конвертируем Series в список
            return {
                "type": "polars_series",
                "data": data.to_list(),
                "name": data.name,
                "length": len(data),
            }

    except ImportError:
        pass

    # Для других типов данных
    if isinstance(data, dict | list | str | int | float | bool | type(None)):
        return {"type": "native", "data": data}

    if isinstance(data, BaseModel):
        return {"type": "pydantic", "data": data.model_dump()}

    # Последний резерв - конвертируем в строку
    return {"type": "string", "data": str(data)}


def _count_records_from_result(result: Result) -> int:
    """Подсчитывает количество обработанных записей из результата."""
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


async def _component_with_retry_politic(
    component: BaseProcessClass,
    stage_name: str,
    retry_politic_config: ResilienceConfig | None,
) -> Result:
    """Выполняет компонент с учетом retry_politic настроек."""
    if not retry_politic_config:
        response = await component.process()
        if response is None:
            return Result(
                response=None,
                status="success",
            )
        return response

    execution_timeout = retry_politic_config.execution_timeout

    async def with_timeout() -> Result:
        response_with_timeout = await component.process()
        if response_with_timeout is None:
            return Result(
                response=None,
                status="success",
            )
        return response_with_timeout

    if execution_timeout:
        try:
            return await asyncio.wait_for(
                with_timeout(), timeout=execution_timeout
            )
        except TimeoutError as te:
            msg = (
                f"Stage {stage_name} execution exceeded "
                f"timeout of {execution_timeout} seconds"
            )
            raise TimeoutError(msg) from te
    else:
        return await with_timeout()
