import logging
from time import time

from core.temporal.interfaces import StageExecutionResult
from core.temporal.utils.activities import should_retry_exception

from core.component import PluginRegistry
from core.interfaces import ExecutionContext
from core.yaml_loader.interfaces import StageConfig
from core.resilience import (
    RetryManager,
    RetryConfig,
    CircuitBreaker,
    CircuitBreakerConfig,
)

from temporalio import activity

activity_logger = logging.getLogger("temporal_activity")


@activity.defn
async def execute_stage_activity(
    stage_name: str,
    stage_config: StageConfig,
    run_id: str,
    pipeline_name: str,
    input_data: dict[str, any] | None = None,
    resilience_config: dict[str, Any] | None = None,
) -> StageExecutionResult:
    activity_logger.info(f"Executing stage: {stage_name} with resilience config")
    start_time = time()

    activity_info = activity.info()
    attempt_number = activity_info.attempt

    resilience_info = {
        "attempt": attempt_number,
        "max_attempts": resilience_config.get("max_attempts", 3)
        if resilience_config
        else 3,
        "retry_policy": resilience_config.get("retry_policy", "exponential_backoff")
        if resilience_config
        else "exponential_backoff",
        "circuit_breaker_enabled": resilience_config.get(
            "circuit_breaker_enabled", True
        )
        if resilience_config
        else True,
    }

    try:
        activity_logger.info(
            f"Stage {stage_name} - Attempt {attempt_number}/{resilience_info['max_attempts']}"
        )

        # Создаем resilience компоненты если конфигурация предоставлена
        retry_manager = None
        circuit_breaker = None

        if resilience_config:
            # Создаем retry manager
            retry_config = RetryConfig(
                max_attempts=resilience_config.get("max_attempts", 3),
                initial_delay=resilience_config.get("initial_delay", 1.0),
                max_delay=resilience_config.get("max_delay", 60.0),
                policy=resilience_config.get("retry_policy", "exponential_backoff"),
                jitter=resilience_config.get("jitter", True),
                backoff_multiplier=resilience_config.get("backoff_multiplier", 2.0),
            )
            retry_manager = RetryManager(retry_config)

            # Создаем circuit breaker если включен
            if resilience_config.get("circuit_breaker_enabled", True):
                cb_config = CircuitBreakerConfig(
                    failure_threshold=resilience_config.get("failure_threshold", 5),
                    recovery_timeout=resilience_config.get("recovery_timeout", 60),
                    success_threshold=resilience_config.get("success_threshold", 3),
                )
                circuit_breaker = CircuitBreaker(cb_config)

        # Инициализируем registry
        registry = PluginRegistry()
        await registry.initialize()

        # Создаем контекст выполнения
        context = ExecutionContext(
            run_id=run_id,
            pipeline_name=pipeline_name,
            stage_name=stage_name,
            attempt=attempt_number,
            metadata={
                "resilience_config": resilience_config,
                "input_data_size": len(input_data) if input_data else 0,
            },
        )

        # Парсим конфигурацию стадии
        stage_cfg = StageConfig(**stage_config)

        # Получаем соответствующий плагин
        group_map = {
            "extract": "extract_processors",
            "transform": "transform_processors",
            "load": "load_processors",
        }

        plugin_group = group_map[stage_cfg.stage]

        # Создаем экземпляр плагина
        plugin_instance = await registry.get_plugin_instance(
            plugin_group, stage_cfg.component, stage_cfg.component_config
        )

        # Функция для выполнения стадии с retry логикой
        async def execute_stage_with_resilience():
            if stage_cfg.stage == "extract":
                return await _execute_extract_stage(plugin_instance, context)
            elif stage_cfg.stage == "transform":
                return await _execute_transform_stage(
                    plugin_instance, context, input_data
                )
            elif stage_cfg.stage == "load":
                return await _execute_load_stage(plugin_instance, context, input_data)
            else:
                raise ValueError(f"Unknown stage type: {stage_cfg.stage}")

        # Выполняем стадию с resilience компонентами
        result = None

        if circuit_breaker:
            # Если circuit breaker включен, используем его
            async def cb_operation():
                if retry_manager:
                    return await retry_manager.execute_with_retry(
                        execute_stage_with_resilience,
                        should_retry=should_retry_exception,
                    )
                else:
                    return await execute_stage_with_resilience()

            result = await circuit_breaker.execute(cb_operation)
        elif retry_manager:
            # Если только retry manager
            result = await retry_manager.execute_with_retry(
                execute_stage_with_resilience, should_retry=should_retry_exception
            )
        else:
            # Без resilience компонентов
            result = await execute_stage_with_resilience()

        execution_time = time() - start_time

        # Обновляем resilience info с результатами
        resilience_info.update(
            {
                "successful_attempt": attempt_number,
                "total_execution_time": execution_time,
                "circuit_breaker_state": "closed" if circuit_breaker else "not_used",
            }
        )

        # Убеждаемся что metadata не None
        metadata = result.metadata if result and result.metadata else {}
        metadata.update(
            {
                "execution_context": {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "attempt": attempt_number,
                }
            }
        )

        return StageExecutionResult(
            stage_name=stage_name,
            status="success",
            records_processed=result.records_processed if result else 0,
            execution_time=execution_time,
            metadata=metadata,
            resilience_info=resilience_info,
        )

    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = str(e)
        error_type = type(e).__name__

        activity_logger.error(
            f"Stage {stage_name} failed on attempt {attempt_number}: {error_msg}"
        )

        # Обновляем resilience info с информацией об ошибке
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
                "attempt": attempt_number,
                "execution_context": {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "attempt": attempt_number,
                },
            },
            resilience_info=resilience_info,
        )
