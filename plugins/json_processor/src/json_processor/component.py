import logging
from datetime import UTC, datetime

import polars as pl

from core.component import BaseProcessClass, Info, Result
from json_processor.config import JSONTransformConfig

logger = logging.getLogger(__name__)


class JSONTransform(BaseProcessClass[JSONTransformConfig]):
    def __init__(self, config: JSONTransformConfig) -> None:
        super().__init__(config)

    async def process(self) -> Result:
        """Основной метод обработки."""
        try:
            logger.info("Starting JSON transformation")

            input_data = self._get_input_data()
            if input_data is None or len(input_data) == 0:
                logger.warning("No input data provided")
                return Result(status="success", response=pl.DataFrame())

            data = input_data.clone()
            original_rows = len(data)
            logger.info(
                "Processing %s rows with %s columns",
                original_rows,
                len(data.columns),
            )

            data = await self._apply_json_normalization(data)
            data = await self._apply_column_operations(data)
            data = await self._apply_filtering(data)
            data = await self._apply_join_operations(data)
            data = await self._apply_aggregation(data)
            data = await self._apply_deduplication(data)
            data = await self._apply_null_handling(data)
            data = await self._apply_sorting(data)
            data = await self._apply_sampling(data)

            if self.config.add_metadata:
                data = self._add_processing_metadata(data)

            final_rows = len(data)
            logger.info(
                "Transformation complete: %s -> %s rows",
                original_rows,
                final_rows,
            )

            return Result(status="success", response=data)

        except Exception as e:
            logger.error(f"JSON transformation failed: {e!s}")
            return Result(status="error", response=None)

    def _get_input_data(self) -> pl.DataFrame | None:  # noqa: PLR0911
        """Получение входных данных из зависимостей или конфигурации."""
        temporal_data = getattr(self, "_temporal_input_data", None)
        if temporal_data:
            logger.debug(f"Found temporal input data: {type(temporal_data)}")

            if isinstance(temporal_data, pl.DataFrame):
                return temporal_data
            if isinstance(temporal_data, dict) and "records" in temporal_data:
                return pl.DataFrame(temporal_data["records"])
            if isinstance(temporal_data, dict):
                return pl.DataFrame([temporal_data])

        if hasattr(self.config, "input_data") and self.config.input_data:
            input_data = self.config.input_data

            if isinstance(input_data, pl.DataFrame):
                return input_data
            if isinstance(input_data, dict):
                if "records" in input_data:
                    return pl.DataFrame(input_data["records"])
                if "dependencies" in input_data:
                    deps = input_data["dependencies"]
                    if deps:
                        first_dep = next(iter(deps.values()))
                        if (
                            hasattr(first_dep, "metadata")
                            and "records" in first_dep.metadata
                        ):
                            return pl.DataFrame(first_dep.metadata["records"])
                return pl.DataFrame([input_data])

        logger.warning("No input data found")
        return None

    def _add_processing_metadata(self, data: pl.DataFrame) -> pl.DataFrame:
        """Добавление метаданных обработки."""
        try:
            metadata_columns = {
                "_processed_at": datetime.now(tz=UTC).isoformat(),
                "_pipeline_name": getattr(
                    self.config, "pipeline_name", "unknown"
                ),
                "_stage_name": getattr(self.config, "stage_name", "unknown"),
                "_run_id": getattr(self.config, "run_id", "unknown"),
            }

            for col_name, value in metadata_columns.items():
                data = data.with_columns(pl.lit(value).alias(col_name))

            logger.debug("Added processing metadata columns")
            return data

        except Exception as e:
            logger.warning(f"Failed to add metadata: {e}")
            return data

    async def _apply_json_normalization(
        self, data: pl.DataFrame
    ) -> pl.DataFrame:
        """Нормализация JSON колонок."""
        if not self.config.json_normalization:
            return data
        # Заглушка - в данном примере не используется
        return data

    async def _apply_column_operations(
        self, data: pl.DataFrame
    ) -> pl.DataFrame:
        """Применение операций над колонками."""
        if not self.config.column_operations:
            return data
        # Заглушка - можно расширить
        return data

    async def _apply_filtering(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение фильтров."""
        if not self.config.filter_conditions:
            return data
        # Заглушка - можно расширить
        return data

    async def _apply_join_operations(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение соединений."""
        if not self.config.join_config:
            return data
        # Заглушка - можно расширить
        return data

    async def _apply_aggregation(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение агрегации."""
        if not self.config.aggregation:
            return data
        # Заглушка - можно расширить
        return data

    async def _apply_deduplication(self, data: pl.DataFrame) -> pl.DataFrame:
        """Удаление дубликатов."""
        if not self.config.deduplicate:
            return data

        try:
            if self.config.deduplicate_columns:
                result = data.unique(subset=self.config.deduplicate_columns)
            else:
                result = data.unique()

            removed = len(data) - len(result)
            logger.debug(f"Removed {removed} duplicate rows")
            return result

        except Exception as e:
            logger.warning(f"Failed to deduplicate: {e}")
            return data

    async def _apply_null_handling(self, data: pl.DataFrame) -> pl.DataFrame:
        """Обработка пропущенных значений."""
        return data  # Заглушка

    async def _apply_sorting(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение сортировки."""
        if not self.config.sort_columns:
            return data

        try:
            result = data.sort(
                self.config.sort_columns,
                descending=self.config.sort_descending,
            )
            logger.debug(f"Applied sorting by: {self.config.sort_columns}")
            return result
        except Exception as e:
            logger.warning(f"Failed to apply sorting: {e}")
            return data

    async def _apply_sampling(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение семплирования."""
        if self.config.sample_size:
            try:
                sample_size = min(self.config.sample_size, len(data))
                result = data.sample(n=sample_size)
                logger.debug(f"Applied sampling: {sample_size} rows")
                return result
            except Exception as e:
                logger.warning(f"Failed to apply size sampling: {e}")

        elif self.config.sample_fraction:
            try:
                result = data.sample(fraction=self.config.sample_fraction)
                logger.debug(
                    f"Applied fraction sampling: {self.config.sample_fraction}"
                )
                return result
            except Exception as e:
                logger.warning(f"Failed to apply fraction sampling: {e}")

        return data

    @classmethod
    def process_info(cls) -> Info:
        return Info(
            name="JSONTransform",
            version="1.0.0",
            description="Мощный компонент трансформации\
             данных с поддержкой JSON операций",
            type_class=cls,
            type_module="transform",
            config_class=JSONTransformConfig,
        )
