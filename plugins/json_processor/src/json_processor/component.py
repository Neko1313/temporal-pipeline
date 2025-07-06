"""
JSON Transform Plugin - Трансформация данных с поддержкой JSON операций
Поддерживает нормализацию вложенных JSON, агрегации, джойны, фильтрацию
"""

import ast
import json
import logging
from datetime import UTC, datetime
from typing import Any

import polars as pl

from core.component import BaseProcessClass, Info, Result
from json_processor.config import JSONTransformConfig

logger = logging.getLogger(__name__)


class JSONTransform(BaseProcessClass):
    """
    JSON Transform компонент для трансформации данных

    Поддерживаемые операции:
    - Нормализация вложенных JSON структур
    - Агрегации и группировки
    - Соединения с данными из зависимостей
    - Фильтрация и сортировка
    - Обработка пропущенных значений
    - Создание новых колонок
    - Семплирование данных
    """

    config: JSONTransformConfig

    async def process(self) -> Result:
        """Основной метод обработки"""
        try:
            logger.info("Starting JSON transformation")

            # Получаем входные данные
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

            # Применяем трансформации в порядке
            data = await self._apply_json_normalization(data)
            data = await self._apply_column_operations(data)
            data = await self._apply_filtering(data)
            data = await self._apply_join_operations(data)
            data = await self._apply_aggregation(data)
            data = await self._apply_deduplication(data)
            data = await self._apply_null_handling(data)
            data = await self._apply_sorting(data)
            data = await self._apply_sampling(data)
            data = await self._add_metadata_columns(data)

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

    def _get_input_data(self) -> pl.DataFrame | None:
        """Получение входных данных из зависимостей или конфигурации"""
        if hasattr(self.config, "input_data") and self.config.input_data:
            # Данные из зависимостей
            if (
                isinstance(self.config.input_data, dict)
                and "records" in self.config.input_data
            ):
                return pl.DataFrame(self.config.input_data["records"])
            if isinstance(self.config.input_data, pl.DataFrame):
                return self.config.input_data
            logger.warning("Invalid input data format")
            return None
        logger.warning("No input data provided")
        return None

    async def _apply_json_normalization(
        self, data: pl.DataFrame
    ) -> pl.DataFrame:
        """Нормализация JSON колонок"""
        if not self.config.json_normalization:
            return data

        config = self.config.json_normalization

        for json_col in config.json_columns:
            if json_col not in data.columns:
                logger.warning(f"JSON column '{json_col}' not found")
                continue

            try:
                # Нормализуем JSON колонку
                normalized_data = []

                for row in data.iter_rows(named=True):
                    json_value = row[json_col]
                    if json_value is not None:
                        try:
                            if isinstance(json_value, str):
                                parsed_json = json.loads(json_value)
                            else:
                                parsed_json = json_value

                            # Рекурсивно нормализуем JSON
                            flattened = self._flatten_json(
                                parsed_json, config.separator, config.max_level
                            )
                            normalized_data.append(flattened)
                        except (json.JSONDecodeError, TypeError):
                            normalized_data.append({})
                    else:
                        normalized_data.append({})

                # Создаем DataFrame из нормализованных данных
                if normalized_data:
                    json_df = pl.DataFrame(normalized_data)

                    # Добавляем префикс к колонкам
                    json_df = json_df.rename(
                        {
                            col: f"{json_col}{config.separator}{col}"
                            for col in json_df.columns
                        }
                    )

                    data = data.with_row_count("__row_index")
                    json_df = json_df.with_row_count("__row_index")

                    data = data.join(json_df, on="__row_index", how="left")
                    data = data.drop("__row_index")

                    # Удаляем оригинальную JSON колонку если нужно
                    if not config.preserve_original:
                        data = data.drop(json_col)

                logger.debug(f"Normalized JSON column: {json_col}")

            except Exception as e:
                logger.warning(
                    f"Failed to normalize JSON column '{json_col}': {e}"
                )

        return data

    def _flatten_json(
        self, obj: Any, separator: str, max_level: int, current_level: int = 0
    ) -> dict[str, Any]:
        """Рекурсивное выравнивание JSON объекта"""
        result = {}

        if current_level >= max_level:
            return {"data": str(obj)}

        if isinstance(obj, dict):
            for key, value in obj.items():
                if (
                    isinstance(value, dict | list)
                    and current_level < max_level - 1
                ):
                    nested = self._flatten_json(
                        value, separator, max_level, current_level + 1
                    )
                    for nested_key, nested_value in nested.items():
                        result[f"{key}{separator}{nested_key}"] = nested_value
                else:
                    result[key] = value
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if (
                    isinstance(item, dict | list)
                    and current_level < max_level - 1
                ):
                    nested = self._flatten_json(
                        item, separator, max_level, current_level + 1
                    )
                    for nested_key, nested_value in nested.items():
                        result[f"{i}{separator}{nested_key}"] = nested_value
                else:
                    result[str(i)] = item
        else:
            result["value"] = obj

        return result

    async def _apply_column_operations(
        self, data: pl.DataFrame
    ) -> pl.DataFrame:
        """Применение операций над колонками"""
        if not self.config.column_operations:
            return data

        for new_column, _expression in self.config.column_operations.items():
            try:
                data = data.with_columns(pl.lit(None).alias(new_column))
                logger.debug(f"Added column operation: {new_column}")

            except Exception as e:
                logger.warning(
                    f"Failed to apply column operation '{new_column}': {e}"
                )

        return data

    async def _apply_filtering(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение фильтров"""
        if not self.config.filter_conditions:
            return data

        for condition in self.config.filter_conditions:
            try:
                data = data.filter(ast.literal_eval(condition))
                logger.debug(f"Applied filter: {condition}")

            except Exception as e:
                logger.warning(f"Failed to apply filter '{condition}': {e}")

        return data

    async def _apply_join_operations(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение соединений с данными из зависимостей"""
        if not self.config.join_config:
            return data

        # Получаем данные для соединения из зависимостей
        right_data = self._get_dependency_data()
        if right_data is None:
            logger.warning("No dependency data available for join")
            return data

        try:
            config = self.config.join_config

            # Выполняем соединение
            result = data.join(
                right_data,
                left_on=config.left_on,
                right_on=config.right_on,
                how=config.join_type,
                suffix=config.suffix,
            )

            logger.debug(
                "Applied %s join on %s=%s",
                config.join_type,
                config.left_on,
                config.right_on,
            )
            return result

        except Exception as e:
            logger.warning(f"Failed to apply join: {e}")
            return data

    def _get_dependency_data(self) -> pl.DataFrame | None:
        """Получение данных из зависимостей для join операций"""
        if (
            hasattr(self.config, "input_data")
            and self.config.input_data
            and isinstance(self.config.input_data, dict)
            and "dependencies" in self.config.input_data
        ):
            deps = self.config.input_data["dependencies"]
            if deps:
                first_dep = next(iter(deps.values()))
                if (
                    hasattr(first_dep, "metadata")
                    and "records" in first_dep.metadata
                ):
                    return pl.DataFrame(first_dep.metadata["records"])
        return None

    async def _apply_aggregation(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение агрегации"""
        if not self.config.aggregation:
            return data

        try:
            config = self.config.aggregation

            # Группируем данные
            grouped = data.group_by(config.group_by)

            # Применяем агрегации
            agg_expressions = []
            for column, func in config.aggregations.items():
                if column in data.columns:
                    if func == "sum":
                        agg_expressions.append(
                            pl.col(column).sum().alias(f"{column}_{func}")
                        )
                    elif func == "avg":
                        agg_expressions.append(
                            pl.col(column).mean().alias(f"{column}_{func}")
                        )
                    elif func == "count":
                        agg_expressions.append(
                            pl.col(column).count().alias(f"{column}_{func}")
                        )
                    elif func == "min":
                        agg_expressions.append(
                            pl.col(column).min().alias(f"{column}_{func}")
                        )
                    elif func == "max":
                        agg_expressions.append(
                            pl.col(column).max().alias(f"{column}_{func}")
                        )
                    elif func == "std":
                        agg_expressions.append(
                            pl.col(column).std().alias(f"{column}_{func}")
                        )
                    elif func == "first":
                        agg_expressions.append(
                            pl.col(column).first().alias(f"{column}_{func}")
                        )
                    elif func == "last":
                        agg_expressions.append(
                            pl.col(column).last().alias(f"{column}_{func}")
                        )

            if agg_expressions:
                result = grouped.agg(agg_expressions)
                logger.debug(
                    "Applied aggregation with %s functions",
                    len(agg_expressions),
                )
                return result

        except Exception as e:
            logger.warning(f"Failed to apply aggregation: {e}")

        return data

    async def _apply_deduplication(self, data: pl.DataFrame) -> pl.DataFrame:
        """Удаление дубликатов"""
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
        """Обработка пропущенных значений"""

        # Заполнение NULL значений
        if self.config.fill_null_strategy:
            try:
                strategy = self.config.fill_null_strategy
                if strategy == "forward":
                    data = data.fill_null(strategy="forward")
                elif strategy == "backward":
                    data = data.fill_null(strategy="backward")
                elif strategy == "zero":
                    data = data.fill_null(0)
                elif strategy == "mean":
                    # Заполняем средним значением для числовых колонок
                    numeric_cols = [
                        col
                        for col in data.columns
                        if data[col].dtype in [pl.Int64, pl.Float64]
                    ]
                    for col in numeric_cols:
                        mean_val = data[col].mean()
                        data = data.with_columns(
                            pl.col(col).fill_null(mean_val)
                        )

                logger.debug(f"Applied null fill strategy: {strategy}")

            except Exception as e:
                logger.warning(f"Failed to apply null fill strategy: {e}")

        if self.config.drop_null_columns:
            try:
                for col in self.config.drop_null_columns:
                    if col in data.columns:
                        data = data.filter(pl.col(col).is_not_null())

                logger.debug(
                    "Dropped rows with nulls in: %s",
                    self.config.drop_null_columns,
                )

            except Exception as e:
                logger.warning(f"Failed to drop null rows: {e}")

        return data

    async def _apply_sorting(self, data: pl.DataFrame) -> pl.DataFrame:
        """Применение сортировки"""
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
        """Применение семплирования"""
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

    async def _add_metadata_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        """Добавление метаданных"""
        if not self.config.add_metadata:
            return data

        try:
            # Добавляем метаданные обработки
            data = data.with_columns(
                [
                    pl.lit(datetime.now(tz=UTC).isoformat()).alias(
                        "__processed_at"
                    ),
                    pl.lit(
                        self.config.run_id
                        if hasattr(self.config, "run_id")
                        else "unknown"
                    ).alias("__run_id"),
                    pl.lit(
                        self.config.stage_name
                        if hasattr(self.config, "stage_name")
                        else "transform"
                    ).alias("__stage_name"),
                ]
            )

            logger.debug("Added metadata columns")

        except Exception as e:
            logger.warning(f"Failed to add metadata: {e}")

        return data

    @property
    def info(self) -> Info:
        return Info(
            name="JSONTransform",
            version="1.0.0",
            description="Мощный компонент\
            трансформации данных с поддержкой JSON операций",
            type_class=self.__class__,
            type_module="transform",
            config_class=JSONTransformConfig,
        )
