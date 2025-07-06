"""
PostgreSQL Load Plugin - Загрузка данных в PostgreSQL
Поддерживает различные стратегии загрузки, батчинг, upsert операции
"""

import asyncio
import hashlib
import logging
from datetime import UTC, datetime

import polars as pl
from asyncpg import Connection

from core.component import BaseProcessClass, Info, Result
from postgres_load.config import PostgreSQLLoadConfig
from postgres_load.const import (
    CREATE_INDEX_COMMAND,
    CREATE_TABLE,
    INSERT_BATCH_COMMAND,
    INSERT_UPSERT_COMMAND,
    SELECT_EXISTS_TABLE,
    TRUNCATE_TABLE,
)

logger = logging.getLogger(__name__)


class PostgreSQLLoad(BaseProcessClass):
    """
    PostgreSQL Load компонент для загрузки данных в PostgreSQL

    Поддерживаемые функции:
    - Различные стратегии загрузки (append, replace, upsert)
    - Батчевая загрузка с контролем размера
    - Параллельная обработка батчей
    - Автоматическое создание таблиц и индексов
    - Upsert операции с поддержкой конфликтов
    - Валидация данных
    - Добавление метаданных загрузки
    """

    config: PostgreSQLLoadConfig
    table_alias: str | None

    def __init__(self) -> None:
        super().__init__()
        self._connection_pool = None
        self._load_id = None
        self.table_alias = None

    async def process(self) -> Result | None:
        """Основной метод обработки"""
        try:
            self.table_alias = (
                f"{self.config.target_schema}.{self.config.target_table}"
            )
            logger.info(
                f"Starting PostgreSQL load to table: {self.table_alias}"
            )
            # Получаем входные данные
            input_data = self._get_input_data()
            if input_data is None or len(input_data) == 0:
                logger.warning("No input data to load")
                return Result(
                    status="success",
                    response={
                        "records_loaded": 0,
                        "message": "No data to load",
                    },
                )

            original_count = len(input_data)
            logger.info(f"Loading {original_count} rows to PostgreSQL")

            # Генерируем уникальный ID загрузки
            self._load_id = self._generate_load_id()

            # Подготавливаем данные
            prepared_data = await self._prepare_data(input_data)

            # Валидируем данные
            if self.config.validate_data:
                prepared_data = await self._validate_data(prepared_data)

            # Устанавливаем соединение
            await self._connect()

            # Подготавливаем таблицу
            await self._prepare_target_table(prepared_data)

            # Загружаем данные
            loaded_count = await self._load_data(prepared_data)

            # Создаем индексы если нужно
            if self.config.create_indexes:
                await self._create_indexes()

            logger.info(
                "Successfully loaded %s rows to %s",
                loaded_count,
                self.table_alias,
            )

            return Result(
                status="success",
                response={
                    "records_loaded": loaded_count,
                    "table": self.table_alias,
                    "load_id": self._load_id,
                    "original_count": original_count,
                },
            )

        except Exception as e:
            logger.error(f"PostgreSQL load failed: {e!s}")
            return Result(status="error", response=None)
        finally:
            await self._disconnect()

    def _get_input_data(self) -> pl.DataFrame | None:
        """Получение входных данных"""
        if hasattr(self.config, "input_data") and self.config.input_data:
            if isinstance(self.config.input_data, dict):
                if "records" in self.config.input_data:
                    return pl.DataFrame(self.config.input_data["records"])
                if "dependencies" in self.config.input_data:
                    # Берем данные из первой зависимости
                    deps = self.config.input_data["dependencies"]
                    if deps:
                        first_dep = next(iter(deps.values()))
                        if (
                            hasattr(first_dep, "metadata")
                            and "records" in first_dep.metadata
                        ):
                            return pl.DataFrame(first_dep.metadata["records"])
            elif isinstance(self.config.input_data, pl.DataFrame):
                return self.config.input_data

        logger.warning("No valid input data found")
        return None

    async def _prepare_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """Подготовка данных для загрузки"""

        # Переименование колонок
        if self.config.column_mapping:
            for source_col, target_col in self.config.column_mapping.items():
                if source_col in data.columns:
                    data = data.rename({source_col: target_col})

        # Добавление метаданных загрузки
        if self.config.add_load_metadata:
            data = await self._add_load_metadata(data)

        logger.debug(
            f"Prepared data: {len(data)} rows, {len(data.columns)} columns"
        )
        return data

    async def _add_load_metadata(self, data: pl.DataFrame) -> pl.DataFrame:
        """Добавление метаданных загрузки"""
        try:
            metadata_additions = []

            if "loaded_at" in self.config.metadata_columns:
                metadata_additions.append(
                    pl.lit(datetime.now(tz=UTC)).alias("loaded_at")
                )

            if "load_id" in self.config.metadata_columns:
                metadata_additions.append(
                    pl.lit(self._load_id).alias("load_id")
                )

            if "source_file" in self.config.metadata_columns:
                source_info = getattr(
                    self.config, "source_file", "etl_pipeline"
                )
                metadata_additions.append(
                    pl.lit(source_info).alias("source_file")
                )

            if metadata_additions:
                data = data.with_columns(metadata_additions)
                logger.debug("Added load metadata columns")

        except Exception as e:
            logger.warning(f"Failed to add metadata: {e}")

        return data

    async def _validate_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """Валидация данных"""
        try:
            original_count = len(data)

            data = data.filter(~pl.all_horizontal(pl.all().is_null()))

            validated_count = len(data)
            if validated_count < original_count:
                logger.info(
                    "Removed %s invalid rows during validation",
                    original_count - validated_count,
                )

            return data

        except Exception as e:
            logger.warning(f"Data validation failed: {e}")
            if self.config.max_errors == 0:
                raise
            return data

    async def _connect(self) -> None:
        """Установка соединения с PostgreSQL"""
        try:
            import asyncpg  # noqa:PLC0415
        except ImportError as ex:
            msg = "asyncpg library is required for PostgreSQL support.\
            Install it with: pip install asyncpg"
            raise ImportError(msg) from ex

        try:
            # Создаем пул соединений
            self._connection_pool = await asyncpg.create_pool(
                self.config.connection_config.uri,
                min_size=1,
                max_size=self.config.connection_config.pool_size,
                command_timeout=self.config.connection_config.connection_timeout,
                server_settings={
                    "jit": "off"  # Отключаем JIT для стабильности
                },
            )

            logger.debug("PostgreSQL connection pool created")

        except Exception as ex:
            msg = f"Failed to connect to PostgreSQL: {ex}"
            raise Exception(msg) from ex

    async def _disconnect(self) -> None:
        """Закрытие соединения"""
        if self._connection_pool:
            await self._connection_pool.close()
            logger.debug("PostgreSQL connection pool closed")

    async def _prepare_target_table(self, data: pl.DataFrame) -> None:
        """Подготовка целевой таблицы"""
        async with self._connection_pool.acquire() as conn:
            # Проверяем существование таблицы
            table_exists = await self._table_exists(conn)

            if not table_exists and self.config.create_table:
                await self._create_table(conn, data)
            elif not table_exists:
                msg = f"Table {self.table_alias}\
                does not exist and create_table=False"
                raise Exception(msg)

            # Обрабатываем стратегию if_exists
            if table_exists and self.config.if_exists == "replace":
                await self._truncate_table(conn)
            elif table_exists and self.config.if_exists == "fail":
                msg = f"Table {self.table_alias}\
                already exists and if_exists='fail'"
                raise Exception(msg)

    async def _table_exists(self, conn: Connection) -> bool:
        """Проверка существования таблицы"""
        return await conn.fetchval(
            SELECT_EXISTS_TABLE,
            self.config.target_schema,
            self.config.target_table,
        )

    async def _create_table(
        self, conn: Connection, data: pl.DataFrame
    ) -> None:
        logger.info(
            "Creating table %s",
            self.table_alias,
        )

        column_definitions = self._generate_column_definitions(data)
        metadata_definitions = self._generate_metadata_columns(data)
        column_definitions.extend(metadata_definitions)

        table_options = self._generate_table_options()

        create_query = CREATE_TABLE.format(
            table=self.table_alias,
            column_definitions=", ".join(column_definitions),
            table_options=table_options,
        )

        await conn.execute(create_query)
        logger.info("Table %s created successfully", self.table_alias)

    def _generate_column_definitions(self, data: pl.DataFrame) -> list[str]:
        column_definitions = []

        for column in data.columns:
            dtype = data[column].dtype
            pg_type = self._map_polars_type_to_postgres(dtype)
            column_definitions.append(f'"{column}" {pg_type}')

        return column_definitions

    def _generate_metadata_columns(self, data: pl.DataFrame) -> list[str]:
        if not (
            self.config.add_load_metadata and self.config.metadata_columns
        ):
            return []

        return [
            f'"{col_name}" {col_type}'
            for col_name, col_type in self.config.metadata_columns.items()
            if col_name not in data.columns
        ]

    def _generate_table_options(self) -> str:
        if (
            self.config.table_options
            and "with_options" in self.config.table_options
        ):
            return f"WITH ({self.config.table_options['with_options']})"
        return ""

    def _map_polars_type_to_postgres(self, dtype: pl.DataType) -> str:  # noqa:PLR0911
        match dtype:
            case pl.Int64:
                return "BIGINT"
            case pl.Int32:
                return "INTEGER"
            case pl.Float64:
                return "DOUBLE PRECISION"
            case pl.Float32:
                return "REAL"
            case pl.Boolean:
                return "BOOLEAN"
            case pl.Date:
                return "DATE"
            case pl.Datetime:
                return "TIMESTAMP"
            case _:
                return "TEXT"

    async def _truncate_table(self, conn: Connection) -> None:
        """Очистка таблицы"""
        query = TRUNCATE_TABLE.format(
            table=self.table_alias,
        )
        await conn.execute(query)
        logger.info(
            "Table %s truncated",
            self.table_alias,
        )

    async def _load_data(self, data: pl.DataFrame) -> int:
        """Загрузка данных в таблицу"""
        total_loaded = 0

        if self.config.if_exists == "upsert":
            total_loaded = await self._upsert_data(data)
        else:
            total_loaded = await self._insert_data(data)

        return total_loaded

    async def _insert_data(self, data: pl.DataFrame) -> int:
        """Вставка данных (INSERT)"""
        total_loaded = 0

        # Разбиваем на батчи
        batches = self._create_batches(data)

        if self.config.parallel_batches > 1:
            # Параллельная обработка батчей
            semaphore = asyncio.Semaphore(self.config.parallel_batches)
            tasks = [
                self._process_batch_with_semaphore(semaphore, batch, batch_idx)
                for batch_idx, batch in enumerate(batches)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, int):
                    total_loaded += result
                elif isinstance(result, Exception):
                    logger.error(f"Batch processing failed: {result}")
                    if self.config.max_errors == 0:
                        raise result
        else:
            # Последовательная обработка
            for batch_idx, batch in enumerate(batches):
                try:
                    loaded = await self._insert_batch(batch, batch_idx)
                    total_loaded += loaded
                except Exception as e:
                    logger.error(f"Batch {batch_idx} failed: {e}")
                    if self.config.max_errors == 0:
                        raise

        return total_loaded

    async def _upsert_data(self, data: pl.DataFrame) -> int:
        """Upsert данных (INSERT ... ON CONFLICT)"""
        if not self.config.upsert_config:
            msg = "upsert_config is required for upsert operation"
            raise ValueError(msg)

        total_loaded = 0
        batches = self._create_batches(data)

        for batch_idx, batch in enumerate(batches):
            try:
                loaded = await self._upsert_batch(batch, batch_idx)
                total_loaded += loaded
            except Exception as e:
                logger.error(f"Upsert batch {batch_idx} failed: {e}")
                if self.config.max_errors == 0:
                    raise

        return total_loaded

    def _create_batches(self, data: pl.DataFrame) -> list[pl.DataFrame]:
        """Создание батчей данных"""
        batches = []
        total_rows = len(data)

        for i in range(0, total_rows, self.config.batch_size):
            batch = data.slice(i, self.config.batch_size)
            batches.append(batch)

        logger.debug(
            f"Created {len(batches)}\
            batches of max size {self.config.batch_size}"
        )
        return batches

    async def _process_batch_with_semaphore(
        self, semaphore: asyncio.Semaphore, batch: pl.DataFrame, batch_idx: int
    ) -> int:
        """Обработка батча с семафором для контроля параллельности"""
        async with semaphore:
            return await self._insert_batch(batch, batch_idx)

    async def _insert_batch(self, batch: pl.DataFrame, batch_idx: int) -> int:
        """Вставка одного батча"""
        async with self._connection_pool.acquire() as conn:
            # Подготавливаем данные для вставки
            columns = batch.columns
            values = []

            for row in batch.iter_rows():
                values.append(row)

            if not values:
                return 0

            # Создаем INSERT запрос
            columns_str = ", ".join(f'"{col}"' for col in columns)
            placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

            insert_query = INSERT_BATCH_COMMAND.format(
                table=self.table_alias,
                columns_str=columns_str,
                placeholders=placeholders,
            )

            # Выполняем вставку
            try:
                await conn.executemany(insert_query, values)
                logger.debug(f"Inserted batch {batch_idx}: {len(values)} rows")
                return len(values)

            except Exception as e:
                logger.error(f"Failed to insert batch {batch_idx}: {e}")
                raise

    async def _upsert_batch(self, batch: pl.DataFrame, batch_idx: int) -> int:
        """Upsert одного батча"""
        async with self._connection_pool.acquire() as conn:
            columns = batch.columns
            values = list(batch.iter_rows())

            if not values:
                return 0

            upsert_config = self.config.upsert_config

            # Создаем UPSERT запрос
            columns_str = ", ".join(f'"{col}"' for col in columns)
            placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

            # Определяем колонки для обновления
            update_columns = upsert_config.update_columns or [
                col
                for col in columns
                if col not in upsert_config.conflict_columns
            ]

            update_set = ", ".join(
                f'"{col}" = EXCLUDED."{col}"' for col in update_columns
            )

            conflict_columns_str = ", ".join(
                f'"{col}"' for col in upsert_config.conflict_columns
            )

            upsert_query = INSERT_UPSERT_COMMAND.format(
                table=self.table_alias,
                columns_str=columns_str,
                placeholders=placeholders,
                conflict_columns_str=conflict_columns_str,
                update_set=update_set,
            )

            if upsert_config.where_condition:
                upsert_query += f" WHERE {upsert_config.where_condition}"

            try:
                await conn.executemany(upsert_query, values)
                logger.debug(f"Upserted batch {batch_idx}: {len(values)} rows")
                return len(values)

            except Exception as e:
                logger.error(f"Failed to upsert batch {batch_idx}: {e}")
                raise

    async def _create_indexes(self) -> None:
        """Создание индексов"""
        if not self.config.create_indexes:
            return

        async with self._connection_pool.acquire() as conn:
            for index_config in self.config.create_indexes:
                try:
                    columns = index_config["columns"]
                    index_name = index_config.get(
                        "name",
                        f"idx_{self.config.target_table}_{'_'.join(columns)}",
                    )
                    unique = index_config.get("unique", False)

                    columns_str = ", ".join(f'"{col}"' for col in columns)
                    unique_str = "UNIQUE " if unique else ""

                    create_index_query = CREATE_INDEX_COMMAND.format(
                        unique_str=unique_str,
                        index_name=index_name,
                        table=self.table_alias,
                        columns_str=columns_str,
                    )

                    await conn.execute(create_index_query)
                    logger.info(f"Created index: {index_name}")

                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")

    def _generate_load_id(self) -> str:
        """Генерация уникального ID загрузки"""
        timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")

        # Добавляем хеш от конфигурации для уникальности
        config_str = f"{self.config.target_table}_{timestamp}"
        hash_suffix = hashlib.sha256(config_str.encode()).hexdigest()[:8]

        return f"load_{timestamp}_{hash_suffix}"

    @property
    def info(self) -> Info:
        return Info(
            name="PostgreSQLLoad",
            version="1.0.0",
            description="Загрузка данных в PostgreSQL\
            с поддержкой upsert, батчинга и индексов",
            type_class=self.__class__,
            type_module="load",
            config_class=PostgreSQLLoadConfig,
        )
