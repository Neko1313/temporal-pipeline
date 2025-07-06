"""
PostgreSQL Load Plugin - Загрузка данных в PostgreSQL
Поддерживает различные стратегии загрузки, батчинг, upsert операции
"""

from typing import Optional, List
import polars as pl
import logging
import asyncio
import hashlib
from datetime import datetime

from core.component import BaseProcessClass, Result, Info
from postgres_load.config import PostgreSQLLoadConfig

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

    def __init__(self):
        super().__init__()
        self._connection_pool = None
        self._load_id = None

    async def process(self) -> Result | None:
        """Основной метод обработки"""
        try:
            logger.info(
                f"Starting PostgreSQL load to table: {self.config.target_schema}.{self.config.target_table}"
            )

            # Получаем входные данные
            input_data = self._get_input_data()
            if input_data is None or len(input_data) == 0:
                logger.warning("No input data to load")
                return Result(
                    status="success",
                    response={"records_loaded": 0, "message": "No data to load"},
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
                f"Successfully loaded {loaded_count} rows to {self.config.target_schema}.{self.config.target_table}"
            )

            return Result(
                status="success",
                response={
                    "records_loaded": loaded_count,
                    "table": f"{self.config.target_schema}.{self.config.target_table}",
                    "load_id": self._load_id,
                    "original_count": original_count,
                },
            )

        except Exception as e:
            logger.error(f"PostgreSQL load failed: {str(e)}")
            return Result(status="error", response=None)
        finally:
            await self._disconnect()

    def _get_input_data(self) -> Optional[pl.DataFrame]:
        """Получение входных данных"""
        if hasattr(self.config, "input_data") and self.config.input_data:
            if isinstance(self.config.input_data, dict):
                if "records" in self.config.input_data:
                    return pl.DataFrame(self.config.input_data["records"])
                elif "dependencies" in self.config.input_data:
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

        logger.debug(f"Prepared data: {len(data)} rows, {len(data.columns)} columns")
        return data

    async def _add_load_metadata(self, data: pl.DataFrame) -> pl.DataFrame:
        """Добавление метаданных загрузки"""
        try:
            metadata_additions = []

            if "loaded_at" in self.config.metadata_columns:
                metadata_additions.append(pl.lit(datetime.now()).alias("loaded_at"))

            if "load_id" in self.config.metadata_columns:
                metadata_additions.append(pl.lit(self._load_id).alias("load_id"))

            if "source_file" in self.config.metadata_columns:
                source_info = getattr(self.config, "source_file", "etl_pipeline")
                metadata_additions.append(pl.lit(source_info).alias("source_file"))

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

            # Удаляем строки со всеми NULL значениями
            data = data.filter(~pl.all_horizontal(pl.all().is_null()))

            # Проверяем на пустые обязательные колонки
            # (здесь можно добавить специфичные проверки)

            validated_count = len(data)
            if validated_count < original_count:
                logger.info(
                    f"Removed {original_count - validated_count} invalid rows during validation"
                )

            return data

        except Exception as e:
            logger.warning(f"Data validation failed: {e}")
            if self.config.max_errors == 0:
                raise
            return data

    async def _connect(self):
        """Установка соединения с PostgreSQL"""
        try:
            import asyncpg
        except ImportError:
            raise ImportError(
                "asyncpg library is required for PostgreSQL support. Install it with: pip install asyncpg"
            )

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

        except Exception as e:
            raise Exception(f"Failed to connect to PostgreSQL: {e}")

    async def _disconnect(self):
        """Закрытие соединения"""
        if self._connection_pool:
            await self._connection_pool.close()
            logger.debug("PostgreSQL connection pool closed")

    async def _prepare_target_table(self, data: pl.DataFrame):
        """Подготовка целевой таблицы"""
        async with self._connection_pool.acquire() as conn:
            # Проверяем существование таблицы
            table_exists = await self._table_exists(conn)

            if not table_exists and self.config.create_table:
                await self._create_table(conn, data)
            elif not table_exists:
                raise Exception(
                    f"Table {self.config.target_schema}.{self.config.target_table} does not exist and create_table=False"
                )

            # Обрабатываем стратегию if_exists
            if table_exists and self.config.if_exists == "replace":
                await self._truncate_table(conn)
            elif table_exists and self.config.if_exists == "fail":
                raise Exception(
                    f"Table {self.config.target_schema}.{self.config.target_table} already exists and if_exists='fail'"
                )

    async def _table_exists(self, conn) -> bool:
        """Проверка существования таблицы"""
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = $1 AND table_name = $2
        )
        """
        result = await conn.fetchval(
            query, self.config.target_schema, self.config.target_table
        )
        return result

    async def _create_table(self, conn, data: pl.DataFrame):
        """Создание таблицы"""
        logger.info(
            f"Creating table {self.config.target_schema}.{self.config.target_table}"
        )

        # Определяем типы колонок на основе данных
        column_definitions = []

        for column in data.columns:
            dtype = data[column].dtype

            if dtype == pl.Int64:
                pg_type = "BIGINT"
            elif dtype == pl.Int32:
                pg_type = "INTEGER"
            elif dtype == pl.Float64:
                pg_type = "DOUBLE PRECISION"
            elif dtype == pl.Float32:
                pg_type = "REAL"
            elif dtype == pl.Boolean:
                pg_type = "BOOLEAN"
            elif dtype == pl.Date:
                pg_type = "DATE"
            elif dtype == pl.Datetime:
                pg_type = "TIMESTAMP"
            else:
                pg_type = "TEXT"

            column_definitions.append(f'"{column}" {pg_type}')

        # Добавляем метаданные колонки
        if self.config.add_load_metadata and self.config.metadata_columns:
            for col_name, col_type in self.config.metadata_columns.items():
                if col_name not in data.columns:
                    column_definitions.append(f'"{col_name}" {col_type}')

        # Дополнительные опции таблицы
        table_options = ""
        if self.config.table_options:
            if "with_options" in self.config.table_options:
                table_options = f"WITH ({self.config.table_options['with_options']})"

        create_query = f"""
        CREATE TABLE "{self.config.target_schema}"."{self.config.target_table}" (
            {", ".join(column_definitions)}
        ) {table_options}
        """

        await conn.execute(create_query)
        logger.info(
            f"Table {self.config.target_schema}.{self.config.target_table} created successfully"
        )

    async def _truncate_table(self, conn):
        """Очистка таблицы"""
        query = (
            f'TRUNCATE TABLE "{self.config.target_schema}"."{self.config.target_table}"'
        )
        await conn.execute(query)
        logger.info(
            f"Table {self.config.target_schema}.{self.config.target_table} truncated"
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
            raise ValueError("upsert_config is required for upsert operation")

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

    def _create_batches(self, data: pl.DataFrame) -> List[pl.DataFrame]:
        """Создание батчей данных"""
        batches = []
        total_rows = len(data)

        for i in range(0, total_rows, self.config.batch_size):
            batch = data.slice(i, self.config.batch_size)
            batches.append(batch)

        logger.debug(
            f"Created {len(batches)} batches of max size {self.config.batch_size}"
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

            insert_query = f"""
            INSERT INTO "{self.config.target_schema}"."{self.config.target_table}" 
            ({columns_str}) VALUES ({placeholders})
            """

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
            values = [row for row in batch.iter_rows()]

            if not values:
                return 0

            upsert_config = self.config.upsert_config

            # Создаем UPSERT запрос
            columns_str = ", ".join(f'"{col}"' for col in columns)
            placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))

            # Определяем колонки для обновления
            update_columns = upsert_config.update_columns or [
                col for col in columns if col not in upsert_config.conflict_columns
            ]

            update_set = ", ".join(
                f'"{col}" = EXCLUDED."{col}"' for col in update_columns
            )

            conflict_columns_str = ", ".join(
                f'"{col}"' for col in upsert_config.conflict_columns
            )

            upsert_query = f"""
            INSERT INTO "{self.config.target_schema}"."{self.config.target_table}" 
            ({columns_str}) VALUES ({placeholders})
            ON CONFLICT ({conflict_columns_str}) 
            DO UPDATE SET {update_set}
            """

            # Добавляем WHERE условие если есть
            if upsert_config.where_condition:
                upsert_query += f" WHERE {upsert_config.where_condition}"

            try:
                await conn.executemany(upsert_query, values)
                logger.debug(f"Upserted batch {batch_idx}: {len(values)} rows")
                return len(values)

            except Exception as e:
                logger.error(f"Failed to upsert batch {batch_idx}: {e}")
                raise

    async def _create_indexes(self):
        """Создание индексов"""
        if not self.config.create_indexes:
            return

        async with self._connection_pool.acquire() as conn:
            for index_config in self.config.create_indexes:
                try:
                    columns = index_config["columns"]
                    index_name = index_config.get(
                        "name", f"idx_{self.config.target_table}_{'_'.join(columns)}"
                    )
                    unique = index_config.get("unique", False)

                    columns_str = ", ".join(f'"{col}"' for col in columns)
                    unique_str = "UNIQUE " if unique else ""

                    create_index_query = f"""
                    CREATE {unique_str}INDEX IF NOT EXISTS "{index_name}"
                    ON "{self.config.target_schema}"."{self.config.target_table}" ({columns_str})
                    """

                    await conn.execute(create_index_query)
                    logger.info(f"Created index: {index_name}")

                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")

    def _generate_load_id(self) -> str:
        """Генерация уникального ID загрузки"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Добавляем хеш от конфигурации для уникальности
        config_str = f"{self.config.target_table}_{timestamp}"
        hash_suffix = hashlib.md5(config_str.encode()).hexdigest()[:8]

        return f"load_{timestamp}_{hash_suffix}"

    @property
    def info(self) -> Info:
        return Info(
            name="PostgreSQLLoad",
            version="1.0.0",
            description="Загрузка данных в PostgreSQL с поддержкой upsert, батчинга и индексов",
            type_class=self.__class__,
            type_module="load",
            config_class=PostgreSQLLoadConfig,
        )
