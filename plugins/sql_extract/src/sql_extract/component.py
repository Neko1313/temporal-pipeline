"""
SQL Extract Plugin - Полная реализация для извлечения данных из SQL баз данных
Поддерживает PostgreSQL, MySQL, SQLite, SQL Server
"""

import asyncio
import hashlib
import logging
from datetime import UTC, datetime
from urllib.parse import urlparse

import aiosqlite
import polars as pl

from core.component import BaseProcessClass, Info, Result
from sql_extract.config import SQLExtractConfig

logger = logging.getLogger(__name__)


class SQLExtract(BaseProcessClass):
    """
    SQL Extract компонент для извлечения данных из SQL баз данных

    Поддерживаемые функции:
    - Подключение к PostgreSQL, MySQL, SQLite, SQL Server
    - Потоковое и батчевое чтение
    - Параметризованные запросы
    - Преобразование типов данных
    - Переименование колонок
    - Кэширование результатов
    """

    config: SQLExtractConfig

    def __init__(self) -> None:
        super().__init__()
        self._connection_pool = None
        self._cache = {}

    async def process(self) -> Result | None:
        """Основной метод обработки"""
        try:
            logger.info(
                f"Starting SQL extraction with\
                query: {self.config.query[:100]}..."
            )

            # Проверяем кэш
            if self.config.cache_results:
                cached_result = self._check_cache()
                if cached_result is not None:
                    logger.info("Using cached result")
                    return Result(status="success", response=cached_result)

            # Подготовка запроса
            final_query = self._prepare_query()
            logger.debug(f"Final query: {final_query}")

            # Установка соединения
            await self._connect()

            # Выполнение запроса
            if self.config.streaming:
                data = await self._execute_streaming_query(final_query)
            else:
                data = await self._execute_batch_query(final_query)

            # Постобработка данных
            if data is not None and len(data) > 0:
                data = self._postprocess_data(data)

                # Кэшируем результат
                if self.config.cache_results:
                    self._cache_result(data)

                logger.info(
                    "Successfully extracted %s rows with %s columns",
                    len(data),
                    len(data.columns),
                )

                return Result(status="success", response=data)
            logger.warning("Query returned no data")
            return Result(status="success", response=pl.DataFrame())

        except Exception as e:
            logger.error(f"SQL extraction failed: {e!s}")
            return Result(status="error", response=None)
        finally:
            await self._disconnect()

    def _prepare_query(self) -> str:
        """Подготовка SQL запроса"""
        query = self.config.query.strip()

        # Добавляем дополнительное WHERE условие
        if self.config.where_clause:
            if "WHERE" in query.upper():
                query += f" AND ({self.config.where_clause})"
            else:
                query += f" WHERE {self.config.where_clause}"

        # Добавляем LIMIT если указан
        if self.config.row_limit and not any(
            keyword in query.upper() for keyword in ["LIMIT", "TOP"]
        ):
            query += f" LIMIT {self.config.row_limit}"

        return query

    async def _connect(self) -> None:
        """Установка соединения с базой данных"""
        parsed_uri = urlparse(self.config.source_config.uri)
        scheme = parsed_uri.scheme.split("+")[0]

        if scheme in ["postgresql", "postgres"]:
            await self._connect_postgresql()
        elif scheme == "mysql":
            await self._connect_mysql()
        elif scheme == "sqlite":
            await self._connect_sqlite()
        elif scheme == "mssql":
            await self._connect_mssql()
        else:
            msg = f"Unsupported database type: {scheme}"
            raise ValueError(msg)

    async def _connect_postgresql(self) -> None:
        """Подключение к PostgreSQL"""
        try:
            import asyncpg  # noqa: PLC0415
        except ImportError as ie:
            msg = "asyncpg library is required for\
            PostgreSQL. Install: pip install asyncpg"
            raise ImportError(msg) from ie

        try:
            self._connection_pool = await asyncpg.create_pool(
                self.config.source_config.uri,
                min_size=1,
                max_size=self.config.source_config.connection_pool_size,
                command_timeout=self.config.source_config.query_timeout,
                server_settings={"jit": "off"},
            )
            logger.debug("PostgreSQL connection pool created")
        except Exception as ex:
            msg = f"Failed to connect to PostgreSQL: {ex}"
            raise Exception(msg) from ex

    async def _connect_mysql(self) -> None:
        """Подключение к MySQL"""
        try:
            import aiomysql  # noqa: PLC0415
        except ImportError as ie:
            msg = "aiomysql library is required for MySQL.\
            Install: pip install aiomysql"
            raise ImportError(msg) from ie

        try:
            self._connection_pool = await aiomysql.create_pool(
                host=self.config.source_config.uri.hostname,
                port=self.config.source_config.uri.port or 3306,
                user=self.config.source_config.uri.username,
                password=self.config.source_config.uri.password,
                db=self.config.source_config.uri.path,
                minsize=1,
                maxsize=self.config.source_config.connection_pool_size,
                autocommit=True,
            )
            logger.debug("MySQL connection pool created")
        except Exception as ex:
            msg = f"Failed to connect to MySQL: {ex}"
            raise Exception(msg) from ex

    async def _connect_sqlite(self) -> None:
        """Подключение к SQLite"""
        try:
            db_path = self.config.source_config.uri.path
            if db_path.startswith("/"):
                db_path = db_path[1:]

            self._sqlite_path = db_path
            logger.debug(f"SQLite connection configured for: {db_path}")
        except Exception as ex:
            msg = f"Failed to configure SQLite connection: {ex}"
            raise Exception(msg) from ex

    async def _connect_mssql(self) -> None:
        """Подключение к SQL Server"""
        try:
            import aioodbc  # noqa: PLC0415
        except ImportError as ie:
            msg = "aioodbc library is required for\
            SQL Server. Install: pip install aioodbc"
            raise ImportError(msg) from ie

        try:
            # Создаем connection string для SQL Server
            self._connection_pool = await aioodbc.create_pool(
                dsn=self.config.source_config.uri,
                minsize=1,
                maxsize=self.config.source_config.connection_pool_size,
            )
            logger.debug("SQL Server connection pool created")
        except Exception as ex:
            msg = f"Failed to connect to SQL Server: {ex}"
            raise Exception(msg) from ex

    async def _disconnect(self) -> None:
        """Закрытие соединения"""
        if self._connection_pool:
            if hasattr(self._connection_pool, "close"):
                await self._connection_pool.close()
            logger.debug("Database connection closed")

    async def _execute_batch_query(self, query: str) -> pl.DataFrame | None:
        """Выполнение запроса в batch режиме"""

        if self.config.source_config.uri.scheme in ["postgresql", "postgres"]:
            return await self._execute_postgresql_query(query)
        if self.config.source_config.uri.scheme == "mysql":
            return await self._execute_mysql_query(query)
        if self.config.source_config.uri.scheme == "sqlite":
            return await self._execute_sqlite_query(query)
        if self.config.source_config.uri.scheme == "mssql":
            return await self._execute_mssql_query(query)
        return None

    async def _execute_postgresql_query(self, query: str) -> pl.DataFrame:
        """Выполнение PostgreSQL запроса"""
        if self._connection_pool is None:
            msg = ""
            raise Exception(msg)
        async with self._connection_pool.acquire() as conn:
            # Подготавливаем параметры
            params = (
                list(self.config.query_parameters.values())
                if self.config.query_parameters
                else []
            )

            try:
                rows = await conn.fetch(query, *params)

                if not rows:
                    return pl.DataFrame()

                # Конвертируем результат в DataFrame
                data = []

                for row in rows:
                    data.append(dict(row))

                df = pl.DataFrame(data)
                logger.debug(
                    "PostgreSQL query executed: %s rows, %s columns",
                    len(df),
                    len(df.columns),
                )
                return df

            except Exception as ex:
                msg = f"PostgreSQL query execution failed: {ex}"
                raise Exception(msg) from ex

    async def _execute_mysql_query(self, query: str) -> pl.DataFrame:
        """Выполнение MySQL запроса"""
        if self._connection_pool is None:
            msg = ""
            raise Exception(msg)
        async with (
            self._connection_pool.acquire() as conn,
            conn.cursor() as cursor,
        ):
            try:
                # Подготавливаем параметры
                params = (
                    list(self.config.query_parameters.values())
                    if self.config.query_parameters
                    else []
                )

                await cursor.execute(query, params)
                rows = await cursor.fetchall()

                if not rows:
                    return pl.DataFrame()

                # Получаем имена колонок
                columns = [desc[0] for desc in cursor.description]

                # Конвертируем в список словарей
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row, strict=False)))

                df = pl.DataFrame(data)
                logger.debug(
                    "MySQL query executed: %s rows, %s columns",
                    len(df),
                    len(df.columns),
                )
                return df

            except Exception as ex:
                msg = f"MySQL query execution failed: {ex}"
                raise Exception(msg) from ex

    async def _execute_sqlite_query(self, query: str) -> pl.DataFrame:
        """Выполнение SQLite запроса"""
        try:
            if self._sqlite_path is None:
                raise NotImplementedError()

            async with aiosqlite.connect(str(self._sqlite_path)) as conn:
                conn.row_factory = aiosqlite.Row  # Для получения словарей

                # Подготавливаем параметры
                params = (
                    list(self.config.query_parameters.values())
                    if self.config.query_parameters
                    else []
                )

                async with conn.execute(query, params) as cursor:
                    rows = await cursor.fetchall()

                    if not rows:
                        return pl.DataFrame()

                    data = [dict(row) for row in rows]

                    df = pl.DataFrame(data)
                    logger.debug(
                        "SQLite query executed: %s rows, %s columns",
                        len(df),
                        len(df.columns),
                    )
                    return df

        except Exception as ex:
            msg = f"SQLite query execution failed: {ex}"
            raise Exception(msg) from ex

    async def _execute_mssql_query(self, query: str) -> pl.DataFrame:
        """Выполнение SQL Server запроса"""
        if self._connection_pool is None:
            msg = ""
            raise Exception(msg)
        async with (
            self._connection_pool.acquire() as conn,
            conn.cursor() as cursor,
        ):
            try:
                # Подготавливаем параметры
                params = (
                    list(self.config.query_parameters.values())
                    if self.config.query_parameters
                    else []
                )

                await cursor.execute(query, params)
                rows = await cursor.fetchall()

                if not rows:
                    return pl.DataFrame()

                # Получаем имена колонок
                columns = [desc[0] for desc in cursor.description]

                # Конвертируем в список словарей
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row, strict=False)))

                df = pl.DataFrame(data)
                logger.debug(
                    "SQL Server query executed: %s rows, %s columns",
                    len(df),
                    len(df.columns),
                )
                return df

            except Exception as ex:
                msg = f"SQL Server query execution failed: {ex}"
                raise Exception(msg) from ex

    async def _execute_streaming_query(self, query: str) -> pl.DataFrame:
        """Выполнение запроса в streaming режиме"""
        all_chunks = []
        batch_offset = 0

        while True:
            # Модифицируем запрос для батчевого чтения
            batch_query = self._add_pagination_to_query(
                query, batch_offset, self.config.batch_size
            )

            # Выполняем батч
            batch_df = await self._execute_batch_query(batch_query)

            if batch_df is None or len(batch_df) == 0:
                break

            all_chunks.append(batch_df)
            logger.debug(
                "Processed streaming batch: %s rows (offset: %s)",
                len(batch_df),
                batch_offset,
            )

            # Если получили меньше данных чем размер батча, это последний батч
            if len(batch_df) < self.config.batch_size:
                break

            batch_offset += self.config.batch_size

            # Небольшая пауза для асинхронности
            await asyncio.sleep(0.001)

        # Объединяем все батчи
        if all_chunks:
            result_df = pl.concat(all_chunks)
            logger.debug(
                f"Streaming query complete: {len(result_df)} total rows"
            )
            return result_df
        return pl.DataFrame()

    def _add_pagination_to_query(
        self, query: str, offset: int, limit: int
    ) -> str:
        """Добавление пагинации к запросу"""
        # Простая реализация - добавляем OFFSET и LIMIT
        if "LIMIT" not in query.upper():
            query += f" LIMIT {limit} OFFSET {offset}"
        return query

    def _postprocess_data(self, data: pl.DataFrame) -> pl.DataFrame:
        if self.config.column_mapping:
            data = self._rename_columns(data)

        if self.config.data_types:
            data = self._cast_columns(data)

        if self.config.row_limit and len(data) > self.config.row_limit:
            data = data.head(self.config.row_limit)

        return data

    def _rename_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        if self.config.column_mapping is None:
            return data

        renames = {
            old_name: new_name
            for old_name, new_name in self.config.column_mapping.items()
            if old_name in data.columns
        }
        return data.rename(renames)

    def _cast_columns(self, data: pl.DataFrame) -> pl.DataFrame:
        if self.config.data_types is None:
            return data

        for column, dtype in self.config.data_types.items():
            if column not in data.columns:
                continue

            try:
                data = data.with_columns(self._cast_column(column, dtype))
            except Exception as e:
                logger.warning(
                    f"Failed to cast column {column} to {dtype}: {e}"
                )

        return data

    def _cast_column(self, column: str, dtype: str) -> pl.Expr:
        match dtype.lower():
            case "int":
                return pl.col(column).cast(pl.Int64)
            case "float":
                return pl.col(column).cast(pl.Float64)
            case "str":
                return pl.col(column).cast(pl.Utf8)
            case "bool":
                return pl.col(column).cast(pl.Boolean)
            case "date":
                return pl.col(column).cast(pl.Date)
            case "datetime":
                return pl.col(column).cast(pl.Datetime)
            case _:
                msg = f"Unsupported dtype: {dtype}"
                raise ValueError(msg)

    def _check_cache(self) -> pl.DataFrame | None:
        """Проверка кэша"""
        cache_key = self._generate_cache_key()

        if cache_key in self._cache:
            cache_entry = self._cache[cache_key]
            if datetime.now(tz=UTC) < cache_entry["expires"]:
                return cache_entry["data"]
            # Удаляем устаревшую запись
            del self._cache[cache_key]

        return None

    def _cache_result(self, data: pl.DataFrame) -> None:
        """Кэширование результата"""
        cache_key = self._generate_cache_key()
        expires = datetime.now(tz=UTC).timestamp() + self.config.cache_ttl

        self._cache[cache_key] = {
            "data": data,
            "expires": datetime.fromtimestamp(expires, tz=UTC),
        }

    def _generate_cache_key(self) -> str:
        """Генерация ключа для кэша"""
        # Создаем хеш на основе запроса и параметров
        cache_string = (
            f"{self.config.query}:{self.config.query_parameters or {}}"
        )
        return hashlib.sha256(cache_string.encode()).hexdigest()

    @property
    def info(self) -> Info:
        return Info(
            name="SQLExtract",
            version="1.0.0",
            description="""
Извлечение данных из SQL баз\
данных с поддержкой множества СУБД
""",
            type_class=self.__class__,
            type_module="extract",
            config_class=SQLExtractConfig,
        )
