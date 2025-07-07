"""
SQL Extract Plugin - Переписанная версия с использованием SQLAlchemy
Поддерживает PostgreSQL, MySQL, SQLite, SQL Server через единый интерфейс
"""

import hashlib
import logging
from typing import Any
from urllib.parse import urlparse

import polars as pl
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from core.component import BaseProcessClass, Result
from sql_extract.config import SQLExtractConfig

logger = logging.getLogger(__name__)


class SQLExtract(BaseProcessClass[SQLExtractConfig]):
    def __init__(self, config: SQLExtractConfig) -> None:
        super().__init__(config)
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker | None = None
        self._cache: dict[str, pl.DataFrame] = {}

    async def process(self) -> Result | None:
        """Основной метод обработки"""
        try:
            logger.info(
                f"Starting SQL extraction: {self.config.query[:100]}..."
            )

            # Проверяем кэш
            if self.config.cache_results:
                cached_result = self._check_cache()
                if cached_result is not None:
                    logger.info("Используется кэшированный результат")
                    return Result(status="success", response=cached_result)

            # Подготовка запроса
            final_query = self._prepare_query()
            logger.debug(f"Финальный запрос: {final_query}")

            # Создание движка SQLAlchemy
            await self._create_engine()

            # Выполнение запроса
            data = await self._execute_query(final_query)

            # Постобработка данных
            if data is not None and len(data) > 0:
                data = self._postprocess_data(data)

                # Кэширование результата
                if self.config.cache_results:
                    self._cache_result(data)

                logger.info(
                    "Успешно извлечено %s строк с %s колонками",
                    len(data),
                    len(data.columns),
                )
                return Result(status="success", response=data)

            logger.warning("Запрос не вернул данных")
            return Result(status="success", response=pl.DataFrame())

        except Exception as e:
            logger.error(f"Ошибка SQL извлечения: {e}")
            return Result(status="error", response=None)
        finally:
            await self._cleanup()

    def _prepare_query(self) -> str:
        """Подготовка SQL запроса с дополнительными условиями"""
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

    async def _create_engine(self) -> None:
        """Создание асинхронного движка SQLAlchemy"""
        try:
            # Конвертируем URI для SQLAlchemy
            uri = self._convert_uri_for_sqlalchemy(
                str(self.config.source_config.uri)
            )

            # Создаем асинхронный движок
            self._engine = create_async_engine(
                uri,
                pool_size=self.config.source_config.connection_pool_size,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False,  # Установите True для отладки SQL запросов
            )

            # Создаем фабрику сессий
            self._session_factory = async_sessionmaker(
                self._engine, expire_on_commit=False
            )

            logger.debug(f"SQLAlchemy движок создан для {uri}")

        except Exception as e:
            msg = f"Не удалось создать SQLAlchemy движок: {e}"
            raise Exception(msg) from e

    def _convert_uri_for_sqlalchemy(self, uri: str) -> str:
        """Конвертация URI для совместимости с SQLAlchemy"""
        parsed = urlparse(uri)
        scheme = parsed.scheme

        scheme_mapping = {
            "postgresql": "postgresql+asyncpg",
            "postgres": "postgresql+asyncpg",
            "mysql": "mysql+aiomysql",
            "sqlite": "sqlite+aiosqlite",
            "mssql": "mssql+aioodbc",
        }

        if scheme in scheme_mapping:
            # Заменяем схему на асинхронную версию
            new_scheme = scheme_mapping[scheme]
            return uri.replace(f"{scheme}://", f"{new_scheme}://", 1)

        return uri

    async def _execute_query(self, query: str) -> pl.DataFrame | None:
        """Выполнение SQL запроса через SQLAlchemy"""
        if not self._session_factory:
            msg = "Session factory не инициализирована"
            raise Exception(msg)

        try:
            async with self._session_factory() as session:
                # Подготавливаем параметры запроса
                params = self.config.query_parameters or {}

                # Выполняем запрос
                result = await session.execute(text(query), params)

                # Получаем результаты
                rows = result.fetchall()

                if not rows:
                    return pl.DataFrame()

                # Получаем названия колонок
                columns = list(result.keys())

                # Конвертируем в список словарей
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row, strict=False)))

                # Создаем DataFrame
                df = pl.DataFrame(data)

                logger.debug(
                    "Запрос выполнен: %s строк, %s колонок",
                    len(df),
                    len(df.columns),
                )

                return df

        except Exception as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise

    def _postprocess_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """Постобработка данных"""
        if self.config.column_mapping:
            data = data.rename(self.config.column_mapping)

        if self.config.type_mapping:
            for column, target_type in self.config.type_mapping.items():
                if column in data.columns:
                    try:
                        if target_type == "string":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Utf8)
                            )
                        elif target_type == "integer":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Int64)
                            )
                        elif target_type == "float":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Float64)
                            )
                        elif target_type == "boolean":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Boolean)
                            )
                        elif target_type == "date":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Date)
                            )
                        elif target_type == "datetime":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Datetime)
                            )
                    except Exception as ex:
                        logger.warning(
                            "Не удалось преобразовать колонку %s в %s: %s",
                            column,
                            target_type,
                            ex,
                        )

        return data

    def _check_cache(self) -> pl.DataFrame | None:
        """Проверка кэша"""
        cache_key = self._generate_cache_key()
        return self._cache.get(cache_key)

    def _cache_result(self, data: pl.DataFrame) -> None:
        """Кэширование результата"""
        cache_key = self._generate_cache_key()
        self._cache[cache_key] = data

    def _generate_cache_key(self) -> str:
        """Генерация ключа кэша"""
        query_hash = hashlib.sha256(
            f"{self.config.query}{self.config.where_clause}{self.config.row_limit}".encode()
        ).hexdigest()
        return f"sql_extract_{query_hash}"

    async def _cleanup(self) -> None:
        """Очистка ресурсов"""
        if self._engine:
            await self._engine.dispose()
            logger.debug("SQLAlchemy движок закрыт")


class SQLQueryBuilder:
    """Утилита для построения SQL запросов"""

    @staticmethod
    def build_select_query(
        table: str,
        columns: list | None = None,
        where_conditions: dict[str, Any] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Построение SELECT запроса"""
        cols = "*" if not columns else ", ".join(columns)
        query = f"SELECT {cols} FROM {table}"  # noqa: S608

        if where_conditions:
            conditions = []
            for key, value in where_conditions.items():
                if isinstance(value, str):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(conditions)

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        return query

    @staticmethod
    def build_count_query(
        table: str, where_conditions: dict[str, Any] | None = None
    ) -> str:
        """Построение COUNT запроса"""
        query = f"SELECT COUNT(*) as total_count FROM {table}"  # noqa: S608

        if where_conditions:
            conditions = []
            for key, value in where_conditions.items():
                if isinstance(value, str):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
            query += " WHERE " + " AND ".join(conditions)

        return query


class DatabaseConnectionTester:
    """Утилита для тестирования подключений к базам данных"""

    @staticmethod
    async def test_connection(uri: str) -> bool:
        """Тестирование подключения к базе данных"""
        try:
            # Создаем временный движок
            engine = create_async_engine(uri, pool_size=1)

            # Пытаемся выполнить простой запрос
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            await engine.dispose()
            return True

        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            return False
