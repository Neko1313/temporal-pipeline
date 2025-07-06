from typing import Any

from pydantic import BaseModel, ClickHouseDsn, Field, MySQLDsn, PostgresDsn

from core.component import ComponentConfig

DB_DSN = MySQLDsn | PostgresDsn | ClickHouseDsn


class SQLSourceConfig(BaseModel):
    """Конфигурация подключения к БД"""

    uri: DB_DSN = Field(..., description="URI подключения к базе данных")
    headers: dict[str, str] | None = Field(
        default=None, description="Дополнительные заголовки"
    )
    connection_pool_size: int = Field(
        default=5, ge=1, le=20, description="Размер пула соединений"
    )
    connection_timeout: int = Field(
        default=30, ge=5, le=300, description="Таймаут соединения в секундах"
    )
    query_timeout: int = Field(
        default=300, ge=10, le=3600, description="Таймаут выполнения запроса"
    )

    # SSL настройки
    ssl_mode: str | None = Field(default=None, description="Режим SSL")
    ssl_cert: str | None = Field(
        default=None, description="Путь к SSL сертификату"
    )
    ssl_key: str | None = Field(default=None, description="Путь к SSL ключу")


class SQLExtractConfig(ComponentConfig):
    """Конфигурация SQL Extract компонента"""

    query: str = Field(
        ..., min_length=1, description="SQL запрос для выполнения"
    )
    source_config: SQLSourceConfig = Field(
        ..., description="Конфигурация подключения к БД"
    )

    # Параметры выполнения
    batch_size: int = Field(
        default=10000, ge=100, le=100000, description="Размер батча для чтения"
    )
    streaming: bool = Field(
        default=False, description="Потоковое чтение данных"
    )

    # Параметры запроса
    query_parameters: dict[str, Any] | None = Field(
        default=None, description="Параметры для SQL запроса"
    )

    # Обработка результатов
    column_mapping: dict[str, str] | None = Field(
        default=None, description="Переименование колонок"
    )
    data_types: dict[str, str] | None = Field(
        default=None, description="Приведение типов данных"
    )

    # Фильтрация и лимиты
    row_limit: int | None = Field(
        default=None, ge=1, description="Лимит строк"
    )
    where_clause: str | None = Field(
        default=None, description="Дополнительное WHERE условие"
    )

    # Кэширование
    cache_results: bool = Field(
        default=False, description="Кэшировать результаты запроса"
    )
    cache_ttl: int = Field(default=3600, description="TTL кэша в секундах")
