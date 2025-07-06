from typing import Any, Literal

from pydantic import BaseModel, Field, PostgresDsn

from core.component import ComponentConfig


class PostgreSQLConnectionConfig(BaseModel):
    """Конфигурация подключения к PostgreSQL"""

    uri: PostgresDsn = Field(..., description="URI подключения к PostgreSQL")
    pool_size: int = Field(default=5, ge=1, le=20, description="Размер пула соединений")
    pool_timeout: int = Field(
        default=30, ge=5, le=300, description="Таймаут пула соединений"
    )
    connection_timeout: int = Field(
        default=60, ge=10, le=600, description="Таймаут соединения"
    )

    # SSL настройки
    ssl_mode: str = Field(default="prefer", description="Режим SSL")
    ssl_cert: str | None = Field(default=None, description="Путь к SSL сертификату")
    ssl_key: str | None = Field(default=None, description="Путь к SSL ключу")


class UpsertConfig(BaseModel):
    """Конфигурация для upsert операций"""

    conflict_columns: list[str] = Field(
        ..., description="Колонки для определения конфликтов"
    )
    update_columns: list[str] | None = Field(
        default=None, description="Колонки для обновления (по умолчанию все)"
    )
    where_condition: str | None = Field(
        default=None, description="Дополнительное WHERE условие для UPDATE"
    )


class PostgreSQLLoadConfig(ComponentConfig):
    """Конфигурация PostgreSQL Load компонента"""

    connection_config: PostgreSQLConnectionConfig = Field(
        ..., description="Конфигурация подключения"
    )

    # Целевая таблица
    target_table: str = Field(..., min_length=1, description="Имя целевой таблицы")
    target_schema: str = Field(default="public", description="Схема базы данных")

    # Стратегия загрузки
    if_exists: Literal["append", "replace", "fail", "upsert"] = Field(
        default="append",
        description="Что делать если таблица существует: append, replace, fail, upsert",
    )

    # Upsert конфигурация
    upsert_config: UpsertConfig | None = Field(
        default=None, description="Конфигурация для upsert операций"
    )

    # Параметры загрузки
    batch_size: int = Field(
        default=1000, ge=100, le=10000, description="Размер батча для загрузки"
    )
    parallel_batches: int = Field(
        default=1, ge=1, le=10, description="Количество параллельных батчей"
    )

    # Обработка данных
    column_mapping: dict[str, str] | None = Field(
        default=None,
        description="Переименование колонок: {source_column: target_column}",
    )

    # Создание таблицы
    create_table: bool = Field(
        default=True, description="Создать таблицу если не существует"
    )
    table_options: dict[str, Any] | None = Field(
        default=None, description="Дополнительные опции для создания таблицы"
    )

    # Индексы
    create_indexes: list[dict[str, Any]] | None = Field(
        default=None,
        description="Индексы для создания: [{columns: [col1, col2], unique: bool, name: str}]",
    )

    # Валидация данных
    validate_data: bool = Field(
        default=True, description="Валидировать данные перед загрузкой"
    )
    max_errors: int = Field(
        default=0, ge=0, description="Максимум ошибок (0 = прервать при первой)"
    )

    # Метаданные
    add_load_metadata: bool = Field(
        default=True, description="Добавить метаданные загрузки"
    )
    metadata_columns: dict[str, str] | None = Field(
        default_factory=lambda: {
            "loaded_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "load_id": "VARCHAR(50)",
            "source_file": "VARCHAR(255)",
        },
        description="Метаданные колонки для добавления",
    )
