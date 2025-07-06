from typing import Literal

from pydantic import BaseModel, Field

from core.component import ComponentConfig


class AggregationConfig(BaseModel):
    """Конфигурация агрегации"""

    group_by: list[str] = Field(..., description="Колонки для группировки")
    aggregations: dict[
        str,
        Literal[
            "sum",
            "avg",
            "count",
            "min",
            "max",
            "std",
            "var",
            "first",
            "last",
        ],
    ] = Field(..., description="Агрегации: {column: function}")


class JoinConfig(BaseModel):
    """Конфигурация соединения с другими данными"""

    join_type: Literal[
        "inner",
        "left",
        "right",
        "outer",
        "cross",
        "anti",
        "semi",
    ] = Field(default="inner", description="Тип соединения")
    left_on: str = Field(..., description="Колонка для соединения (левая)")
    right_on: str = Field(..., description="Колонка для соединения (правая)")
    suffix: str = Field(
        default="_right", description="Суффикс для дублирующихся колонок"
    )


class JSONNormalizationConfig(BaseModel):
    """Конфигурация нормализации JSON"""

    json_columns: list[str] = Field(..., description="Колонки с JSON данными")
    max_level: int = Field(
        default=3, ge=1, le=10, description="Максимальная глубина нормализации"
    )
    separator: str = Field(
        default="_", description="Разделитель для имен колонок"
    )
    preserve_original: bool = Field(
        default=False, description="Сохранить оригинальные JSON колонки"
    )


class JSONTransformConfig(ComponentConfig):
    """Конфигурация JSON Transform компонента"""

    json_normalization: JSONNormalizationConfig | None = Field(
        default=None, description="Конфигурация нормализации JSON"
    )

    column_operations: dict[str, str] | None = Field(
        default=None,
        description="Операции над колонками: {new_column: expression}",
    )

    filter_conditions: list[str] | None = Field(
        default=None, description="Условия фильтрации (Polars expressions)"
    )

    sort_columns: list[str] | None = Field(
        default=None, description="Колонки для сортировки"
    )
    sort_descending: bool = Field(
        default=False, description="Сортировка по убыванию"
    )

    aggregation: AggregationConfig | None = Field(
        default=None, description="Конфигурация агрегации"
    )

    join_config: JoinConfig | None = Field(
        default=None, description="Конфигурация соединения"
    )

    deduplicate: bool = Field(default=False, description="Удалить дубликаты")
    deduplicate_columns: list[str] | None = Field(
        default=None, description="Колонки для определения дубликатов"
    )

    fill_null_strategy: str | None = Field(
        default=None,
        description="Стратегия заполнения NULL: forward, backward, zero, mean",
    )
    drop_null_columns: list[str] | None = Field(
        default=None, description="Колонки для удаления строк с NULL"
    )

    sample_size: int | None = Field(
        default=None, ge=1, description="Размер выборки"
    )
    sample_fraction: float | None = Field(
        default=None, ge=0.0, le=1.0, description="Доля для семплирования"
    )

    add_metadata: bool = Field(
        default=False, description="Добавить метаданные обработки"
    )
