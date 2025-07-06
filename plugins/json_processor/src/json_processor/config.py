from pydantic import BaseModel, Field, field_validator

from core.component import ComponentConfig


class AggregationConfig(BaseModel):
    """Конфигурация агрегации"""

    group_by: list[str] = Field(..., description="Колонки для группировки")
    aggregations: dict[str, str] = Field(
        ..., description="Агрегации: {column: function}"
    )

    @field_validator("aggregations")
    @classmethod
    def validate_aggregations(cls, v):
        valid_functions = {
            "sum",
            "avg",
            "count",
            "min",
            "max",
            "std",
            "var",
            "first",
            "last",
        }
        for col, func in v.items():
            if func not in valid_functions:
                raise ValueError(
                    f"Invalid aggregation function '{func}'. Valid: {valid_functions}"
                )
        return v


class JoinConfig(BaseModel):
    """Конфигурация соединения с другими данными"""

    join_type: str = Field(default="inner", description="Тип соединения")
    left_on: str = Field(..., description="Колонка для соединения (левая)")
    right_on: str = Field(..., description="Колонка для соединения (правая)")
    suffix: str = Field(
        default="_right", description="Суффикс для дублирующихся колонок"
    )

    @field_validator("join_type")
    @classmethod
    def validate_join_type(cls, v):
        valid_types = {"inner", "left", "right", "outer", "cross", "anti", "semi"}
        if v not in valid_types:
            raise ValueError(f"Invalid join type '{v}'. Valid: {valid_types}")
        return v


class JSONNormalizationConfig(BaseModel):
    """Конфигурация нормализации JSON"""

    json_columns: list[str] = Field(..., description="Колонки с JSON данными")
    max_level: int = Field(
        default=3, ge=1, le=10, description="Максимальная глубина нормализации"
    )
    separator: str = Field(default="_", description="Разделитель для имен колонок")
    preserve_original: bool = Field(
        default=False, description="Сохранить оригинальные JSON колонки"
    )


class JSONTransformConfig(ComponentConfig):
    """Конфигурация JSON Transform компонента"""

    json_normalization: JSONNormalizationConfig | None = Field(
        default=None, description="Конфигурация нормализации JSON"
    )

    column_operations: dict[str, str] | None = Field(
        default=None, description="Операции над колонками: {new_column: expression}"
    )

    filter_conditions: list[str] | None = Field(
        default=None, description="Условия фильтрации (Polars expressions)"
    )

    # Сортировка
    sort_columns: list[str] | None = Field(
        default=None, description="Колонки для сортировки"
    )
    sort_descending: bool = Field(default=False, description="Сортировка по убыванию")

    # Агрегация
    aggregation: AggregationConfig | None = Field(
        default=None, description="Конфигурация агрегации"
    )

    # Соединение с dependency данными
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

    sample_size: int | None = Field(default=None, ge=1, description="Размер выборки")
    sample_fraction: float | None = Field(
        default=None, ge=0.0, le=1.0, description="Доля для семплирования"
    )

    add_metadata: bool = Field(
        default=False, description="Добавить метаданные обработки"
    )
