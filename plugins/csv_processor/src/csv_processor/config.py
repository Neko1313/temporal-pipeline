from pydantic import BaseModel, Field

from core.component import ComponentConfig


class CSVSourceConfig(BaseModel):
    """Конфигурация источника CSV данных"""

    path: str = Field(
        ..., description="Путь к CSV файлу (локальный, HTTP, S3, FTP)"
    )
    encoding: str = Field(default="utf-8", description="Кодировка файла")

    # HTTP/HTTPS параметры
    headers: dict[str, str] | None = Field(
        default=None, description="HTTP заголовки"
    )
    timeout: int = Field(
        default=60, ge=5, le=600, description="Таймаут для HTTP запросов"
    )

    # S3 параметры
    aws_access_key: str | None = Field(
        default=None, description="AWS Access Key"
    )
    aws_secret_key: str | None = Field(
        default=None, description="AWS Secret Key"
    )
    aws_region: str | None = Field(default=None, description="AWS Region")


class CSVExtractConfig(ComponentConfig):
    """Конфигурация CSV Extract компонента"""

    source_config: CSVSourceConfig = Field(
        ..., description="Конфигурация источника"
    )

    delimiter: str = Field(default=",", description="Разделитель столбцов")
    quote_char: str = Field(default='"', description="Символ кавычек")
    has_header: bool = Field(default=True, description="Наличие заголовка")
    skip_rows: int = Field(
        default=0, ge=0, description="Пропустить строк сверху"
    )

    columns: list[str] | None = Field(
        default=None, description="Список колонок для чтения"
    )
    column_mapping: dict[str, str] | None = Field(
        default=None, description="Переименование колонок"
    )
    data_types: dict[str, str] | None = Field(
        default=None, description="Типы данных колонок"
    )

    row_limit: int | None = Field(
        default=None, ge=1, description="Лимит строк"
    )
    filter_condition: str | None = Field(
        default=None, description="Условие фильтрации (Polars expression)"
    )

    ignore_errors: bool = Field(
        default=False, description="Игнорировать ошибки парсинга"
    )
    null_values: list[str] = Field(
        default_factory=lambda: ["", "NULL", "null", "None"],
        description="Значения для NULL",
    )

    batch_size: int = Field(
        default=8192, ge=1024, le=65536, description="Размер батча для чтения"
    )
    streaming: bool = Field(
        default=False, description="Потоковое чтение больших файлов"
    )
