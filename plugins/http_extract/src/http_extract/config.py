from typing import Any, Literal

from pydantic import BaseModel, Field

from core.component import ComponentConfig


class AuthConfig(BaseModel):
    """Конфигурация аутентификации."""

    auth_type: Literal["bearer", "basic", "api_key", "oauth2", "none"] = Field(
        ..., description="Тип аутентификации: bearer, basic, api_key, oauth2"
    )

    # Bearer Token
    bearer_token: str | None = Field(default=None, description="Bearer токен")

    # Basic Auth
    username: str | None = Field(
        default=None, description="Имя пользователя для Basic Auth"
    )
    password: str | None = Field(
        default=None, description="Пароль для Basic Auth"
    )

    # API Key
    api_key: str | None = Field(default=None, description="API ключ")
    api_key_header: str = Field(
        default="X-API-Key", description="Название заголовка для API ключа"
    )

    # OAuth2
    oauth2_token_url: str | None = Field(
        default=None, description="URL для получения OAuth2 токена"
    )
    client_id: str | None = Field(default=None, description="OAuth2 Client ID")
    client_secret: str | None = Field(
        default=None, description="OAuth2 Client Secret"
    )
    scope: str | None = Field(default=None, description="OAuth2 scope")


class PaginationConfig(BaseModel):
    """Конфигурация пагинации."""

    pagination_type: Literal[
        "offset", "page", "cursor", "link_header", "none"
    ] = Field(
        ..., description="Тип пагинации: offset, page, cursor, link_header"
    )

    # Offset-based pagination
    limit_param: str = Field(
        default="limit", description="Параметр для лимита"
    )
    offset_param: str = Field(
        default="offset", description="Параметр для смещения"
    )
    default_limit: int = Field(default=100, description="Лимит по умолчанию")
    max_limit: int = Field(default=1000, description="Максимальный лимит")

    # Page-based pagination
    page_param: str = Field(
        default="page", description="Параметр для номера страницы"
    )
    page_size_param: str = Field(
        default="page_size", description="Параметр для размера страницы"
    )
    start_page: int = Field(default=1, description="Начальная страница")

    # Cursor-based pagination
    cursor_param: str = Field(
        default="cursor", description="Параметр для курсора"
    )
    next_cursor_path: str = Field(
        default="next_cursor", description="JSON path для следующего курсора"
    )

    # Link header pagination
    link_header: str = Field(
        default="Link", description="Название заголовка с ссылками"
    )

    # Общие настройки
    max_pages: int = Field(
        default=100, description="Максимальное количество страниц"
    )
    data_path: str = Field(
        default="data", description="JSON path к данным в ответе"
    )


class HTTPExtractConfig(ComponentConfig):
    """Конфигурация HTTP Extract компонента."""

    # Основные параметры
    url: str = Field(..., description="URL для запроса")
    method: Literal[
        "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"
    ] = Field(default="GET", description="HTTP метод")
    headers: dict[str, str] | None = Field(
        default=None, description="HTTP заголовки"
    )
    params: dict[str, Any] | None = Field(
        default=None, description="URL параметры"
    )

    # Аутентификация
    auth_config: AuthConfig | None = Field(
        default=None, description="Конфигурация аутентификации"
    )

    # Пагинация
    pagination_config: PaginationConfig | None = Field(
        default=None, description="Конфигурация пагинации"
    )

    # Формат данных
    response_format: Literal["json", "xml", "csv", "text"] = Field(
        default="json", description="Формат ответа: json, xml, csv, text"
    )
    encoding: str = Field(default="utf-8", description="Кодировка ответа")

    # JSON обработка
    json_data_path: str = Field(
        default="", description="JSON path к данным (например: data.items)"
    )
    json_flatten: bool = Field(
        default=False, description="Выравнивать вложенные JSON объекты"
    )

    # XML обработка
    xml_record_tag: str | None = Field(
        default=None, description="Тег для записей в XML"
    )
    xml_namespaces: dict[str, str] | None = Field(
        default=None, description="XML namespaces"
    )

    # Параметры запроса
    timeout: int = Field(
        default=30, ge=1, le=300, description="Таймаут запроса в секундах"
    )
    retries: int = Field(
        default=3, ge=0, le=10, description="Количество повторных попыток"
    )
    retry_delay: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Задержка между попытками"
    )

    # Обработка ошибок
    ignore_http_errors: bool = Field(
        default=False, description="Игнорировать HTTP ошибки"
    )
    valid_status_codes: list[int] = Field(
        default_factory=lambda: [200], description="Валидные коды ответа"
    )

    # Кэширование
    cache_responses: bool = Field(
        default=False, description="Кэшировать ответы"
    )
    cache_ttl: int = Field(default=3600, description="TTL кэша в секундах")

    # Rate limiting
    rate_limit: float | None = Field(
        default=None, description="Лимит запросов в секунду"
    )
