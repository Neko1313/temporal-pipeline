"""
HTTP Extract Plugin - Извлечение данных из REST API и HTTP источников
Поддерживает JSON, XML, CSV, pagination, authentication
"""

from typing import Optional, Dict, Any, List
import polars as pl
import logging
import asyncio
import aiohttp
import json
import xml.etree.ElementTree as ET
import base64
from datetime import datetime, timedelta

from core.component import BaseProcessClass, Result, Info
from http_extract.config import HTTPExtractConfig

logger = logging.getLogger(__name__)


class HTTPExtract(BaseProcessClass):
    """
    HTTP Extract компонент для извлечения данных из REST API и HTTP источников

    Поддерживаемые функции:
    - Различные типы аутентификации (Bearer, Basic, API Key, OAuth2)
    - Пагинация (offset, page, cursor, link header)
    - Обработка JSON, XML, CSV ответов
    - Retry механизм с экспоненциальной задержкой
    - Rate limiting
    - Кэширование ответов
    """

    config: HTTPExtractConfig

    def __init__(self):
        super().__init__()
        self._session = None
        self._auth_headers = {}
        self._cache = {}
        self._last_request_time = None

    async def process(self) -> Result:
        """Основной метод обработки"""
        try:
            logger.info(f"Starting HTTP extraction from: {self.config.url}")

            # Создаем HTTP сессию
            await self._create_session()

            # Настраиваем аутентификацию
            await self._setup_authentication()

            # Извлекаем данные
            if self.config.pagination_config:
                data = await self._extract_paginated_data()
            else:
                data = await self._extract_single_request()

            # Обрабатываем данные
            if data is not None and len(data) > 0:
                logger.info(
                    f"Successfully extracted {len(data)} rows with {len(data.columns)} columns"
                )

                return Result(status="success", response=data)
            else:
                logger.warning("No data extracted from HTTP source")
                return Result(status="success", response=pl.DataFrame())

        except Exception as e:
            logger.error(f"HTTP extraction failed: {str(e)}")
            return Result(status="error", response=None)
        finally:
            await self._close_session()

    async def _create_session(self):
        """Создание HTTP сессии"""
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)

        self._session = aiohttp.ClientSession(
            timeout=timeout, connector=connector, headers=self.config.headers or {}
        )

    async def _close_session(self):
        """Закрытие HTTP сессии"""
        if self._session:
            await self._session.close()

    async def _setup_authentication(self):
        """Настройка аутентификации"""
        if not self.config.auth_config:
            return

        auth_config = self.config.auth_config

        if auth_config.auth_type == "bearer" and auth_config.bearer_token:
            self._auth_headers["Authorization"] = f"Bearer {auth_config.bearer_token}"

        elif (
            auth_config.auth_type == "basic"
            and auth_config.username
            and auth_config.password
        ):
            credentials = base64.b64encode(
                f"{auth_config.username}:{auth_config.password}".encode()
            ).decode()
            self._auth_headers["Authorization"] = f"Basic {credentials}"

        elif auth_config.auth_type == "api_key" and auth_config.api_key:
            self._auth_headers[auth_config.api_key_header] = auth_config.api_key

        elif auth_config.auth_type == "oauth2":
            await self._setup_oauth2()

    async def _setup_oauth2(self):
        """Настройка OAuth2 аутентификации"""
        auth_config = self.config.auth_config

        if not auth_config.oauth2_token_url:
            raise ValueError("oauth2_token_url is required for OAuth2 authentication")

        token_data = {
            "grant_type": "client_credentials",
            "client_id": auth_config.client_id,
            "client_secret": auth_config.client_secret,
        }

        if auth_config.scope:
            token_data["scope"] = auth_config.scope

        async with self._session.post(
            auth_config.oauth2_token_url, data=token_data
        ) as response:
            response.raise_for_status()
            token_response = await response.json()

            access_token = token_response.get("access_token")
            if access_token:
                self._auth_headers["Authorization"] = f"Bearer {access_token}"
                logger.debug("OAuth2 token obtained successfully")
            else:
                raise ValueError("Failed to obtain OAuth2 access token")

    async def _extract_single_request(self) -> pl.DataFrame:
        """Извлечение данных одним запросом"""
        response_data = await self._make_request(self.config.url, self.config.params)
        return await self._parse_response(response_data)

    async def _extract_paginated_data(self) -> pl.DataFrame:
        """Извлечение данных с пагинацией"""
        all_dataframes = []
        pagination_config = self.config.pagination_config

        if pagination_config.pagination_type == "offset":
            all_dataframes = await self._extract_offset_pagination()
        elif pagination_config.pagination_type == "page":
            all_dataframes = await self._extract_page_pagination()
        elif pagination_config.pagination_type == "cursor":
            all_dataframes = await self._extract_cursor_pagination()
        elif pagination_config.pagination_type == "link_header":
            all_dataframes = await self._extract_link_header_pagination()

        # Объединяем все DataFrame'ы
        if all_dataframes:
            combined_df = pl.concat(all_dataframes)
            logger.info(
                f"Combined {len(all_dataframes)} pages into {len(combined_df)} total rows"
            )
            return combined_df
        else:
            return pl.DataFrame()

    async def _extract_offset_pagination(self) -> List[pl.DataFrame]:
        """Пагинация по offset"""
        dataframes = []
        pagination_config = self.config.pagination_config

        offset = 0
        limit = pagination_config.default_limit
        page_count = 0

        while page_count < pagination_config.max_pages:
            params = dict(self.config.params or {})
            params[pagination_config.limit_param] = limit
            params[pagination_config.offset_param] = offset

            logger.debug(
                f"Fetching page {page_count + 1} (offset={offset}, limit={limit})"
            )

            response_data = await self._make_request(self.config.url, params)
            page_df = await self._parse_response(response_data)

            if page_df is None or len(page_df) == 0:
                logger.debug("No more data, stopping pagination")
                break

            dataframes.append(page_df)

            # Если получили меньше данных чем лимит, значит это последняя страница
            if len(page_df) < limit:
                logger.debug("Received less data than limit, stopping pagination")
                break

            offset += limit
            page_count += 1

            # Rate limiting
            await self._apply_rate_limit()

        return dataframes

    async def _extract_page_pagination(self) -> List[pl.DataFrame]:
        """Пагинация по номеру страницы"""
        dataframes = []
        pagination_config = self.config.pagination_config

        page = pagination_config.start_page
        page_size = pagination_config.default_limit
        page_count = 0

        while page_count < pagination_config.max_pages:
            params = dict(self.config.params or {})
            params[pagination_config.page_param] = page
            params[pagination_config.page_size_param] = page_size

            logger.debug(f"Fetching page {page} (size={page_size})")

            response_data = await self._make_request(self.config.url, params)
            page_df = await self._parse_response(response_data)

            if page_df is None or len(page_df) == 0:
                break

            dataframes.append(page_df)

            if len(page_df) < page_size:
                break

            page += 1
            page_count += 1
            await self._apply_rate_limit()

        return dataframes

    async def _extract_cursor_pagination(self) -> List[pl.DataFrame]:
        """Пагинация по cursor"""
        dataframes = []
        pagination_config = self.config.pagination_config

        cursor = None
        page_count = 0

        while page_count < pagination_config.max_pages:
            params = dict(self.config.params or {})
            if cursor:
                params[pagination_config.cursor_param] = cursor

            logger.debug(f"Fetching page {page_count + 1} (cursor={cursor})")

            response_data = await self._make_request(self.config.url, params)

            # Парсим ответ
            page_df = await self._parse_response(response_data)

            if page_df is None or len(page_df) == 0:
                break

            dataframes.append(page_df)

            # Извлекаем следующий cursor
            if isinstance(response_data, dict):
                cursor = self._get_nested_value(
                    response_data, pagination_config.next_cursor_path
                )
                if not cursor:
                    break
            else:
                break

            page_count += 1
            await self._apply_rate_limit()

        return dataframes

    async def _extract_link_header_pagination(self) -> List[pl.DataFrame]:
        """Пагинация по Link заголовку"""
        dataframes = []
        pagination_config = self.config.pagination_config

        url = self.config.url
        page_count = 0

        while page_count < pagination_config.max_pages:
            logger.debug(f"Fetching page {page_count + 1} from: {url}")

            response, headers = await self._make_request_with_headers(
                url, self.config.params
            )
            page_df = await self._parse_response(response)

            if page_df is None or len(page_df) == 0:
                break

            dataframes.append(page_df)

            # Ищем следующую ссылку в Link заголовке
            link_header = headers.get(pagination_config.link_header.lower())
            next_url = self._parse_link_header(link_header)

            if not next_url:
                break

            url = next_url
            page_count += 1
            await self._apply_rate_limit()

        return dataframes

    async def _make_request(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Выполнение HTTP запроса"""
        response, _ = await self._make_request_with_headers(url, params)
        return response

    async def _make_request_with_headers(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> tuple:
        """Выполнение HTTP запроса с возвратом заголовков"""

        # Применяем rate limiting
        await self._apply_rate_limit()

        # Проверяем кэш
        cache_key = self._generate_cache_key(url, params)
        if self.config.cache_responses and cache_key in self._cache:
            cache_entry = self._cache[cache_key]
            if datetime.now() < cache_entry["expires"]:
                logger.debug(f"Using cached response for {url}")
                return cache_entry["data"], cache_entry["headers"]

        headers = {**self._auth_headers}

        # Retry механизм
        for attempt in range(self.config.retries + 1):
            try:
                async with self._session.request(
                    self.config.method, url, params=params, headers=headers
                ) as response:
                    # Проверяем статус код
                    if response.status not in self.config.valid_status_codes:
                        if self.config.ignore_http_errors:
                            logger.warning(f"HTTP {response.status} ignored: {url}")
                            return None, {}
                        else:
                            response.raise_for_status()

                    # Читаем ответ
                    if self.config.response_format == "json":
                        data = await response.json()
                    elif self.config.response_format == "xml":
                        text = await response.text(encoding=self.config.encoding)
                        data = text
                    elif self.config.response_format == "csv":
                        text = await response.text(encoding=self.config.encoding)
                        data = text
                    else:
                        data = await response.text(encoding=self.config.encoding)

                    response_headers = dict(response.headers)

                    # Кэшируем ответ
                    if self.config.cache_responses:
                        self._cache[cache_key] = {
                            "data": data,
                            "headers": response_headers,
                            "expires": datetime.now()
                            + timedelta(seconds=self.config.cache_ttl),
                        }

                    logger.debug(f"HTTP {response.status} from {url}")
                    return data, response_headers

            except Exception as e:
                if attempt < self.config.retries:
                    delay = self.config.retry_delay * (
                        2**attempt
                    )  # Экспоненциальная задержка
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}), retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Request failed after {self.config.retries + 1} attempts: {e}"
                    )
                    raise

    async def _parse_response(self, response_data: Any) -> Optional[pl.DataFrame]:
        """Парсинг ответа в DataFrame"""
        if response_data is None:
            return None

        try:
            if self.config.response_format == "json":
                return await self._parse_json_response(response_data)
            elif self.config.response_format == "xml":
                return await self._parse_xml_response(response_data)
            elif self.config.response_format == "csv":
                return await self._parse_csv_response(response_data)
            else:
                # Text format - преобразуем в простой DataFrame
                return pl.DataFrame({"content": [response_data]})

        except Exception as e:
            logger.error(f"Failed to parse response: {e}")
            return None

    async def _parse_json_response(self, json_data: Dict[str, Any]) -> pl.DataFrame:
        """Парсинг JSON ответа"""

        # Извлекаем данные по указанному пути
        if self.config.json_data_path:
            data = self._get_nested_value(json_data, self.config.json_data_path)
        else:
            data = json_data

        # Если данные не список, делаем их списком
        if not isinstance(data, list):
            if isinstance(data, dict):
                data = [data]
            else:
                return pl.DataFrame({"value": [data]})

        if not data:
            return pl.DataFrame()

        # Создаем DataFrame
        df = pl.DataFrame(data)

        # Выравниваем JSON если нужно
        if self.config.json_flatten:
            df = self._flatten_json_columns(df)

        return df

    async def _parse_xml_response(self, xml_text: str) -> pl.DataFrame:
        """Парсинг XML ответа"""
        try:
            root = ET.fromstring(xml_text)

            # Определяем записи для парсинга
            if self.config.xml_record_tag:
                records = root.findall(f".//{self.config.xml_record_tag}")
            else:
                # Берем дочерние элементы корня
                records = list(root)

            # Извлекаем данные из каждой записи
            data = []
            for record in records:
                record_data = {}
                for child in record:
                    tag_name = child.tag
                    # Убираем namespace если есть
                    if "}" in tag_name:
                        tag_name = tag_name.split("}")[1]
                    record_data[tag_name] = child.text
                data.append(record_data)

            return pl.DataFrame(data) if data else pl.DataFrame()

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return pl.DataFrame()

    async def _parse_csv_response(self, csv_text: str) -> pl.DataFrame:
        """Парсинг CSV ответа"""
        try:
            import io

            csv_buffer = io.StringIO(csv_text)
            return pl.read_csv(csv_buffer)
        except Exception as e:
            logger.error(f"CSV parsing error: {e}")
            return pl.DataFrame()

    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Получение значения по вложенному пути (например: data.items.0.name)"""
        if not path:
            return data

        keys = path.split(".")
        current = data

        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list) and key.isdigit():
                idx = int(key)
                current = current[idx] if idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current

    def _flatten_json_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Выравнивание JSON колонок в DataFrame"""
        # Простая реализация - можно расширить
        return df

    def _parse_link_header(self, link_header: Optional[str]) -> Optional[str]:
        """Парсинг Link заголовка для получения следующей ссылки"""
        if not link_header:
            return None

        # Простой парсер Link заголовка
        # Формат: <https://api.example.com/data?page=2>; rel="next"
        links = link_header.split(",")
        for link in links:
            if 'rel="next"' in link:
                url_part = link.split(";")[0].strip()
                if url_part.startswith("<") and url_part.endswith(">"):
                    return url_part[1:-1]
        return None

    async def _apply_rate_limit(self):
        """Применение rate limiting"""
        if not self.config.rate_limit:
            return

        if self._last_request_time:
            time_since_last = datetime.now() - self._last_request_time
            min_interval = timedelta(seconds=1.0 / self.config.rate_limit)

            if time_since_last < min_interval:
                sleep_time = (min_interval - time_since_last).total_seconds()
                await asyncio.sleep(sleep_time)

        self._last_request_time = datetime.now()

    def _generate_cache_key(self, url: str, params: Optional[Dict[str, Any]]) -> str:
        """Генерация ключа для кэша"""
        import hashlib

        params_str = json.dumps(params or {}, sort_keys=True)
        cache_string = f"{url}:{params_str}"
        return hashlib.md5(cache_string.encode()).hexdigest()

    @property
    def info(self) -> Info:
        return Info(
            name="HTTPExtract",
            version="1.0.0",
            description="Извлечение данных из REST API с поддержкой аутентификации и пагинации",
            type_class=self.__class__,
            type_module="extract",
            config_class=HTTPExtractConfig,
        )
