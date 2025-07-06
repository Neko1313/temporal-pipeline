import ast
import asyncio
import io
import logging
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import aiofiles
import aiohttp
import polars as pl

from core.component import BaseProcessClass, Info, Result
from csv_processor.config import CSVExtractConfig

logger = logging.getLogger(__name__)


class CSVExtract(BaseProcessClass):
    """
    CSV Extract компонент для извлечения данных из CSV файлов

    Поддерживаемые источники:
    - Локальные файлы (file://, относительные пути)
    - HTTP/HTTPS (http://, https://)
    - S3 (s3://)
    - FTP (ftp://)

    Функции:
    - Автоопределение разделителей
    - Потоковое чтение больших файлов
    - Гибкая фильтрация и трансформация
    - Обработка различных кодировок
    """

    config: CSVExtractConfig

    async def process(self) -> Result:
        """Основной метод обработки"""
        try:
            logger.info(
                "Starting CSV extraction from: %s",
                self.config.source_config.path,
            )

            # Получаем данные из источника
            csv_content = await self._fetch_csv_content()

            # Парсим CSV
            if self.config.streaming:
                data = await self._parse_csv_streaming(csv_content)
            else:
                data = await self._parse_csv_batch(csv_content)

            # Постобработка
            if data is not None and len(data) > 0:
                data = self._postprocess_data(data)

                logger.info(
                    "Successfully extracted %s rows with %s columns",
                    len(data),
                    len(data.columns),
                )

                return Result(status="success", response=data)
            logger.warning(
                "CSV file is empty or no data matches filter conditions"
            )
            return Result(status="success", response=pl.DataFrame())

        except Exception as e:
            logger.error(f"CSV extraction failed: {e!s}")
            return Result(status="error", response=None)

    async def _fetch_csv_content(self) -> str | bytes:
        """Получение содержимого CSV из различных источников"""
        path = self.config.source_config.path
        parsed_url = urlparse(path)

        if parsed_url.scheme in ["http", "https"]:
            return await self._fetch_http_csv()
        if parsed_url.scheme == "s3":
            return await self._fetch_s3_csv()
        if parsed_url.scheme == "ftp":
            return await self._fetch_ftp_csv()
        # Локальный файл
        return await self._fetch_local_csv()

    async def _fetch_local_csv(self) -> str:
        """Чтение локального CSV файла"""
        file_path = Path(self.config.source_config.path)

        if not file_path.exists():
            msg = f"CSV file not found: {file_path}"
            raise FileNotFoundError(msg)

        async with aiofiles.open(
            file_path, encoding=self.config.source_config.encoding
        ) as f:
            content = await f.read()

        logger.debug(f"Read {len(content)} characters from local file")
        return content

    async def _fetch_http_csv(self) -> str:
        """Скачивание CSV файла по HTTP/HTTPS"""
        headers = self.config.source_config.headers or {}
        timeout = aiohttp.ClientTimeout(
            total=self.config.source_config.timeout
        )

        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.get(
                self.config.source_config.path, headers=headers
            ) as response,
        ):
            response.raise_for_status()

            content = await response.text(
                encoding=self.config.source_config.encoding
            )
            logger.debug(
                f"Downloaded {len(content)} characters from HTTP source"
            )
            return content

    async def _fetch_s3_csv(self) -> str:
        """Загрузка CSV файла из S3"""
        try:
            import boto3  # noqa: PLC0415
            from botocore.exceptions import (  # noqa: PLC0415
                ClientError,
                NoCredentialsError,
            )
        except ImportError as ie:
            msg = "boto3 library is required for S3 support.\
            Install it with: pip install boto3"
            raise ImportError(msg) from ie

        parsed_url = urlparse(self.config.source_config.path)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip("/")

        # Настройка S3 клиента
        session = boto3.Session(
            aws_access_key_id=self.config.source_config.aws_access_key,
            aws_secret_access_key=self.config.source_config.aws_secret_key,
            region_name=self.config.source_config.aws_region,
        )

        s3_client = session.client("s3")

        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = (
                response["Body"]
                .read()
                .decode(self.config.source_config.encoding)
            )
            logger.debug(
                f"Downloaded {len(content)} characters from S3: s3://{bucket}/{key}"
            )
            return content

        except (NoCredentialsError, ClientError) as ex:
            msg = f"S3 access error: {ex}"
            raise Exception(msg) from ex

    async def _fetch_ftp_csv(self) -> str:
        """Загрузка CSV файла по FTP"""
        try:
            import aioftp  # noqa: PLC0415
        except ImportError as ie:
            msg = "aioftp library is required for FTP support.\
            Install it with: pip install aioftp"
            raise ImportError(msg) from ie

        parsed_url = urlparse(self.config.source_config.path)
        host = parsed_url.hostname
        port = parsed_url.port or 21
        username = parsed_url.username or "anonymous"
        password = parsed_url.password or ""
        file_path = parsed_url.path

        async with aioftp.Client() as client:
            await client.connect(host, port)
            await client.login(username, password)

            with tempfile.NamedTemporaryFile(
                mode="w+b", delete=False
            ) as temp_file:
                await client.download(file_path, temp_file.name)

                # Читаем содержимое
                async with aiofiles.open(
                    temp_file.name,
                    encoding=self.config.source_config.encoding,
                ) as f:
                    content = await f.read()

                # Удаляем временный файл
                Path(temp_file.name).unlink()

                logger.debug(
                    "Downloaded %s characters from FTP: %s",
                    len(content),
                    self.config.source_config.path,
                )
                return content

    async def _parse_csv_batch(self, content: str) -> pl.DataFrame:
        """Парсинг CSV в batch режиме"""

        # Создаем StringIO объект для polars
        csv_buffer = io.StringIO(content)

        # Настройки для polars
        read_options = {
            "separator": self.config.delimiter,
            "quote_char": self.config.quote_char,
            "has_header": self.config.has_header,
            "skip_rows": self.config.skip_rows,
            "encoding": self.config.source_config.encoding,
            "ignore_errors": self.config.ignore_errors,
            "null_values": self.config.null_values,
        }

        # Добавляем лимит строк если указан
        if self.config.row_limit:
            read_options["n_rows"] = self.config.row_limit

        # Указываем колонки если нужно
        if self.config.columns:
            read_options["columns"] = self.config.columns

        # Читаем CSV
        try:
            df = pl.read_csv(csv_buffer, **read_options)
            logger.debug(
                f"Parsed CSV: {len(df)} rows, {len(df.columns)} columns"
            )
            return df

        except Exception as ex:
            if self.config.ignore_errors:
                logger.warning(f"CSV parsing error (ignored): {ex}")
                return pl.DataFrame()
            msg = f"CSV parsing failed: {ex}"
            raise Exception(msg) from ex

    async def _parse_csv_streaming(self, content: str) -> pl.DataFrame:
        """Потоковый парсинг CSV для больших файлов"""

        # Для демонстрации - читаем батчами
        _ = io.StringIO(content)
        chunks = []

        # Читаем первый чанк чтобы получить схему
        lines = content.split("\n")
        _ = 1 if self.config.has_header else 0

        current_line = 0
        while current_line < len(lines):
            # Берем батч строк
            batch_lines = lines[
                current_line : current_line + self.config.batch_size
            ]

            if not batch_lines or (
                len(batch_lines) == 1 and not batch_lines[0].strip()
            ):
                break

            # Если это не первый батч, добавляем заголовок
            if current_line > 0 and self.config.has_header:
                batch_content = lines[0] + "\n" + "\n".join(batch_lines)
            else:
                batch_content = "\n".join(batch_lines)

            # Парсим батч
            batch_buffer = io.StringIO(batch_content)

            read_options = {
                "separator": self.config.delimiter,
                "quote_char": self.config.quote_char,
                "has_header": self.config.has_header
                if current_line == 0
                else True,
                "encoding": self.config.source_config.encoding,
                "ignore_errors": self.config.ignore_errors,
                "null_values": self.config.null_values,
            }

            try:
                chunk_df = pl.read_csv(batch_buffer, **read_options)
                if len(chunk_df) > 0:
                    chunks.append(chunk_df)
                    logger.debug(f"Processed batch: {len(chunk_df)} rows")

            except Exception as ex:
                if not self.config.ignore_errors:
                    msg = f"Streaming CSV parsing\
                    failed at line {current_line}: {ex}"
                    raise Exception(msg) from ex
                logger.warning(
                    "Skipped batch at line %s due to error: %s",
                    current_line,
                    ex,
                )

            current_line += self.config.batch_size

            # Небольшая пауза для асинхронности
            await asyncio.sleep(0.001)

        # Объединяем все чанки
        if chunks:
            result_df = pl.concat(chunks)
            logger.debug(
                f"Streaming parse complete: {len(result_df)} total rows"
            )
            return result_df
        return pl.DataFrame()

    def _postprocess_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """Постобработка данных"""
        # Переименование колонок
        if self.config.column_mapping:
            for old_name, new_name in self.config.column_mapping.items():
                if old_name in data.columns:
                    data = data.rename({old_name: new_name})

        # Приведение типов данных
        if self.config.data_types:
            for column, dtype in self.config.data_types.items():
                if column in data.columns:
                    try:
                        if dtype.lower() == "int":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Int64)
                            )
                        elif dtype.lower() == "float":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Float64)
                            )
                        elif dtype.lower() == "str":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Utf8)
                            )
                        elif dtype.lower() == "bool":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Boolean)
                            )
                        elif dtype.lower() == "date":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Date)
                            )
                        elif dtype.lower() == "datetime":
                            data = data.with_columns(
                                pl.col(column).cast(pl.Datetime)
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to cast column {column} to {dtype}: {e}"
                        )

        # Применение фильтра
        if self.config.filter_condition:
            try:
                # Безопасная оценка условия
                data = data.filter(
                    ast.literal_eval(self.config.filter_condition)
                )
                logger.debug(
                    "Applied filter: %s", self.config.filter_condition
                )
            except Exception as ex:
                logger.warning(
                    "Failed to apply filter '%s': %s",
                    self.config.filter_condition,
                    ex,
                )

        # Лимит строк (если не был применен при чтении)
        if self.config.row_limit and len(data) > self.config.row_limit:
            data = data.head(self.config.row_limit)

        return data

    @property
    def info(self) -> Info:
        return Info(
            name="CSVExtract",
            version="1.0.0",
            description="Извлечение данных из CSV\
            файлов с поддержкой различных источников",
            type_class=self.__class__,
            type_module="extract",
            config_class=CSVExtractConfig,
        )
