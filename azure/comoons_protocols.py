"""Azure JSON-to-Parquet example using Protocol-based structural subtyping."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ProcessorConfig:
    """Runtime configuration for the data processor."""

    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    compression: str = "snappy"
    row_group_size: int | None = None

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")


@runtime_checkable
class SchemaProvider(Protocol):
    """Structural contract for schema providers."""

    def get_schema(self, data: pd.DataFrame) -> pa.Schema: ...


@runtime_checkable
class DataReader(Protocol):
    """Structural contract for dataframe readers."""

    def read(self, source: Any) -> pd.DataFrame: ...


class InferredSchemaProvider:
    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        logger.info("Inferring schema from data")
        return pa.Schema.from_pandas(data)


class ExplicitSchemaProvider:
    def __init__(self, schema: pa.Schema) -> None:
        self.schema = schema

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        logger.info("Using explicit schema")
        return self.schema


class DictSchemaProvider:
    def __init__(self, schema_dict: dict[str, str]) -> None:
        self.schema_dict = schema_dict
        self._type_mapping = {
            "int32": pa.int32(),
            "int64": pa.int64(),
            "float32": pa.float32(),
            "float64": pa.float64(),
            "string": pa.string(),
            "bool": pa.bool_(),
            "timestamp": pa.timestamp("ms"),
            "date": pa.date32(),
        }

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        fields: list[pa.Field] = []
        for column, type_name in self.schema_dict.items():
            try:
                arrow_type = self._type_mapping[type_name]
            except KeyError as exc:
                raise ValueError(f"Unsupported type: {type_name}") from exc
            fields.append(pa.field(column, arrow_type))
        return pa.schema(fields)


class ValidationSchemaProvider:
    def __init__(self, expected_columns: set[str]) -> None:
        self.expected_columns = expected_columns

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        missing = self.expected_columns - set(data.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        return pa.Schema.from_pandas(data)


class JSONFileReader:
    def read(self, source: str) -> pd.DataFrame:
        logger.info("Reading JSON from file: %s", source)
        with open(source) as file_handle:
            payload = json.load(file_handle)
        return pd.DataFrame(payload if isinstance(payload, list) else [payload])


class JSONStringReader:
    def read(self, source: str) -> pd.DataFrame:
        payload = json.loads(source)
        return pd.DataFrame(payload if isinstance(payload, list) else [payload])


class JSONLinesReader:
    def read(self, source: str) -> pd.DataFrame:
        return pd.read_json(source, lines=True)


class AzureBlobReader:
    """Placeholder reader demonstrating Protocol extension without inheritance."""

    def __init__(self, connection_string: str, container_name: str) -> None:
        self.connection_string = connection_string
        self.container_name = container_name

    def read(self, source: str) -> pd.DataFrame:
        raise NotImplementedError(
            "Azure Blob reading requires an azure-storage-blob adapter"
        )


class RetryHandler:
    def __init__(self, config: ProcessorConfig) -> None:
        self.config = config

    def execute_with_retry(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        last_exception: Exception | None = None

        for attempt in range(self.config.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as exc:
                last_exception = exc
                if attempt < self.config.max_retries:
                    logger.warning(
                        "Attempt %s failed: %s. Retrying in %ss...",
                        attempt + 1,
                        exc,
                        self.config.retry_delay_seconds,
                    )
                    time.sleep(self.config.retry_delay_seconds)

        raise last_exception or RuntimeError("Operation failed without an exception")


class DataProcessor:
    """Convert JSON-like sources to Parquet using Protocol-based dependencies."""

    def __init__(
        self,
        reader: DataReader,
        schema_provider: SchemaProvider,
        config: ProcessorConfig | None = None,
    ) -> None:
        if not isinstance(reader, DataReader):
            raise TypeError(f"{type(reader).__name__} does not implement DataReader")
        if not isinstance(schema_provider, SchemaProvider):
            raise TypeError(
                f"{type(schema_provider).__name__} does not implement SchemaProvider"
            )

        self.reader = reader
        self.schema_provider = schema_provider
        self.config = config or ProcessorConfig()
        self.retry_handler = RetryHandler(self.config)

    def _convert_to_parquet(self, frame: pd.DataFrame, output_path: str) -> None:
        schema = self.schema_provider.get_schema(frame)
        table = pa.Table.from_pandas(frame, schema=schema)
        pq.write_table(
            table,
            output_path,
            compression=self.config.compression,
            row_group_size=self.config.row_group_size,
        )

    def process(self, source: Any, output_path: str) -> None:
        frame = self.retry_handler.execute_with_retry(self.reader.read, source)
        self.retry_handler.execute_with_retry(
            self._convert_to_parquet, frame, output_path
        )


class DataProcessorFactory:
    @staticmethod
    def inferred(
        reader: DataReader, config: ProcessorConfig | None = None
    ) -> DataProcessor:
        return DataProcessor(reader, InferredSchemaProvider(), config)

    @staticmethod
    def explicit(
        reader: DataReader,
        schema: pa.Schema,
        config: ProcessorConfig | None = None,
    ) -> DataProcessor:
        return DataProcessor(reader, ExplicitSchemaProvider(schema), config)

    @staticmethod
    def from_dict(
        reader: DataReader,
        schema_dict: dict[str, str],
        config: ProcessorConfig | None = None,
    ) -> DataProcessor:
        return DataProcessor(reader, DictSchemaProvider(schema_dict), config)

    @staticmethod
    def validating(
        reader: DataReader,
        expected_columns: set[str],
        config: ProcessorConfig | None = None,
    ) -> DataProcessor:
        return DataProcessor(reader, ValidationSchemaProvider(expected_columns), config)


class CustomTransformingReader:
    """Decorate any DataReader with a dataframe transformation."""

    def __init__(
        self,
        base_reader: DataReader,
        transform: Callable[[pd.DataFrame], pd.DataFrame],
    ) -> None:
        self.base_reader = base_reader
        self.transform = transform

    def read(self, source: Any) -> pd.DataFrame:
        return self.transform(self.base_reader.read(source))
