"""
Azure JSON to Parquet Data Processor with SOLID Principles
Handles schema inference and explicit schemas with configurable retry logic
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import json
import logging
from pathlib import Path
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration (Single Responsibility Principle)
# ============================================================================


@dataclass
class ProcessorConfig:
    """Configuration for the data processor"""

    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    compression: str = "snappy"
    row_group_size: Optional[int] = None

    def __post_init__(self):
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")


# ============================================================================
# Schema Management (Single Responsibility Principle)
# ============================================================================


class SchemaProvider(ABC):
    """Abstract base class for schema providers (Open/Closed Principle)"""

    @abstractmethod
    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Get PyArrow schema for the data"""
        pass


class InferredSchemaProvider(SchemaProvider):
    """Infers schema from the data automatically"""

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Infer schema from pandas DataFrame"""
        logger.info("Inferring schema from data")
        return pa.Schema.from_pandas(data)


class ExplicitSchemaProvider(SchemaProvider):
    """Uses an explicitly provided schema"""

    def __init__(self, schema: pa.Schema):
        self.schema = schema

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Return the explicit schema"""
        logger.info("Using explicit schema")
        return self.schema


class DictSchemaProvider(SchemaProvider):
    """Builds schema from a dictionary specification"""

    def __init__(self, schema_dict: Dict[str, str]):
        """
        Args:
            schema_dict: Dict mapping column names to PyArrow type strings
                        e.g., {'id': 'int64', 'name': 'string', 'value': 'float64'}
        """
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
        """Build PyArrow schema from dictionary"""
        logger.info("Building schema from dictionary")
        fields = []
        for col_name, type_str in self.schema_dict.items():
            if type_str not in self._type_mapping:
                raise ValueError(f"Unsupported type: {type_str}")
            fields.append(pa.field(col_name, self._type_mapping[type_str]))
        return pa.schema(fields)


# ============================================================================
# Data Reading (Single Responsibility Principle)
# ============================================================================


class DataReader(ABC):
    """Abstract base class for data readers"""

    @abstractmethod
    def read(self, source: Any) -> pd.DataFrame:
        """Read data from source and return DataFrame"""
        pass


class JSONFileReader(DataReader):
    """Reads JSON data from file"""

    def read(self, source: str) -> pd.DataFrame:
        """Read JSON file into DataFrame"""
        logger.info(f"Reading JSON from file: {source}")
        with open(source, "r") as f:
            data = json.load(f)

        # Handle both single object and array of objects
        if isinstance(data, dict):
            data = [data]

        return pd.DataFrame(data)


class JSONStringReader(DataReader):
    """Reads JSON data from string"""

    def read(self, source: str) -> pd.DataFrame:
        """Parse JSON string into DataFrame"""
        logger.info("Reading JSON from string")
        data = json.loads(source)

        if isinstance(data, dict):
            data = [data]

        return pd.DataFrame(data)


class JSONLinesReader(DataReader):
    """Reads newline-delimited JSON (JSONL)"""

    def read(self, source: str) -> pd.DataFrame:
        """Read JSONL file into DataFrame"""
        logger.info(f"Reading JSONL from file: {source}")
        return pd.read_json(source, lines=True)


# ============================================================================
# Retry Mechanism (Single Responsibility Principle)
# ============================================================================


class RetryHandler:
    """Handles retry logic for operations"""

    def __init__(self, config: ProcessorConfig):
        self.config = config

    def execute_with_retry(self, operation, *args, **kwargs):
        """Execute operation with retry logic"""
        last_exception = None

        for attempt in range(self.config.max_retries + 1):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.config.max_retries:
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {str(e)}. "
                        f"Retrying in {self.config.retry_delay_seconds}s..."
                    )
                    time.sleep(self.config.retry_delay_seconds)
                else:
                    logger.error(f"All {self.config.max_retries + 1} attempts failed")

        raise last_exception


# ============================================================================
# Data Processor (Dependency Inversion Principle)
# ============================================================================


class DataProcessor:
    """
    Main data processor that converts JSON to Parquet
    Depends on abstractions (SchemaProvider, DataReader) not concrete implementations
    """

    def __init__(
        self,
        reader: DataReader,
        schema_provider: SchemaProvider,
        config: Optional[ProcessorConfig] = None,
    ):
        """
        Args:
            reader: Data reader implementation
            schema_provider: Schema provider implementation
            config: Processor configuration
        """
        self.reader = reader
        self.schema_provider = schema_provider
        self.config = config or ProcessorConfig()
        self.retry_handler = RetryHandler(self.config)

    def _convert_to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """Internal method to convert DataFrame to Parquet"""
        schema = self.schema_provider.get_schema(df)
        table = pa.Table.from_pandas(df, schema=schema)

        pq.write_table(
            table,
            output_path,
            compression=self.config.compression,
            row_group_size=self.config.row_group_size,
        )

        logger.info(f"Successfully wrote Parquet file: {output_path}")

    def process(self, source: Any, output_path: str) -> None:
        """
        Process data from source and write to Parquet

        Args:
            source: Input source (file path, string, etc.)
            output_path: Output Parquet file path
        """
        logger.info("Starting data processing")

        # Read data with retry
        df = self.retry_handler.execute_with_retry(self.reader.read, source)

        logger.info(f"Read {len(df)} records")

        # Convert to Parquet with retry
        self.retry_handler.execute_with_retry(self._convert_to_parquet, df, output_path)

        logger.info("Processing completed successfully")


# ============================================================================
# Factory (Factory Pattern for easy instantiation)
# ============================================================================


class DataProcessorFactory:
    """Factory for creating configured DataProcessor instances"""

    @staticmethod
    def create_with_inferred_schema(
        reader: DataReader, config: Optional[ProcessorConfig] = None
    ) -> DataProcessor:
        """Create processor with schema inference"""
        return DataProcessor(
            reader=reader, schema_provider=InferredSchemaProvider(), config=config
        )

    @staticmethod
    def create_with_explicit_schema(
        reader: DataReader, schema: pa.Schema, config: Optional[ProcessorConfig] = None
    ) -> DataProcessor:
        """Create processor with explicit schema"""
        return DataProcessor(
            reader=reader, schema_provider=ExplicitSchemaProvider(schema), config=config
        )

    @staticmethod
    def create_with_dict_schema(
        reader: DataReader,
        schema_dict: Dict[str, str],
        config: Optional[ProcessorConfig] = None,
    ) -> DataProcessor:
        """Create processor with dictionary schema"""
        return DataProcessor(
            reader=reader,
            schema_provider=DictSchemaProvider(schema_dict),
            config=config,
        )


# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    # Example 1: Process JSON file with inferred schema
    print("\n=== Example 1: Inferred Schema ===")
    config = ProcessorConfig(max_retries=3, retry_delay_seconds=1.0)
    processor = DataProcessorFactory.create_with_inferred_schema(
        reader=JSONFileReader(), config=config
    )
    # processor.process("input.json", "output.parquet")

    # Example 2: Process with explicit schema dictionary
    print("\n=== Example 2: Explicit Schema from Dict ===")
    schema_dict = {
        "id": "int64",
        "name": "string",
        "value": "float64",
        "timestamp": "timestamp",
    }
    processor = DataProcessorFactory.create_with_dict_schema(
        reader=JSONFileReader(), schema_dict=schema_dict, config=config
    )
    # processor.process("input.json", "output.parquet")

    # Example 3: Process JSON string with custom retry config
    print("\n=== Example 3: JSON String with Custom Retry ===")
    custom_config = ProcessorConfig(max_retries=5, retry_delay_seconds=2.0)
    processor = DataProcessorFactory.create_with_inferred_schema(
        reader=JSONStringReader(), config=custom_config
    )
    json_data = '[{"id": 1, "name": "test", "value": 42.5}]'
    # processor.process(json_data, "output.parquet")

    # Example 4: Process JSONL file
    print("\n=== Example 4: JSONL File ===")
    processor = DataProcessorFactory.create_with_inferred_schema(
        reader=JSONLinesReader()
    )
    # processor.process("input.jsonl", "output.parquet")

    print("\nAll examples configured successfully!")
