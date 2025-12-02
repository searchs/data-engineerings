"""
Azure JSON to Parquet Data Processor using Protocol (Structural Subtyping)
Handles schema inference and explicit schemas with configurable retry logic
"""

from typing import Protocol, Optional, Dict, Any, runtime_checkable
from dataclasses import dataclass
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
# Protocols (Structural Subtyping - Duck Typing with Type Safety)
# ============================================================================


@runtime_checkable
class SchemaProvider(Protocol):
    """
    Protocol for schema providers (structural subtyping)
    Any class implementing get_schema() is automatically a SchemaProvider
    No explicit inheritance needed!
    """

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Get PyArrow schema for the data"""
        ...


@runtime_checkable
class DataReader(Protocol):
    """
    Protocol for data readers
    Any class implementing read() is automatically a DataReader
    """

    def read(self, source: Any) -> pd.DataFrame:
        """Read data from source and return DataFrame"""
        ...


# ============================================================================
# Schema Management (Single Responsibility Principle)
# ============================================================================


class InferredSchemaProvider:
    """Infers schema from the data automatically"""

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Infer schema from pandas DataFrame"""
        logger.info("Inferring schema from data")
        return pa.Schema.from_pandas(data)


class ExplicitSchemaProvider:
    """Uses an explicitly provided schema"""

    def __init__(self, schema: pa.Schema):
        self.schema = schema

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Return the explicit schema"""
        logger.info("Using explicit schema")
        return self.schema


class DictSchemaProvider:
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


class ValidationSchemaProvider:
    """
    Schema provider that validates data matches expected schema
    Example of Protocol flexibility - no inheritance needed!
    """

    def __init__(self, expected_columns: set[str]):
        self.expected_columns = expected_columns

    def get_schema(self, data: pd.DataFrame) -> pa.Schema:
        """Validate columns and infer schema"""
        logger.info("Validating schema")
        missing = self.expected_columns - set(data.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        return pa.Schema.from_pandas(data)


# ============================================================================
# Data Reading (Single Responsibility Principle)
# ============================================================================


class JSONFileReader:
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


class JSONStringReader:
    """Reads JSON data from string"""

    def read(self, source: str) -> pd.DataFrame:
        """Parse JSON string into DataFrame"""
        logger.info("Reading JSON from string")
        data = json.loads(source)

        if isinstance(data, dict):
            data = [data]

        return pd.DataFrame(data)


class JSONLinesReader:
    """Reads newline-delimited JSON (JSONL)"""

    def read(self, source: str) -> pd.DataFrame:
        """Read JSONL file into DataFrame"""
        logger.info(f"Reading JSONL from file: {source}")
        return pd.read_json(source, lines=True)


class AzureBlobReader:
    """
    Example of Protocol flexibility - can add new readers without modifying Protocol
    This automatically satisfies DataReader Protocol
    """

    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name

    def read(self, source: str) -> pd.DataFrame:
        """Read JSON from Azure Blob Storage"""
        logger.info(f"Reading from Azure Blob: {self.container_name}/{source}")
        # Placeholder for actual Azure implementation
        # from azure.storage.blob import BlobServiceClient
        # blob_service = BlobServiceClient.from_connection_string(self.connection_string)
        # blob_client = blob_service.get_blob_client(self.container_name, source)
        # data = json.loads(blob_client.download_blob().readall())
        raise NotImplementedError(
            "Azure Blob reading requires azure-storage-blob package"
        )


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
# Data Processor (Dependency Inversion with Protocols)
# ============================================================================


class DataProcessor:
    """
    Main data processor that converts JSON to Parquet
    Uses Protocols for structural subtyping - more flexible than ABC!
    """

    def __init__(
        self,
        reader: DataReader,
        schema_provider: SchemaProvider,
        config: Optional[ProcessorConfig] = None,
    ):
        """
        Args:
            reader: Any object implementing read() method
            schema_provider: Any object implementing get_schema() method
            config: Processor configuration
        """
        # Runtime validation that objects conform to protocols
        if not isinstance(reader, DataReader):
            raise TypeError(
                f"{type(reader).__name__} does not implement DataReader protocol"
            )
        if not isinstance(schema_provider, SchemaProvider):
            raise TypeError(
                f"{type(schema_provider).__name__} does not implement SchemaProvider protocol"
            )

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

    @staticmethod
    def create_with_validation(
        reader: DataReader,
        expected_columns: set[str],
        config: Optional[ProcessorConfig] = None,
    ) -> DataProcessor:
        """Create processor with column validation"""
        return DataProcessor(
            reader=reader,
            schema_provider=ValidationSchemaProvider(expected_columns),
            config=config,
        )


# ============================================================================
# Custom Implementation Example - Shows Protocol Flexibility
# ============================================================================


class CustomTransformingReader:
    """
    Custom reader that also transforms data
    Automatically satisfies DataReader Protocol without inheritance!
    """

    def __init__(self, base_reader: DataReader, transform_func):
        self.base_reader = base_reader
        self.transform_func = transform_func

    def read(self, source: Any) -> pd.DataFrame:
        """Read and transform data"""
        df = self.base_reader.read(source)
        return self.transform_func(df)


# ============================================================================
# Usage Examples
# ============================================================================

if __name__ == "__main__":
    # Example 1: Basic usage with inferred schema
    print("\n=== Example 1: Inferred Schema ===")
    config = ProcessorConfig(max_retries=3, retry_delay_seconds=1.0)
    processor = DataProcessorFactory.create_with_inferred_schema(
        reader=JSONFileReader(), config=config
    )
    # processor.process("input.json", "output.parquet")

    # Example 2: Explicit schema dictionary
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

    # Example 3: Schema validation
    print("\n=== Example 3: Schema Validation ===")
    processor = DataProcessorFactory.create_with_validation(
        reader=JSONFileReader(), expected_columns={"id", "name", "value"}
    )
    # processor.process("input.json", "output.parquet")

    # Example 4: Custom transforming reader (Protocol flexibility!)
    print("\n=== Example 4: Custom Transforming Reader ===")

    def add_processing_timestamp(df: pd.DataFrame) -> pd.DataFrame:
        df["processed_at"] = pd.Timestamp.now()
        return df

    custom_reader = CustomTransformingReader(
        base_reader=JSONFileReader(), transform_func=add_processing_timestamp
    )
    processor = DataProcessorFactory.create_with_inferred_schema(reader=custom_reader)
    # processor.process("input.json", "output.parquet")

    # Example 5: Demonstrate Protocol checking
    print("\n=== Example 5: Protocol Type Checking ===")
    print(
        f"JSONFileReader implements DataReader: {isinstance(JSONFileReader(), DataReader)}"
    )
    print(
        f"InferredSchemaProvider implements SchemaProvider: {isinstance(InferredSchemaProvider(), SchemaProvider)}"
    )
    print(
        f"CustomTransformingReader implements DataReader: {isinstance(custom_reader, DataReader)}"
    )

    print("\nAll examples configured successfully!")
    print("\nProtocol Benefits:")
    print("✓ No explicit inheritance needed")
    print("✓ Structural subtyping (duck typing with safety)")
    print("✓ Easy to add new implementations")
    print("✓ Runtime validation available with @runtime_checkable")
    print("✓ More Pythonic and flexible than ABC")
