I'll create a robust, SOLID-compliant data processor for converting JSON to Parquet with configurable retry mechanisms.I've created a robust, SOLID-compliant data processor for converting JSON to Parquet. Here's how it implements SOLID principles:

## **SOLID Principles Applied:**

1. **Single Responsibility** - Each class has one job:
   - `ProcessorConfig` - Configuration management
   - `SchemaProvider` - Schema handling
   - `DataReader` - Data reading
   - `RetryHandler` - Retry logic

2. **Open/Closed** - Extensible without modification:
   - Add new `SchemaProvider` implementations (e.g., `AvroSchemaProvider`)
   - Add new `DataReader` implementations (e.g., `AzureBlobReader`)

3. **Liskov Substitution** - All implementations are interchangeable

4. **Interface Segregation** - Focused abstract base classes

5. **Dependency Inversion** - `DataProcessor` depends on abstractions, not concrete classes

## **Key Features:**

- ✅ **Configurable retry mechanism** (default 3 retries)
- ✅ **Schema inference or explicit schema**
- ✅ **Multiple JSON formats** (file, string, JSONL)
- ✅ **Comprehensive logging**
- ✅ **Easy extension** via abstract base classes
- ✅ **Factory pattern** for convenient instantiation

## **Usage:**

```python
# With inferred schema
config = ProcessorConfig(max_retries=3)
processor = DataProcessorFactory.create_with_inferred_schema(
    reader=JSONFileReader(),
    config=config
)
processor.process("input.json", "output.parquet")

# With explicit schema
schema_dict = {'id': 'int64', 'name': 'string'}
processor = DataProcessorFactory.create_with_dict_schema(
    reader=JSONFileReader(),
    schema_dict=schema_dict
)
```

The design makes it easy to add Azure-specific readers or custom schema providers without modifying existing code!


Excellent question! I've refactored the code to use `Protocol` instead of `ABC`. Here are the key advantages:

## **Protocol vs ABC - Key Differences:**

### **1. Structural Subtyping (Duck Typing)**
- **Protocol**: Classes don't need explicit inheritance - if they have the right methods, they automatically satisfy the protocol
- **ABC**: Requires explicit inheritance with `class MyClass(AbstractBase)`

### **2. More Pythonic**
```python
# With Protocol - just implement the method!
class MyReader:
    def read(self, source) -> pd.DataFrame:
        return pd.DataFrame()

# Automatically satisfies DataReader Protocol!
```

### **3. Flexibility**
- Add `ValidationSchemaProvider` and `CustomTransformingReader` examples showing you can create new implementations without touching the Protocol
- No inheritance hierarchy to manage
- Easier testing with mock objects

### **4. Runtime Validation**
```python
@runtime_checkable  # Enables isinstance() checks
class DataReader(Protocol):
    def read(self, source: Any) -> pd.DataFrame: ...
```

## **Benefits for Your Azure Use Case:**

1. **Easy Extension**: Add Azure-specific readers without modifying base code
2. **Loose Coupling**: Components are even more decoupled
3. **Better Testing**: Can create simple test doubles without inheritance
4. **Type Safety**: Still get full type checking from mypy/pyright
5. **Composition**: Easier to compose behaviors (see `CustomTransformingReader`)

The Protocol approach is generally preferred in modern Python for interfaces, especially when building flexible, extensible systems like your data processor!
