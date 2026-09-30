# Apache Beam attendance pipeline

Curated from the legacy `attendance_pipeline` repository.

The original file mixed several unrelated notebook-style experiments in one Python module. This version keeps the core attendance-counting lesson and expresses it as a small Beam pipeline.

```python
import apache_beam as beam


def parse_row(line: str) -> list[str]:
    return [part.strip() for part in line.split(",")]


def is_department(row: list[str], department: str) -> bool:
    return len(row) > 3 and row[3] == department


with beam.Pipeline() as pipeline:
    rows = (
        pipeline
        | "Read attendance data" >> beam.io.ReadFromText("dept-data.txt")
        | "Parse CSV-like rows" >> beam.Map(parse_row)
    )

    accounts = (
        rows
        | "Accounts only" >> beam.Filter(is_department, "Accounts")
        | "Pair employee" >> beam.Map(lambda row: (row[1], 1))
        | "Count attendance" >> beam.CombinePerKey(sum)
        | "Write result" >> beam.io.WriteToText("output/accounts")
    )
```

## Durable Beam concepts

- model parsing and filtering as separate transforms;
- prefer `CombinePerKey(sum)` to a manual `GroupByKey` followed by summation;
- use named transforms so the execution graph is readable;
- branch a `PCollection` when several downstream calculations share the same parsed input;
- keep external data acquisition separate from transformation logic.

## Intentionally omitted

The legacy source also contained notebook shell commands, Wikipedia pagecount experiments and environment-specific files in the same module. Those were not carried into the canonical lab because they represent separate concerns and made the original script non-importable as normal Python.
