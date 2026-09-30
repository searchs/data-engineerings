# Data Engineering Consolidation Plan

This repository is the canonical destination for durable data-engineering labs, case studies and reusable patterns currently spread across older standalone repositories.

## Migration principles

1. Preserve authored implementations that still demonstrate useful engineering practice.
2. Keep explicit attribution for course-derived case studies.
3. Do not migrate generated output, large disposable datasets, `.crc` files, `_SUCCESS` markers, vendored dependencies or build artefacts.
4. Prefer one curated migration commit for small labs; preserve history only where it adds meaningful provenance.
5. Do not delete a source repository until its migrated content has been verified here.

## Planned destinations

| Source repository | Destination |
| --- | --- |
| `polars-processings` | `processing/polars/` |
| `gcp-projects` | `cloud/gcp/` |
| `KafkaFlow` | `streaming/kafka/` |
| `streaming-with-kafka` | `streaming/kafka/` |
| `KafkaStreamer` | `streaming/kafka/` |
| `attendance_pipeline` | `pipelines/attendance/` |
| `data-sparks` | `processing/spark/` |
| `bigdatabox` | selected material across the relevant domains |
| `ga-data` | selected material under `analytics/` or relevant domain folders |
| `crime-monitoring-with-spark` | `case-studies/spark-kafka/sf-crime-streaming/` |
| `fraud-detection-apps` | selected data/ML material where still useful |
| `analytics` | selected data-engineering material |
| `invoice-processing-spark` | generic Spark material under `processing/spark/`; invoice-specific work moves to `invoice-platform` |

## Target taxonomy

```text
cloud/
  aws/
  azure/
  gcp/
streaming/
  kafka/
processing/
  spark/
    python/
    java/
    scala/
  polars/
pipelines/
orchestration/
databases/
ingestion/
analytics/
case-studies/
docs/
tests/
```

The existing `azure/` workspace remains valid and should be moved under `cloud/azure/` as part of the structural migration rather than being rewritten unnecessarily.
