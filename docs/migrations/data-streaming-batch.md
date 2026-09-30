# Data streaming consolidation batch

This migration consolidates durable authored material from four legacy repositories:

- `gcp-projects`
- `KafkaFlow`
- `streaming-with-kafka`
- `KafkaStreamer`

## Curation rules

- Preserve concepts and authored examples, not repository scaffolding.
- Correct obvious defects while keeping the original learning intent.
- Do not copy generated data, local datasets, lock files, IDE files, or obsolete dependency setups.
- Keep legacy technology context explicit where an example is intentionally historical.

## Destination map

| Source | Destination |
| --- | --- |
| `gcp-projects` | `cloud/gcp/bigquery/` |
| `KafkaFlow` | `streaming/kafka/java/` |
| `streaming-with-kafka` | `streaming/kafka/python/` and Kafka notes |
| `KafkaStreamer` | `streaming/kafka/scala/` |

The source repositories remain unchanged until this migration is reviewed and merged.
