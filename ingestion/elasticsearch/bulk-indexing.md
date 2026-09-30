# Elasticsearch bulk-indexing notes

Curated from an early `ga-data` script that looped over JSON lines and invoked `curl` once per document.

The original experiment demonstrated document-by-document indexing, but a production ingestion path should use Elasticsearch/OpenSearch bulk APIs instead.

## Preferred design

- parse and validate input records before submission
- batch documents into bounded bulk requests
- use the official client rather than spawning `curl`
- make index/endpoint/configuration external
- handle per-item bulk failures explicitly
- retry only transient failures with bounded backoff
- collect throughput, rejection and latency metrics
- make ingestion idempotent where document identifiers are known

This preserves the ingestion lesson while avoiding the old `localhost:9200/catalogue/product/...` type-specific API and one-process-per-document approach.
