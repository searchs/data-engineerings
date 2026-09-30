# Kafka streaming labs

This area consolidates Kafka-related learning and implementation patterns from several older repositories into one capability-oriented structure.

## Layout

- `java/` — producer fundamentals, keyed vs unkeyed records.
- `python/` — producer/consumer design, event contracts, Avro and Schema Registry lessons.
- `scala/` — Spark Structured Streaming consuming Kafka topics.

## Migration provenance

- `KafkaFlow` → Java producer examples.
- `streaming-with-kafka` → Python producer/consumer, Avro and Schema Registry lessons.
- `KafkaStreamer` → Scala + Spark Structured Streaming example.

The old repositories mixed learning scaffolds, generated data, obsolete client APIs and environment-specific setup. The canonical versions preserve the durable engineering concepts while removing those incidental dependencies.

## Design progression

A useful learning sequence is:

1. publish plain string records;
2. introduce keys and partition affinity;
3. model JSON events;
4. introduce schema-backed contracts;
5. connect consumers and stream processors;
6. add delivery semantics, retries, observability and checkpointing;
7. test failure and replay behaviour deliberately.
