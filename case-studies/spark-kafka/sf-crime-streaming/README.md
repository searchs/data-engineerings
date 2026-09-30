# San Francisco crime streaming case study

> Historical learning project based on Udacity's **San Francisco Crime Statistics with Spark Streaming** exercise. The implementation and observations below summarise personal work completed against that project. This is not presented as a greenfield commercial application.

## Objective

Process San Francisco crime-call events through Kafka and Spark Structured Streaming to answer questions such as:

- which crime types occur most frequently;
- how incidents are distributed across locations/dispositions;
- how Spark streaming configuration changes throughput and latency.

## Original flow

```text
crime JSON
   ↓
Kafka producer
   ↓
Kafka topic: crimes.calls
   ↓
Spark Structured Streaming
   ↓
JSON parsing + watermark
   ↓
windowed aggregation
   ↓
console / analytical sink
```

## Durable implementation pattern

The original `data_stream.py` used a typed `StructType`, cast Kafka values to strings, parsed them with `from_json`, added a five-minute watermark and aggregated crime type counts over one-hour windows.

A representative shape is:

```python
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", "crimes.calls")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 200)
    .load()
)

parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS payload", "timestamp")
    .select(from_json(col("payload"), schema).alias("event"), col("timestamp"))
    .select("event.*", "timestamp")
)

aggregated = (
    parsed
    .withWatermark("call_date_time", "5 minutes")
    .groupBy(
        col("original_crime_type_name"),
        window(col("call_date_time"), "60 minutes"),
    )
    .count()
)
```

## Performance lessons retained

The original project explored how `maxOffsetsPerTrigger` affected `numInputRows` and `processedRowsPerSecond`. The useful lesson is not one magic value, but that ingestion limits, partition count, trigger interval and downstream work must be tuned together against measured latency and throughput.

Checkpointing is also critical for restart/recovery behaviour in stateful streaming jobs.

## Modernisation notes

The historical project targeted Spark 2.4.x, Scala 2.11, Java 8 and Kafka with ZooKeeper. Those exact environment instructions are intentionally not migrated. A modern implementation should use currently supported Spark/Kafka versions and a contemporary Kafka deployment model.

## Intentionally omitted

- Udacity-provided scaffolding and datasets;
- Kafka/ZooKeeper configuration files;
- large screenshots of Spark UI/progress reports;
- course-specific helper scripts;
- legacy dependency pins.

The source repository can therefore be retired once this curated case-study note is accepted.
