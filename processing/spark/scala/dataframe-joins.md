# Scala Spark DataFrame and join patterns

Curated from the generic Spark exercises that previously lived in `invoice-processing-spark`. These lessons are intentionally kept outside the invoice product boundary.

## Core patterns

The old exercises compared equivalent Spark SQL and DataFrame API operations over small in-memory store/occupancy datasets.

### Filtering and projection

```scala
val stores = spark.createDataFrame(storeRows)

stores.where(col("closes") >= 22)
stores.select("name").where(col("capacity") > 20)
```

### Inner join

```scala
val joined = stores.join(
  occupants,
  stores("name") === occupants("storename"),
  "inner"
)
```

### Left / right / full joins

```scala
stores.join(occupants, stores("name") === occupants("storename"), "left")
stores.join(occupants, stores("name") === occupants("storename"), "right")
stores.join(occupants, stores("name") === occupants("storename"), "full")
```

### Semi and anti joins

Semi joins are useful when another dataset is a filter condition without needing its columns in the result. Anti joins return unmatched rows.

```scala
stores.join(boutiques, stores("name") === boutiques("boutiquename"), "semi")
stores.join(boutiques, stores("name") === boutiques("boutiquename"), "anti")
```

### Derived availability

```scala
val availability = stores
  .join(occupants, stores("name") === occupants("storename"))
  .withColumn("availability", col("capacity") - col("occupants"))
  .where(col("availability") >= 4)
  .select("name", "availability")
```

## Curation notes

- The original exercise mixed several equivalent syntaxes in one executable file; this note keeps the durable concepts without preserving noisy demo output.
- Generic word-count exercises were not retained because equivalent examples already exist widely and add little unique value.
- Invoice-specific streaming/flattening logic was moved to the canonical Invoice Platform instead.
