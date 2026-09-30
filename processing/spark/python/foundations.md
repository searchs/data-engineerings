# PySpark foundations and ETL patterns

Curated from the legacy `bigdatabox` scratch repository.

## RDD fundamentals

```python
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("RDDExample")
sc = SparkContext(conf=conf)

people = sc.parallelize(
    [("Richard", 22), ("Alfred", 23), ("Loki", 4), ("Albert", 12)]
)

left = sc.parallelize([("Richard", 1), ("Alfred", 4)])
right = sc.parallelize([("Richard", 2), ("Alfred", 5)])
joined = left.join(right)
```

For most structured workloads, prefer DataFrames/Datasets because Spark can optimise their logical plans.

## DataFrame loading

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("CSVLoader")
    .getOrCreate()
)

df = spark.read.option("header", True).csv("input.csv")
df.select("neighbourhood").distinct().show(10, truncate=False)
```

## Partition guardrail

A reusable partition-sizing helper can prevent both tiny partition counts and needless partition explosion:

```python
def normalise_partitions(df, minimum: int, maximum: int):
    current = df.rdd.getNumPartitions()
    if current < minimum:
        return df.repartition(minimum)
    if current > maximum:
        return df.coalesce(maximum)
    return df
```

`repartition` performs a shuffle and is appropriate when increasing partitions or redistributing data. `coalesce` can reduce partitions without a full shuffle in common cases.

## JDBC ETL shape

The old repository also explored Spark + PostgreSQL ETL. The durable pattern is:

```python
jdbc_options = {
    "url": jdbc_url,
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver",
}

movies = spark.read.format("jdbc").options(**jdbc_options, dbtable="movies").load()
ratings = spark.read.format("jdbc").options(**jdbc_options, dbtable="ratings").load()

average_ratings = ratings.groupBy("movie_id").avg("rating")
result = movies.join(average_ratings, movies.id == average_ratings.movie_id)

(
    result.write
    .mode("overwrite")
    .jdbc(url=jdbc_url, table="avg_ratings", properties=connection_properties)
)
```

Do not hard-code JDBC credentials or developer-local driver paths. Supply them through environment/configuration and package/runtime configuration instead.

## Data-quality reminder

A useful principle retained from `bigdatabox`: a cleansing workflow should preserve the raw source, a clean output, a data dictionary/codebook, and a reproducible record of the transformations that produced the clean data.
