# Java/Spark data-processing patterns

Curated from personal solution work previously stored in `data-sparks`.

The legacy repository was based on a third-party interview/exercise pack, so the original questions, environment setup, generated Maven output and proprietary scaffolding are **not** reproduced here. This note preserves only generic Spark patterns demonstrated in the personal solution.

## Local Spark test harness

A lightweight test harness can create one `SparkSession` per test class and stop it after the test suite:

```java
SparkSession spark = SparkSession.builder()
    .appName("SparkDataPatterns")
    .master("local[1]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate();
```

Keep JDBC URLs and credentials outside source code rather than embedding developer-specific absolute paths.

## Useful Dataset patterns

### Filter rows

```java
Dataset<Row> selected = users
    .filter(col("DISPLAY_NAME").startsWith("Adam"));
```

### Derive an adjusted value

Prefer DataFrame expressions when possible:

```java
Dataset<Row> adjusted = users
    .withColumn(
        "ADJUSTED_REPUTATION",
        when(col("DISPLAY_NAME").startsWith("Adam"), col("REPUTATION").plus(100))
            .when(col("DISPLAY_NAME").startsWith("Ben"), col("REPUTATION").plus(5000))
            .otherwise(col("REPUTATION"))
    );
```

The historical solution also explored RDD `map`/`flatMap`. Those APIs are useful for learning, but Dataset/DataFrame expressions are normally preferable when Spark can optimise the logical plan.

### Aggregate by time period

```java
Dataset<Row> averages = comments
    .groupBy(year(col("CREATION_DATE")).alias("year"))
    .agg(avg("SCORE").alias("average_score"))
    .orderBy("year");
```

### Join and aggregate

```java
Dataset<Row> activity = comments
    .join(users, comments.col("USER_ID").equalTo(users.col("ID")), "inner")
    .groupBy(users.col("DISPLAY_NAME"))
    .count()
    .orderBy(col("count").desc());
```

## Migration note

Only generic learning patterns were retained. The third-party interview question bank, screenshots, generated `target/` directory and local database setup remain excluded from the canonical repository.
