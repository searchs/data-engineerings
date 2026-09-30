# BigQuery lab

Curated from the legacy `gcp-projects` repository.

## BigQuery CLI quick start

Authenticate with the Google Cloud CLI using your normal local/CI identity, then select a project:

```bash
gcloud auth list
gcloud config list project
# gcloud config set project YOUR_PROJECT_ID
```

Inspect BigQuery resources:

```bash
bq ls
bq ls bigquery-public-data:
bq show bigquery-public-data:samples.shakespeare
bq help query
```

Run a Standard SQL query:

```bash
bq query --use_legacy_sql=false '
SELECT word, SUM(word_count) AS count
FROM `bigquery-public-data.samples.shakespeare`
WHERE word LIKE "%raisin%"
GROUP BY word
ORDER BY count DESC
'
```

Create, inspect and remove a dataset:

```bash
bq mk babynames
bq ls
bq show babynames

# Example load command once you have a local CSV-like source file:
bq load \
  --source_format=CSV \
  babynames.names2010 \
  yob2010.txt \
  name:STRING,gender:STRING,count:INTEGER

bq show babynames.names2010
bq rm -r -f babynames
```

## BigQuery ML

`taxi_fare_bigquery_ml.sql` preserves the main learning objective from the original taxi-fare experiment:

1. derive training features in SQL;
2. split rows deterministically;
3. train a linear-regression model;
4. evaluate RMSE;
5. generate predictions.

The historical source used older NYC taxi dataset names. Before running the example, confirm the currently available public table and update the `SOURCE_TABLE` reference accordingly.

## Intentionally omitted

The legacy repository also contained a TensorFlow Estimator / Cloud ML Engine housing-price tutorial. That API/tooling generation is obsolete and was not migrated. If a modern Vertex AI example is needed, it should be implemented as a new lab rather than preserving the old tutorial verbatim.
