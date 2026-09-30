# `bigdatabox` curation record

`bigdatabox` was a broad data-analysis and big-data scratch repository rather than one coherent application. It mixed notebooks, SQL, shell scripts, infrastructure snippets and large local datasets.

## Material retained conceptually

The canonical repository now preserves the most durable engineering lessons in focused locations, including:

- PySpark RDD/DataFrame fundamentals and partition sizing;
- Spark JDBC ETL patterns;
- data-quality workflow principles;
- BigQuery material under `cloud/gcp/bigquery/` where overlapping GCP experiments have been consolidated.

## Material intentionally not migrated

### Large/raw datasets

Examples included churn, pulsar, housing, salary and other CSV datasets, including files measured in multiple megabytes. Raw learning datasets do not justify permanent duplication in the canonical engineering repository.

### Notebook scratch work

Numerous notebooks covered Pandas/NumPy workshops, stock analysis, clickstream analysis, MongoDB, PySpark walkthroughs and exploratory data science. They are useful historically but overlap the canonical labs and are not being migrated wholesale.

### Obsolete infrastructure examples

The legacy EMR CloudFormation template used EMR 5.7.0, old `m3` instance types, hard-coded subnet/security-group identifiers and legacy managed-role assumptions. It is retained only in Git history of the source repository until that repository is deleted; a future AWS/EMR lab should be written against current infrastructure practices.

### Mixed shell/admin snippets

Hadoop/Hive, database-cleaner and Airflow setup scripts were environment-specific scratch notes. Where those operational patterns remain valuable, they should be reintroduced as deliberate, tested runbooks rather than copied as-is.

## Deletion readiness

No unique commercial product or active system was identified in `bigdatabox`. The durable learning content has been represented in `data-engineerings`; the bulky scratch datasets and obsolete setup artefacts are intentionally excluded.
