-- Curated from the legacy gcp-projects taxi-fare experiment.
-- Historical source table used by the original lab:
--   `nyc-tlc.yellow.trips`
-- Confirm the currently available public table before executing.

CREATE OR REPLACE MODEL `taxi.taxifare_model`
OPTIONS (
  model_type = 'linear_reg',
  input_label_cols = ['total_fare']
) AS
WITH daynames AS (
  SELECT ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] AS days
), training AS (
  SELECT
    (tolls_amount + fare_amount) AS total_fare,
    days[ORDINAL(EXTRACT(DAYOFWEEK FROM pickup_datetime))] AS day_of_week,
    EXTRACT(HOUR FROM pickup_datetime) AS hour_of_day,
    SQRT(
      POW(pickup_longitude - dropoff_longitude, 2) +
      POW(pickup_latitude - dropoff_latitude, 2)
    ) AS distance,
    passenger_count AS passengers
  FROM `nyc-tlc.yellow.trips`, daynames
  WHERE trip_distance > 0
    AND fare_amount BETWEEN 6 AND 200
    AND pickup_longitude BETWEEN -75 AND -73
    AND dropoff_longitude BETWEEN -75 AND -73
    AND pickup_latitude BETWEEN 40 AND 42
    AND dropoff_latitude BETWEEN 40 AND 42
    AND MOD(ABS(FARM_FINGERPRINT(CAST(pickup_datetime AS STRING))), 1000) = 1
)
SELECT * FROM training;

-- Evaluate against a deterministic hold-out sample.
SELECT
  SQRT(mean_squared_error) AS rmse
FROM ML.EVALUATE(
  MODEL `taxi.taxifare_model`,
  (
    WITH daynames AS (
      SELECT ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'] AS days
    )
    SELECT
      (tolls_amount + fare_amount) AS total_fare,
      days[ORDINAL(EXTRACT(DAYOFWEEK FROM pickup_datetime))] AS day_of_week,
      EXTRACT(HOUR FROM pickup_datetime) AS hour_of_day,
      SQRT(
        POW(pickup_longitude - dropoff_longitude, 2) +
        POW(pickup_latitude - dropoff_latitude, 2)
      ) AS distance,
      passenger_count AS passengers
    FROM `nyc-tlc.yellow.trips`, daynames
    WHERE trip_distance > 0
      AND fare_amount BETWEEN 6 AND 200
      AND pickup_longitude BETWEEN -75 AND -73
      AND dropoff_longitude BETWEEN -75 AND -73
      AND pickup_latitude BETWEEN 40 AND 42
      AND dropoff_latitude BETWEEN 40 AND 42
      AND MOD(ABS(FARM_FINGERPRINT(CAST(pickup_datetime AS STRING))), 1000) = 2
  )
);
