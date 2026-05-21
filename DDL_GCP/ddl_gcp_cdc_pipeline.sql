DROP TABLE IF EXISTS `dbt-taxi-explore.dev_bronze_cdc_events.ride_events`;

CREATE TABLE `dbt-taxi-explore.dev_bronze_cdc_events.ride_events`
(
  ride_id INT64,
  rider_id INT64,
  driver_id INT64,
  vehicle_id INT64,

  ride_status STRING,
  service_type STRING,
  city_code STRING,

  -- PostgreSQL TIMESTAMPTZ dari Debezium terbaca sebagai STRING.
  requested_at STRING,
  accepted_at STRING,
  arrived_at STRING,
  started_at STRING,
  completed_at STRING,
  cancelled_at STRING,

  cancelled_by_user_id INT64,
  cancel_reason_code STRING,
  cancel_reason_note STRING,

  estimated_distance_km NUMERIC,
  estimated_duration_min NUMERIC,

  -- PostgreSQL TIMESTAMPTZ dari Debezium terbaca sebagai STRING.
  created_at STRING,
  updated_at STRING,

  __op STRING,
  __table STRING,
  __lsn INT64,
  __source_ts_ms INT64
)
PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)
CLUSTER BY ride_id, driver_id, ride_status
OPTIONS (
  require_partition_filter = TRUE,
  description = "Historical CDC events from Debezium topic cdc.public.ride. One row represents one CDC event."
);


DROP TABLE IF EXISTS `dbt-taxi-explore.dev_bronze_cdc_events.driver_profile_events`;

CREATE TABLE `dbt-taxi-explore.dev_bronze_cdc_events.driver_profile_events`
(
  driver_id INT64,
  user_id INT64,

  license_number STRING,
  license_expiry DATE,

  driver_status STRING,
  verification_status STRING,

  -- PostgreSQL TIMESTAMPTZ dari Debezium terbaca sebagai STRING.
  verified_at STRING,
  suspended_at STRING,

  rating_avg NUMERIC,
  rating_count INT64,

  -- PostgreSQL TIMESTAMPTZ dari Debezium terbaca sebagai STRING.
  created_at STRING,
  updated_at STRING,

  __op STRING,
  __table STRING,
  __lsn INT64,
  __source_ts_ms INT64
)
PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)
CLUSTER BY driver_id, driver_status, verification_status
OPTIONS (
  require_partition_filter = TRUE,
  description = "Historical CDC events from Debezium topic cdc.public.driver_profile. One row represents one CDC event."
);


DROP TABLE IF EXISTS `dbt-taxi-explore.dev_bronze_cdc_events.payment_transaction_events`;

CREATE TABLE `dbt-taxi-explore.dev_bronze_cdc_events.payment_transaction_events`
(
  transaction_id INT64,
  ride_id INT64,
  user_payment_method_id INT64,

  provider_name STRING,
  provider_transaction_id STRING,
  idempotency_key STRING,

  amount NUMERIC,
  method_fee NUMERIC,
  currency_code STRING,

  payment_status STRING,
  failure_code STRING,
  failure_message STRING,

  -- PostgreSQL TIMESTAMPTZ dari Debezium terbaca sebagai STRING.
  authorized_at STRING,
  captured_at STRING,
  paid_at STRING,
  created_at STRING,
  updated_at STRING,

  __op STRING,
  __table STRING,
  __lsn INT64,
  __source_ts_ms INT64
)
PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR)
CLUSTER BY transaction_id, ride_id, payment_status
OPTIONS (
  require_partition_filter = TRUE,
  description = "Historical CDC events from Debezium topic cdc.public.payment_transaction. One row represents one CDC event."
);


CREATE OR REPLACE VIEW `dbt-taxi-explore.dev_bronze_cdc_current.ride` AS

WITH ranked AS (
  SELECT
    e.*,
    _PARTITIONTIME AS _bq_partition_time,
    FORMAT_TIMESTAMP(
      '%Y-%m-%d %H:00:00',
      _PARTITIONTIME,
      'Asia/Jakarta'
    ) AS _load_hour_jakarta,

    ROW_NUMBER() OVER (
      PARTITION BY ride_id
      ORDER BY
        SAFE_CAST(__source_ts_ms AS INT64) DESC,
        SAFE_CAST(__lsn AS INT64) DESC,
        _PARTITIONTIME DESC
    ) AS row_num

  FROM `dbt-taxi-explore.dev_bronze_cdc_events.ride_events` e
)

SELECT * EXCEPT(row_num)
FROM ranked
WHERE row_num = 1
  AND COALESCE(CAST(__op AS STRING), '') != 'd';


CREATE OR REPLACE VIEW `dbt-taxi-explore.dev_bronze_cdc_current.driver_profile` AS

WITH ranked AS (
  SELECT
    e.*,
    _PARTITIONTIME AS _bq_partition_time,
    FORMAT_TIMESTAMP(
      '%Y-%m-%d %H:00:00',
      _PARTITIONTIME,
      'Asia/Jakarta'
    ) AS _load_hour_jakarta,

    ROW_NUMBER() OVER (
      PARTITION BY driver_id
      ORDER BY
        SAFE_CAST(__source_ts_ms AS INT64) DESC,
        SAFE_CAST(__lsn AS INT64) DESC,
        _PARTITIONTIME DESC
    ) AS row_num

  FROM `dbt-taxi-explore.dev_bronze_cdc_events.driver_profile_events` e
)

SELECT * EXCEPT(row_num)
FROM ranked
WHERE row_num = 1
  AND COALESCE(CAST(__op AS STRING), '') != 'd';


CREATE OR REPLACE VIEW `dbt-taxi-explore.dev_bronze_cdc_current.payment_transaction` AS

WITH ranked AS (
  SELECT
    e.*,
    _PARTITIONTIME AS _bq_partition_time,
    FORMAT_TIMESTAMP(
      '%Y-%m-%d %H:00:00',
      _PARTITIONTIME,
      'Asia/Jakarta'
    ) AS _load_hour_jakarta,

    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY
        SAFE_CAST(__source_ts_ms AS INT64) DESC,
        SAFE_CAST(__lsn AS INT64) DESC,
        _PARTITIONTIME DESC
    ) AS row_num

  FROM `dbt-taxi-explore.dev_bronze_cdc_events.payment_transaction_events` e
)

SELECT * EXCEPT(row_num)
FROM ranked
WHERE row_num = 1
  AND COALESCE(CAST(__op AS STRING), '') != 'd';