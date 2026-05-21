-- ─────────────────────────────────────────────────────────────────────────────
-- Batch append-only tables
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.ride_status_history` (
  ride_status_history_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  old_status STRING,
  new_status STRING NOT NULL,
  changed_by_user_id INT64,
  reason_code STRING,
  reason_note STRING,
  changed_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(changed_at, DAY)
CLUSTER BY ride_id, new_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.ride_location` (
  ride_location_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  location_type STRING NOT NULL,
  latitude NUMERIC NOT NULL,
  longitude NUMERIC NOT NULL,
  address_text STRING,
  place_id STRING,
  captured_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(captured_at, DAY)
CLUSTER BY ride_id, location_type;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.ride_tracking_point` (
  tracking_point_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  driver_id INT64 NOT NULL,
  latitude NUMERIC NOT NULL,
  longitude NUMERIC NOT NULL,
  speed_kmh NUMERIC,
  heading_degree NUMERIC,
  accuracy_meter NUMERIC,
  recorded_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(recorded_at, DAY)
CLUSTER BY ride_id, driver_id;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.ride_fare` (
  fare_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  fare_type STRING NOT NULL,
  fare_version INT64 NOT NULL,
  currency_code STRING NOT NULL,
  distance_km NUMERIC NOT NULL,
  duration_min NUMERIC NOT NULL,
  base_fare NUMERIC NOT NULL,
  distance_fare NUMERIC NOT NULL,
  time_fare NUMERIC NOT NULL,
  surge_multiplier NUMERIC NOT NULL,
  surge_amount NUMERIC NOT NULL,
  discount_amount NUMERIC NOT NULL,
  tax_amount NUMERIC NOT NULL,
  platform_fee NUMERIC NOT NULL,
  driver_earning NUMERIC NOT NULL,
  total_fare NUMERIC NOT NULL,
  fare_rule_code STRING NOT NULL,
  calculated_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(calculated_at, MONTH)
CLUSTER BY ride_id, fare_type;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.ride_fare_component` (
  fare_component_id INT64 NOT NULL,
  fare_id INT64 NOT NULL,
  component_code STRING NOT NULL,
  component_name STRING NOT NULL,
  component_amount NUMERIC NOT NULL,
  description STRING,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY fare_id, component_code;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.promo_usage` (
  promo_usage_id INT64 NOT NULL,
  promotion_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  rider_id INT64 NOT NULL,
  discount_amount_applied NUMERIC NOT NULL,
  used_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(used_at, MONTH)
CLUSTER BY promotion_id, rider_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- Batch upsert tables
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.role` (
  role_id INT64 NOT NULL,
  role_code STRING NOT NULL,
  role_name STRING NOT NULL,
  description STRING,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY role_code, is_active;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.payment_method_type` (
  payment_method_type_id INT64 NOT NULL,
  method_code STRING NOT NULL,
  method_name STRING NOT NULL,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY method_code, is_active;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.promotion` (
  promotion_id INT64 NOT NULL,
  promo_code STRING NOT NULL,
  promo_description STRING,
  discount_type STRING NOT NULL,
  discount_pct NUMERIC,
  discount_amount NUMERIC,
  max_discount_amount NUMERIC,
  min_fare_amount NUMERIC NOT NULL,
  usage_limit_total INT64,
  usage_limit_per_user INT64,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP NOT NULL,
  promotion_status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY promo_code, promotion_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.user_account` (
  user_id INT64 NOT NULL,
  username STRING NOT NULL,
  email STRING,
  phone_number STRING,
  account_status STRING NOT NULL,
  email_verified_at TIMESTAMP,
  phone_verified_at TIMESTAMP,
  last_login_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY account_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.user_role` (
  user_id INT64 NOT NULL,
  role_id INT64 NOT NULL,
  assigned_at TIMESTAMP NOT NULL,
  assigned_by INT64,
  is_active BOOL NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(assigned_at, MONTH)
CLUSTER BY role_id, is_active;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.driver_document` (
  document_id INT64 NOT NULL,
  driver_id INT64 NOT NULL,
  document_type STRING NOT NULL,
  document_number STRING NOT NULL,
  verification_status STRING NOT NULL,
  submitted_at TIMESTAMP NOT NULL,
  verified_at TIMESTAMP,
  verified_by INT64,
  expires_at DATE,
  rejection_reason STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY driver_id, document_type, verification_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.vehicle` (
  vehicle_id INT64 NOT NULL,
  license_plate STRING NOT NULL,
  vehicle_make STRING NOT NULL,
  vehicle_model STRING NOT NULL,
  vehicle_year INT64 NOT NULL,
  vehicle_capacity INT64 NOT NULL,
  vehicle_color STRING,
  vehicle_type STRING NOT NULL,
  vehicle_status STRING NOT NULL,
  verified_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY vehicle_type, vehicle_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.driver_vehicle_assignment` (
  assignment_id INT64 NOT NULL,
  driver_id INT64 NOT NULL,
  vehicle_id INT64 NOT NULL,
  assigned_from TIMESTAMP NOT NULL,
  assigned_to TIMESTAMP,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(assigned_from, MONTH)
CLUSTER BY driver_id, vehicle_id, is_active;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.user_payment_method` (
  user_payment_method_id INT64 NOT NULL,
  user_id INT64 NOT NULL,
  payment_method_type_id INT64 NOT NULL,
  provider_name STRING,
  provider_customer_id STRING,
  masked_account STRING,
  expiry_month INT64,
  expiry_year INT64,
  is_default BOOL NOT NULL,
  payment_method_status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY user_id, payment_method_status;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.review` (
  review_id INT64 NOT NULL,
  ride_id INT64 NOT NULL,
  reviewer_id INT64 NOT NULL,
  reviewee_id INT64 NOT NULL,
  review_type STRING NOT NULL,
  rating_score INT64 NOT NULL,
  comments STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(created_at, MONTH)
CLUSTER BY ride_id, review_type;

CREATE TABLE IF NOT EXISTS `dbt-taxi-explore.dev_bronze_pg.payment_refund` (
  refund_id INT64 NOT NULL,
  transaction_id INT64 NOT NULL,
  provider_refund_id STRING,
  refund_amount NUMERIC NOT NULL,
  currency_code STRING NOT NULL,
  refund_status STRING NOT NULL,
  refund_reason_code STRING,
  refund_reason_note STRING,
  requested_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  _ingested_at TIMESTAMP NOT NULL,
  _source_system STRING NOT NULL
)
PARTITION BY TIMESTAMP_TRUNC(requested_at, MONTH)
CLUSTER BY transaction_id, refund_status;