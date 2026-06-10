"""
DAG: ingest_postgres_to_bq_trino
Airflow: 3.x
Engine: Trino federation layer for PostgreSQL and BigQuery catalogs.

Pipeline per table:
1. Create BigQuery temp table.
2. Insert PostgreSQL source data into temp table via Trino.
3. Sync final table schema.
4. Merge temp table into final table.
5. Drop temp table.

Add or remove tables only from TABLE_CONFIGS.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, chain
from airflow.timetables.interval import CronDataIntervalTimetable

from helpers.refactored_trino_helper import TableConfig, make_table_task_group


# =============================================================================
# Global config
# =============================================================================

DAG_ID = "ingest_postgres_to_bq_trino_weekly_v3"
SOURCE_TZ = "Asia/Jakarta"

TRINO_CONN_ID = "trino_default"
GCP_CONN_ID = "google_cloud_default"

TRINO_BQ_CAT = "bigquery"
TRINO_PG_CAT = "postgresql"

BQ_PROJECT = "dbt-taxi-explore"
BQ_DATASET = "raw_ride_pg"
BQ_LOCATION = "us"
PG_SCHEMA = "public"

BASE_LABELS: dict[str, str] = {
    "env": "dev",
    "team": "data-eng",
    "dag-id": DAG_ID.lower().replace("_", "-")[:63],
    "layer": "raw",
    "pipeline": "ingestion",
}


# =============================================================================
# Table configs
# =============================================================================

TABLE_CONFIGS = [
    # Generated from init_postgres.sql.

    TableConfig(
        pg_table        = "rider_account",
        bq_final_table  = "rider_account",
        merge_key       = "rider_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["city_code", "account_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "rider_id",       "type": "INT64",     "mode": "REQUIRED"},
            {"name": "username",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "full_name",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email",          "type": "STRING",    "mode": "NULLABLE"},
            {"name": "phone_number",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "account_status", "type": "STRING",    "mode": "REQUIRED"},
            {"name": "city_code",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            rider_id, username, full_name, email, phone_number, account_status,
            city_code, created_at, updated_at, deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "driver_profile",
        bq_final_table  = "driver_profile",
        merge_key       = "driver_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["city_code", "driver_status", "verification_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "driver_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_name",         "type": "STRING",    "mode": "REQUIRED"},
            {"name": "phone_number",        "type": "STRING",    "mode": "NULLABLE"},
            {"name": "city_code",           "type": "STRING",    "mode": "REQUIRED"},
            {"name": "driver_status",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "verification_status", "type": "STRING",    "mode": "REQUIRED"},
            {"name": "rating_avg",          "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "rating_count",        "type": "INT64",     "mode": "REQUIRED"},
            {"name": "created_at",          "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",          "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",          "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",      "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            driver_id, driver_name, phone_number, city_code, driver_status,
            verification_status, rating_avg, rating_count, created_at, updated_at,
            deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "vehicle",
        bq_final_table  = "vehicle",
        merge_key       = "vehicle_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "vehicle_type", "vehicle_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "vehicle_id",     "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",      "type": "INT64",     "mode": "NULLABLE"},
            {"name": "license_plate",  "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_type",   "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_make",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "vehicle_model",  "type": "STRING",    "mode": "NULLABLE"},
            {"name": "vehicle_year",   "type": "INT64",     "mode": "NULLABLE"},
            {"name": "vehicle_status", "type": "STRING",    "mode": "REQUIRED"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            vehicle_id, driver_id, license_plate, vehicle_type, vehicle_make,
            vehicle_model, vehicle_year, vehicle_status, created_at, updated_at,
            deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "driver_vehicle_assignment",
        bq_final_table  = "driver_vehicle_assignment",
        merge_key       = "assignment_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "vehicle_id", "is_active"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "assignment_id",  "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",      "type": "INT64",     "mode": "REQUIRED"},
            {"name": "vehicle_id",     "type": "INT64",     "mode": "REQUIRED"},
            {"name": "assigned_from",  "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "assigned_to",    "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "is_active",      "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            assignment_id, driver_id, vehicle_id, assigned_from, assigned_to,
            is_active, created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "driver_shift",
        bq_final_table  = "driver_shift",
        merge_key       = "shift_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "shift_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "shift_id",       "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",      "type": "INT64",     "mode": "REQUIRED"},
            {"name": "shift_status",   "type": "STRING",    "mode": "REQUIRED"},
            {"name": "started_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "ended_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            shift_id, driver_id, shift_status, started_at, ended_at, created_at,
            updated_at
        """,
    ),

    TableConfig(
        pg_table        = "ride",
        bq_final_table  = "ride",
        merge_key       = "ride_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["city_code", "ride_status", "service_type"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "ride_id",                "type": "INT64",     "mode": "REQUIRED"},
            {"name": "rider_id",               "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",              "type": "INT64",     "mode": "NULLABLE"},
            {"name": "vehicle_id",             "type": "INT64",     "mode": "NULLABLE"},
            {"name": "ride_status",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "service_type",           "type": "STRING",    "mode": "REQUIRED"},
            {"name": "city_code",              "type": "STRING",    "mode": "REQUIRED"},
            {"name": "requested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "accepted_at",            "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "arrived_at",             "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "started_at",             "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "completed_at",           "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cancelled_at",           "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "cancelled_by_type",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "cancel_reason_code",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "estimated_distance_km",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "estimated_duration_min", "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",             "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            ride_id, rider_id, driver_id, vehicle_id, ride_status, service_type,
            city_code, requested_at, accepted_at, arrived_at, started_at,
            completed_at, cancelled_at, cancelled_by_type, cancel_reason_code,
            estimated_distance_km, estimated_duration_min, created_at, updated_at,
            deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_status_history",
        bq_final_table  = "ride_status_history",
        merge_key       = "ride_status_history_id",
        partition_field = "changed_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "new_status", "changed_by_type"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "ride_status_history_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",                "type": "INT64",     "mode": "REQUIRED"},
            {"name": "old_status",             "type": "STRING",    "mode": "NULLABLE"},
            {"name": "new_status",             "type": "STRING",    "mode": "REQUIRED"},
            {"name": "changed_by_type",        "type": "STRING",    "mode": "REQUIRED"},
            {"name": "changed_by_id",          "type": "INT64",     "mode": "NULLABLE"},
            {"name": "reason_code",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "reason_note",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "changed_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            ride_status_history_id, ride_id, old_status, new_status,
            changed_by_type, changed_by_id, reason_code, reason_note, changed_at,
            created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_location",
        bq_final_table  = "ride_location",
        merge_key       = "ride_location_id",
        partition_field = "captured_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "location_type"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "ride_location_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",          "type": "INT64",     "mode": "REQUIRED"},
            {"name": "location_type",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "latitude",         "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "longitude",        "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "address_text",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "captured_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",   "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            ride_location_id, ride_id, location_type, latitude, longitude,
            address_text, captured_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_tracking_point",
        bq_final_table  = "ride_tracking_point",
        merge_key       = "tracking_point_id",
        partition_field = "recorded_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "driver_id"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "tracking_point_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",         "type": "INT64",     "mode": "REQUIRED"},
            {"name": "latitude",          "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "longitude",         "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "speed_kmh",         "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "recorded_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            tracking_point_id, ride_id, driver_id, latitude, longitude, speed_kmh,
            recorded_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_fare",
        bq_final_table  = "ride_fare",
        merge_key       = "fare_id",
        partition_field = "updated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "fare_type", "is_corrected"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "fare_id",          "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",          "type": "INT64",     "mode": "REQUIRED"},
            {"name": "fare_type",        "type": "STRING",    "mode": "REQUIRED"},
            {"name": "fare_version",     "type": "INT64",     "mode": "REQUIRED"},
            {"name": "currency_code",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "distance_km",      "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "duration_min",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "base_fare",        "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "distance_fare",    "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "time_fare",        "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "surge_multiplier", "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "surge_amount",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "discount_amount",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "tax_amount",       "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "platform_fee",     "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "driver_earning",   "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "total_fare",       "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "fare_rule_code",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "is_corrected",     "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "calculated_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",   "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            fare_id, ride_id, fare_type, fare_version, currency_code, distance_km,
            duration_min, base_fare, distance_fare, time_fare, surge_multiplier,
            surge_amount, discount_amount, tax_amount, platform_fee,
            driver_earning, total_fare, fare_rule_code, is_corrected,
            calculated_at, created_at, updated_at
        """,
    ),
]


# =============================================================================
# DAG
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id=DAG_ID,
    description=(
        f"PostgreSQL to BigQuery via Trino multi-table ingestion "
        f"to {BQ_DATASET} ({len(TABLE_CONFIGS)} tables)"
    ),
    default_args=default_args,
    schedule=CronDataIntervalTimetable("0 0 * * 0", timezone=SOURCE_TZ),
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    end_date=pendulum.datetime(2024, 3, 2, tz="Asia/Jakarta"),
    catchup=True,
    max_active_runs=2,
    tags=["postgres", "bigquery", "trino", "ingestion", "multi-table"],
    doc_md=__doc__,
    params={
        "window_start": Param(
            default=None,
            type=["null", "string"],
            description=f"Window start inclusive in {SOURCE_TZ}. Example: 2026-03-11 09:00:00",
        ),
        "window_end": Param(
            default=None,
            type=["null", "string"],
            description=f"Window end exclusive in {SOURCE_TZ}. Example: 2026-03-12 09:00:00",
        ),
    },
) as dag:

    table_groups = [
        make_table_task_group(
            cfg, 
            bq_project=BQ_PROJECT,
            bq_dataset=BQ_DATASET,
            bq_location=BQ_LOCATION,
            pg_schema=PG_SCHEMA,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            trino_bq_cat=TRINO_BQ_CAT,
            trino_pg_cat=TRINO_PG_CAT,
            source_tz=SOURCE_TZ,
            dag_labels=BASE_LABELS,
        ) 
        for cfg in TABLE_CONFIGS
    ]

    # Default mode: all TaskGroups run in parallel.
    # Sequential mode, if needed:
    # from airflow.sdk import chain
    chain(*table_groups)
