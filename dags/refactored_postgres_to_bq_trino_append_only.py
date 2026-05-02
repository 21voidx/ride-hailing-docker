"""
DAG: postgres_to_bq_trino_append_only
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

DAG_ID = "refactored_postgres_to_bq_append_only"
SOURCE_TZ = "Asia/Jakarta"

TRINO_CONN_ID = "trino_default"
GCP_CONN_ID = "google_cloud_default"

TRINO_BQ_CAT = "bigquery"
TRINO_PG_CAT = "postgresql"

BQ_PROJECT = "dbt-taxi-explore"
BQ_DATASET = "dev_bronze_pg"
BQ_LOCATION = "US"
PG_SCHEMA = "public"


BASE_LABELS: dict[str, str] = {
    "env": "dev",
    "team": "data-eng",
    "dag-id": DAG_ID.lower().replace("_", "-")[:63],
    "layer": "bronze",
    "pipeline": "ingestion",
}


SHARED = {
    "bq_project": BQ_PROJECT,
    "bq_dataset": BQ_DATASET,
    "bq_location": BQ_LOCATION,
    "pg_schema": PG_SCHEMA,
    "trino_conn_id": TRINO_CONN_ID,
    "gcp_conn_id": GCP_CONN_ID,
    "trino_bq_cat": TRINO_BQ_CAT,
    "trino_pg_cat": TRINO_PG_CAT,
    "source_tz": SOURCE_TZ,
    "dag_labels": BASE_LABELS,
}


# =============================================================================
# Table configs
# =============================================================================

TABLE_CONFIGS = [

    # ── Batch append-only tables ───────────────────────────────────────────

    TableConfig(
        pg_table        = "ride_status_history",
        bq_final_table  = "ride_status_history",
        merge_key       = "ride_status_history_id",
        partition_field = "changed_at",
        partition_type  = "DAY",
        cluster_fields  = ["ride_id", "new_status"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "ride_status_history_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",                "type": "INT64",     "mode": "REQUIRED"},
            {"name": "old_status",             "type": "STRING",    "mode": "NULLABLE"},
            {"name": "new_status",             "type": "STRING",    "mode": "REQUIRED"},
            {"name": "changed_by_user_id",      "type": "INT64",    "mode": "NULLABLE"},
            {"name": "reason_code",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "reason_note",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "changed_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            ride_status_history_id, ride_id, old_status, new_status,
            changed_by_user_id, reason_code, reason_note, changed_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_location",
        bq_final_table  = "ride_location",
        merge_key       = "ride_location_id",
        partition_field = "captured_at",
        partition_type  = "DAY",
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
            {"name": "place_id",         "type": "STRING",    "mode": "NULLABLE"},
            {"name": "captured_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",   "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            ride_location_id, ride_id, location_type, latitude, longitude,
            address_text, place_id, captured_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_tracking_point",
        bq_final_table  = "ride_tracking_point",
        merge_key       = "tracking_point_id",
        partition_field = "recorded_at",
        partition_type  = "DAY",
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
            {"name": "heading_degree",    "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "accuracy_meter",    "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "recorded_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            tracking_point_id, ride_id, driver_id, latitude, longitude,
            speed_kmh, heading_degree, accuracy_meter, recorded_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_fare",
        bq_final_table  = "ride_fare",
        merge_key       = "fare_id",
        partition_field = "calculated_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "fare_type"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "fare_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "fare_type",         "type": "STRING",    "mode": "REQUIRED"},
            {"name": "fare_version",      "type": "INT64",     "mode": "REQUIRED"},
            {"name": "currency_code",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "distance_km",       "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "duration_min",      "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "base_fare",         "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "distance_fare",     "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "time_fare",         "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "surge_multiplier",  "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "surge_amount",      "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "discount_amount",   "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "tax_amount",        "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "platform_fee",      "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "driver_earning",    "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "total_fare",        "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "fare_rule_code",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "calculated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            fare_id, ride_id, fare_type, fare_version, currency_code,
            distance_km, duration_min, base_fare, distance_fare, time_fare,
            surge_multiplier, surge_amount, discount_amount, tax_amount,
            platform_fee, driver_earning, total_fare, fare_rule_code,
            calculated_at, created_at
        """,
    ),

    TableConfig(
        pg_table        = "ride_fare_component",
        bq_final_table  = "ride_fare_component",
        merge_key       = "fare_component_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["fare_id", "component_code"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "fare_component_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "fare_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "component_code",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "component_name",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "component_amount",  "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "description",       "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            fare_component_id, fare_id, component_code, component_name,
            component_amount, description, created_at
        """,
    ),

    TableConfig(
        pg_table        = "promo_usage",
        bq_final_table  = "promo_usage",
        merge_key       = "promo_usage_id",
        partition_field = "used_at",
        partition_type  = "MONTH",
        cluster_fields  = ["promotion_id", "rider_id"],
        source_system   = "ride_ops_pg",
        append_only     = True,
        schema_fields   = [
            {"name": "promo_usage_id",          "type": "INT64",     "mode": "REQUIRED"},
            {"name": "promotion_id",            "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",                 "type": "INT64",     "mode": "REQUIRED"},
            {"name": "rider_id",                "type": "INT64",     "mode": "REQUIRED"},
            {"name": "discount_amount_applied", "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "used_at",                 "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at",              "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",            "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",          "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            promo_usage_id, promotion_id, ride_id, rider_id,
            discount_amount_applied, used_at, created_at
        """,
    )
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
    schedule=CronDataIntervalTimetable("30 22 * * *", timezone=SOURCE_TZ),
    start_date=pendulum.datetime(2026, 3, 30, tz=SOURCE_TZ),
    catchup=False,
    max_active_runs=1,
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
    table_groups = [make_table_task_group(cfg, **SHARED) for cfg in TABLE_CONFIGS]

    # Default mode: all TaskGroups run in parallel.
    # Sequential mode, if needed:
    # from airflow.sdk import chain
    chain(*table_groups)
