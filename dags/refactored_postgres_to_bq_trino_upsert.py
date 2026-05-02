"""
DAG: postgres_to_bq_trino_upsert
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

DAG_ID = "refactored_postgres_to_bq_upsert"
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
    # ── Batch upsert tables ────────────────────────────────────────────────

    TableConfig(
        pg_table        = "role",
        bq_final_table  = "role",
        merge_key       = "role_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["role_code", "is_active"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "role_id",        "type": "INT64",     "mode": "REQUIRED"},
            {"name": "role_code",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "role_name",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "description",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "is_active",      "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            role_id, role_code, role_name, description, is_active,
            created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "payment_method_type",
        bq_final_table  = "payment_method_type",
        merge_key       = "payment_method_type_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["method_code", "is_active"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "payment_method_type_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "method_code",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "method_name",            "type": "STRING",    "mode": "REQUIRED"},
            {"name": "is_active",              "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            payment_method_type_id, method_code, method_name,
            is_active, created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "promotion",
        bq_final_table  = "promotion",
        merge_key       = "promotion_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["promo_code", "promotion_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "promotion_id",         "type": "INT64",     "mode": "REQUIRED"},
            {"name": "promo_code",           "type": "STRING",    "mode": "REQUIRED"},
            {"name": "promo_description",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "discount_type",        "type": "STRING",    "mode": "REQUIRED"},
            {"name": "discount_pct",         "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "discount_amount",      "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "max_discount_amount",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "min_fare_amount",      "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "usage_limit_total",    "type": "INT64",     "mode": "NULLABLE"},
            {"name": "usage_limit_per_user", "type": "INT64",     "mode": "NULLABLE"},
            {"name": "valid_from",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "valid_to",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "promotion_status",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "created_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",         "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",       "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            promotion_id, promo_code, promo_description, discount_type,
            discount_pct, discount_amount, max_discount_amount,
            min_fare_amount, usage_limit_total, usage_limit_per_user,
            valid_from, valid_to, promotion_status, created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "user_account",
        bq_final_table  = "user_account",
        merge_key       = "user_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["account_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "user_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "username",          "type": "STRING",    "mode": "REQUIRED"},
            {"name": "email",             "type": "STRING",    "mode": "NULLABLE"},
            {"name": "phone_number",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "account_status",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "email_verified_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "phone_verified_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "last_login_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",        "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",      "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",    "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            user_id, username, email, phone_number, account_status,
            email_verified_at, phone_verified_at, last_login_at,
            created_at, updated_at, deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "user_role",
        bq_final_table  = "user_role",
        merge_key       = ["user_id", "role_id"],
        partition_field = "assigned_at",
        partition_type  = "MONTH",
        cluster_fields  = ["role_id", "is_active"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "user_id",        "type": "INT64",     "mode": "REQUIRED"},
            {"name": "role_id",        "type": "INT64",     "mode": "REQUIRED"},
            {"name": "assigned_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "assigned_by",    "type": "INT64",     "mode": "NULLABLE"},
            {"name": "is_active",      "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            user_id, role_id, assigned_at, assigned_by, is_active
        """,
    ),

    TableConfig(
        pg_table        = "driver_document",
        bq_final_table  = "driver_document",
        merge_key       = "document_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["driver_id", "document_type", "verification_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "document_id",         "type": "INT64",     "mode": "REQUIRED"},
            {"name": "driver_id",           "type": "INT64",     "mode": "REQUIRED"},
            {"name": "document_type",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "document_number",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "verification_status", "type": "STRING",    "mode": "REQUIRED"},
            {"name": "submitted_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "verified_at",         "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "verified_by",         "type": "INT64",     "mode": "NULLABLE"},
            {"name": "expires_at",          "type": "DATE",      "mode": "NULLABLE"},
            {"name": "rejection_reason",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",          "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",          "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",      "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            document_id, driver_id, document_type, document_number,
            verification_status, submitted_at, verified_at, verified_by,
            expires_at, rejection_reason, created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "vehicle",
        bq_final_table  = "vehicle",
        merge_key       = "vehicle_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["vehicle_type", "vehicle_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "vehicle_id",       "type": "INT64",     "mode": "REQUIRED"},
            {"name": "license_plate",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_make",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_model",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_year",     "type": "INT64",     "mode": "REQUIRED"},
            {"name": "vehicle_capacity", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "vehicle_color",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "vehicle_type",     "type": "STRING",    "mode": "REQUIRED"},
            {"name": "vehicle_status",   "type": "STRING",    "mode": "REQUIRED"},
            {"name": "verified_at",      "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",   "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            vehicle_id, license_plate, vehicle_make, vehicle_model,
            vehicle_year, vehicle_capacity, vehicle_color, vehicle_type,
            vehicle_status, verified_at, created_at, updated_at, deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "driver_vehicle_assignment",
        bq_final_table  = "driver_vehicle_assignment",
        merge_key       = "assignment_id",
        partition_field = "assigned_from",
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
            assignment_id, driver_id, vehicle_id, assigned_from,
            assigned_to, is_active, created_at, updated_at
        """,
    ),

    TableConfig(
        pg_table        = "user_payment_method",
        bq_final_table  = "user_payment_method",
        merge_key       = "user_payment_method_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["user_id", "payment_method_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "user_payment_method_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "user_id",                "type": "INT64",     "mode": "REQUIRED"},
            {"name": "payment_method_type_id", "type": "INT64",     "mode": "REQUIRED"},
            {"name": "provider_name",          "type": "STRING",    "mode": "NULLABLE"},
            {"name": "provider_customer_id",   "type": "STRING",    "mode": "NULLABLE"},
            {"name": "masked_account",         "type": "STRING",    "mode": "NULLABLE"},
            {"name": "expiry_month",           "type": "INT64",     "mode": "NULLABLE"},
            {"name": "expiry_year",            "type": "INT64",     "mode": "NULLABLE"},
            {"name": "is_default",             "type": "BOOL",      "mode": "REQUIRED"},
            {"name": "payment_method_status",  "type": "STRING",    "mode": "REQUIRED"},
            {"name": "created_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",             "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",             "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",           "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",         "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            user_payment_method_id, user_id, payment_method_type_id,
            provider_name, provider_customer_id, masked_account,
            expiry_month, expiry_year, is_default, payment_method_status,
            created_at, updated_at, deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "review",
        bq_final_table  = "review",
        merge_key       = "review_id",
        partition_field = "created_at",
        partition_type  = "MONTH",
        cluster_fields  = ["ride_id", "review_type"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "review_id",      "type": "INT64",     "mode": "REQUIRED"},
            {"name": "ride_id",        "type": "INT64",     "mode": "REQUIRED"},
            {"name": "reviewer_id",    "type": "INT64",     "mode": "REQUIRED"},
            {"name": "reviewee_id",    "type": "INT64",     "mode": "REQUIRED"},
            {"name": "review_type",    "type": "STRING",    "mode": "REQUIRED"},
            {"name": "rating_score",   "type": "INT64",     "mode": "REQUIRED"},
            {"name": "comments",       "type": "STRING",    "mode": "NULLABLE"},
            {"name": "created_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",     "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at",   "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            review_id, ride_id, reviewer_id, reviewee_id, review_type,
            rating_score, comments, created_at, updated_at, deleted_at
        """,
    ),

    TableConfig(
        pg_table        = "payment_refund",
        bq_final_table  = "payment_refund",
        merge_key       = "refund_id",
        partition_field = "requested_at",
        partition_type  = "MONTH",
        cluster_fields  = ["transaction_id", "refund_status"],
        source_system   = "ride_ops_pg",
        append_only     = False,
        schema_fields   = [
            {"name": "refund_id",          "type": "INT64",     "mode": "REQUIRED"},
            {"name": "transaction_id",     "type": "INT64",     "mode": "REQUIRED"},
            {"name": "provider_refund_id", "type": "STRING",    "mode": "NULLABLE"},
            {"name": "refund_amount",      "type": "NUMERIC",   "mode": "REQUIRED"},
            {"name": "currency_code",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "refund_status",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "refund_reason_code", "type": "STRING",    "mode": "NULLABLE"},
            {"name": "refund_reason_note", "type": "STRING",    "mode": "NULLABLE"},
            {"name": "requested_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "completed_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at",         "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at",         "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at",       "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system",     "type": "STRING",    "mode": "REQUIRED"},
        ],
        table_columns   = """
            refund_id, transaction_id, provider_refund_id, refund_amount,
            currency_code, refund_status, refund_reason_code, refund_reason_note,
            requested_at, completed_at, created_at, updated_at
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
