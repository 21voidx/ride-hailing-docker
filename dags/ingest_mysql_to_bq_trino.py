from __future__ import annotations

from datetime import timedelta
import os
import pendulum
from airflow.sdk import DAG, Param, chain
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.operators.empty import EmptyOperator
from helpers.refactored_trino_helper_v2 import TableConfig, make_table_task_group

DAG_ID = "ingest_mysql_to_bq_trino_weekly_v3"
SOURCE_TZ = "Asia/Jakarta"
TRINO_CONN_ID = "trino_default"
GCP_CONN_ID = "google_cloud_default"
TRINO_BQ_CAT = "bigquery"
BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "dbt-taxi-explore")
BQ_DATASET = os.getenv("BQ_BRONZE_MYSQL_DATASET", "raw_ride_mysql")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

BASE_LABELS = {"env": "dev", "team": "data-eng", "layer": "bronze", "pipeline": "mysql-trino-bq"}


TABLE_CONFIGS = [
    TableConfig(
        source_table="payment_method",
        bq_final_table="payment_method",
        merge_key='payment_method_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['rider_id', 'method_code', 'payment_method_status'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "payment_method_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "rider_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "method_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "provider_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "masked_account", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_method_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "is_default", "type": "BOOL", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""payment_method_id, rider_id, method_code, provider_name, masked_account, payment_method_status, is_default, created_at, updated_at, deleted_at""",
    ),

    TableConfig(
        source_table="payment_transaction",
        bq_final_table="payment_transaction",
        merge_key='payment_transaction_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['ride_id', 'payment_status', 'provider_name'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "payment_transaction_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "rider_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_method_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "provider_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "provider_transaction_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "idempotency_key", "type": "STRING", "mode": "REQUIRED"},
            {"name": "amount", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "method_fee", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "currency_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "payment_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "failure_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "failure_message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "authorized_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "captured_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "paid_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""payment_transaction_id, ride_id, rider_id, payment_method_id, provider_name, provider_transaction_id, idempotency_key, amount, method_fee, currency_code, payment_status, failure_code, failure_message, authorized_at, captured_at, paid_at, created_at, updated_at, deleted_at""",
    ),

    TableConfig(
        source_table="payment_refund",
        bq_final_table="payment_refund",
        merge_key='refund_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['ride_id', 'refund_status'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "refund_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "payment_transaction_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "refund_amount", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "refund_reason_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "refund_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "requested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "processed_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""refund_id, payment_transaction_id, ride_id, refund_amount, refund_reason_code, refund_status, requested_at, processed_at, created_at, updated_at""",
    ),

    TableConfig(
        source_table="promotion",
        bq_final_table="promotion",
        merge_key='promotion_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['promo_code', 'promotion_status'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "promotion_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "promo_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "promo_description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "discount_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "discount_pct", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "discount_amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "max_discount_amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "min_fare_amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "valid_from", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "valid_to", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "promotion_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""promotion_id, promo_code, promo_description, discount_type, discount_pct, discount_amount, max_discount_amount, min_fare_amount, valid_from, valid_to, promotion_status, created_at, updated_at, deleted_at""",
    ),

    TableConfig(
        source_table="promo_usage",
        bq_final_table="promo_usage",
        merge_key='promo_usage_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['promotion_id', 'ride_id'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "promo_usage_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "promotion_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "rider_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "discount_amount_applied", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "used_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""promo_usage_id, promotion_id, ride_id, rider_id, discount_amount_applied, used_at, created_at, updated_at""",
    ),

    TableConfig(
        source_table="review",
        bq_final_table="review",
        merge_key='review_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['ride_id', 'review_status', 'rating_score'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "review_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "reviewer_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "reviewer_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "reviewee_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "reviewee_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "rating_score", "type": "INT64", "mode": "REQUIRED"},
            {"name": "comments", "type": "STRING", "mode": "NULLABLE"},
            {"name": "review_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""review_id, ride_id, reviewer_type, reviewer_id, reviewee_type, reviewee_id, rating_score, comments, review_status, created_at, updated_at, deleted_at""",
    ),

    TableConfig(
        source_table="support_ticket",
        bq_final_table="support_ticket",
        merge_key='ticket_id',
        partition_field="updated_at",
        partition_type="DAY",
        cluster_fields=['ticket_status', 'priority', 'ticket_category'],
        source_catalog="mysql",
        source_schema="billing_growth_db",
        source_system="billing_growth_db",
        schema_fields=[
            {"name": "ticket_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "ride_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "rider_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "driver_id", "type": "INT64", "mode": "NULLABLE"},
            {"name": "ticket_category", "type": "STRING", "mode": "REQUIRED"},
            {"name": "ticket_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "priority", "type": "STRING", "mode": "REQUIRED"},
            {"name": "opened_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "resolved_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "deleted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "_source_system", "type": "STRING", "mode": "REQUIRED"}
        ],
        table_columns="""ticket_id, ride_id, rider_id, driver_id, ticket_category, ticket_status, priority, opened_at, resolved_at, created_at, updated_at, deleted_at""",
    )
]



default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    description="Batch upsert MySQL source tables to BigQuery bronze via Trino.",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 10, 1, tz="Asia/Jakarta"),
    # end_date=pendulum.datetime(2024, 3, 2, tz="Asia/Jakarta"),
    schedule=CronDataIntervalTimetable("0 0 * * 0", timezone=SOURCE_TZ),
    catchup=False,
    max_active_runs=2,
    tags=["ride-hailing", "bronze", "mysql", "trino", "bigquery"],
    params={
        "window_start": Param(default=None, type=["null", "string"]),
        "window_end": Param(default=None, type=["null", "string"]),
    },
) as dag:
    
    groups = [
        make_table_task_group(
            cfg,
            bq_project=BQ_PROJECT,
            bq_dataset=BQ_DATASET,
            bq_location=BQ_LOCATION,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            trino_bq_cat=TRINO_BQ_CAT,
            source_tz=SOURCE_TZ,
            dag_labels=BASE_LABELS,
        )
        for cfg in TABLE_CONFIGS
    ]
    chain(*groups)
