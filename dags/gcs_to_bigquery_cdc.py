from __future__ import annotations

import os
from datetime import datetime

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.timetables.interval import CronDataIntervalTimetable



# ============================================================
# CONFIG
# ============================================================

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dbt-taxi-explore")

BUCKET_NAME = "dbt-taxi-explore-bucket"

EVENT_DATASET = "dev_bronze_cdc_events"
CURRENT_DATASET = "dev_bronze_cdc_current"

BQ_LOCATION = os.getenv("BQ_LOCATION", "us")
GCP_CONN_ID = "google_cloud_default"

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")


# ============================================================
# CDC TABLE CONFIG
# ============================================================

CDC_TABLES = {
    "ride": {
        "topic": "cdc.public.ride",
        "event_table": "ride_events",
        "current_table": "ride",
        "pk": "ride_id",
        "cluster_fields": ["ride_id", "driver_id", "ride_status"],
    },
    "driver_profile": {
        "topic": "cdc.public.driver_profile",
        "event_table": "driver_profile_events",
        "current_table": "driver_profile",
        "pk": "driver_id",
        "cluster_fields": ["driver_id", "driver_status", "verification_status"],
    },
    "payment_transaction": {
        "topic": "cdc.public.payment_transaction",
        "event_table": "payment_transaction_events",
        "current_table": "payment_transaction",
        "pk": "transaction_id",
        "cluster_fields": ["transaction_id", "ride_id", "payment_status"],
    },
}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_process_hour(context: dict) -> pendulum.DateTime:
    """
    DAG berjalan setiap jam di menit ke-10.
    Yang diproses adalah folder GCS untuk jam sebelumnya.

    Contoh:
    - DAG run: 2026-05-05 18:10 Asia/Jakarta
    - Process hour: 2026-05-05 17:00 Asia/Jakarta
    - GCS path: year=2026/month=05/day=05/hour=17
    """

    data_interval_end = context["data_interval_end"].in_timezone(LOCAL_TZ)
    return data_interval_end.subtract(hours=1)


def build_gcs_prefix(table_name: str, context: dict) -> str:
    table_conf = CDC_TABLES[table_name]
    topic = table_conf["topic"]

    process_hour = get_process_hour(context)

    return (
        f"raw/cdc/{topic}/"
        f"year={process_hour.strftime('%Y')}/"
        f"month={process_hour.strftime('%m')}/"
        f"day={process_hour.strftime('%d')}/"
        f"hour={process_hour.strftime('%H')}"
    )


def build_bq_partition_suffix(context: dict) -> str:
    """
    BigQuery partition decorator untuk hourly ingestion-time partition
    memakai UTC.

    GCS Sink memakai timezone Asia/Jakarta untuk path:
      year=YYYY/month=MM/day=DD/hour=HH

    Contoh:
      GCS hour=17 Asia/Jakarta
      = 10 UTC
      partition decorator = YYYYMMDD10
    """

    process_hour_jakarta = get_process_hour(context)
    process_hour_utc = process_hour_jakarta.in_timezone("UTC")

    return process_hour_utc.strftime("%Y%m%d%H")


def check_gcs_files(table_name: str, **context) -> None:
    """
    Cek apakah prefix GCS berisi file Parquet.
    Jika kosong, task tabel tersebut di-skip agar DAG tidak gagal semua.
    """

    prefix = build_gcs_prefix(table_name, context)
    partition_suffix = build_bq_partition_suffix(context)

    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

    objects = hook.list(
        bucket_name=BUCKET_NAME,
        prefix=prefix,
    ) or []

    parquet_objects = [
        obj for obj in objects
        if obj.endswith(".parquet")
    ]

    if not parquet_objects:
        raise AirflowSkipException(
            f"Tidak ada file Parquet untuk table={table_name}, "
            f"prefix=gs://{BUCKET_NAME}/{prefix}/"
        )

    source_object = f"{prefix}/*.parquet"

    context["ti"].xcom_push(
        key="source_object",
        value=source_object,
    )

    context["ti"].xcom_push(
        key="bq_partition_suffix",
        value=partition_suffix,
    )

    print(f"Table: {table_name}")
    print(f"GCS prefix: gs://{BUCKET_NAME}/{prefix}/")
    print(f"Source object: gs://{BUCKET_NAME}/{source_object}")
    print(f"BigQuery partition suffix: {partition_suffix}")
    print(f"Found {len(parquet_objects)} parquet file(s):")

    for obj in parquet_objects:
        print(f" - gs://{BUCKET_NAME}/{obj}")


def build_current_view_sql(table_name: str) -> str:
    conf = CDC_TABLES[table_name]

    event_table = conf["event_table"]
    current_table = conf["current_table"]
    pk = conf["pk"]

    source_table = f"`{PROJECT_ID}.{EVENT_DATASET}.{event_table}`"
    target_view = f"`{PROJECT_ID}.{CURRENT_DATASET}.{current_table}`"

    return f"""
    CREATE OR REPLACE VIEW {target_view} AS

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
                PARTITION BY `{pk}`
                ORDER BY
                    SAFE_CAST(`__source_ts_ms` AS INT64) DESC,
                    SAFE_CAST(`__lsn` AS INT64) DESC,
                    _PARTITIONTIME DESC
            ) AS row_num

        FROM {source_table} e

    )

    SELECT * EXCEPT(row_num)
    FROM ranked
    WHERE row_num = 1
      AND COALESCE(CAST(`__op` AS STRING), '') != 'd';
    """


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="cdc_gcs_to_bq",
    description=(
        "Load CDC Parquet from GCS to manually-created BigQuery "
        "hourly partitioned event tables with schema evolution support, "
        "then refresh current/latest views."
    ),
    start_date=datetime(2026, 5, 5, 16, 0, 0, tzinfo=LOCAL_TZ),
    schedule=CronDataIntervalTimetable("10 * * * *", timezone="Asia/Jakarta"),
    catchup=True,
    max_active_runs=2,
    tags=[
        "ride-hailing",
        "cdc",
        "gcs",
        "bigquery",
        "schema-evolution",
        "backfill-safe",
    ],
) as dag:

    for table_name, conf in CDC_TABLES.items():

        check_files = PythonOperator(
            task_id=f"check_gcs_files_{table_name}",
            python_callable=check_gcs_files,
            op_kwargs={
                "table_name": table_name,
            },
        )

        load_events_to_partition = GCSToBigQueryOperator(
            task_id=f"load_{table_name}_events_to_bq_partition",
            bucket=BUCKET_NAME,
            source_objects=[
                "{{ ti.xcom_pull(task_ids='check_gcs_files_"
                + table_name
                + "', key='source_object') }}"
            ],
            destination_project_dataset_table=(
                f"{PROJECT_ID}.{EVENT_DATASET}.{conf['event_table']}$"
                "{{ ti.xcom_pull(task_ids='check_gcs_files_"
                + table_name
                + "', key='bq_partition_suffix') }}"
            ),
            source_format="PARQUET",

            # Retry/backfill aman:
            # partition hour yang sama ditimpa ulang, bukan append ulang.
            write_disposition="WRITE_TRUNCATE",

            # Tabel harus sudah dibuat manual lewat DDL.
            create_disposition="CREATE_NEVER",

            # Penting untuk schema evolution dari Parquet.
            # BigQuery akan membaca schema dari file Parquet.
            autodetect=True,

            # Penting:
            # Mendukung penambahan kolom nullable dan relaxation.
            # Berlaku karena destination memakai partition decorator.
            schema_update_options=[
                "ALLOW_FIELD_ADDITION",
                "ALLOW_FIELD_RELAXATION",
            ],

            # Target table harus hourly ingestion-time partitioned.
            time_partitioning={
                "type": "HOUR",
            },

            cluster_fields=conf["cluster_fields"],

            location=BQ_LOCATION,
            gcp_conn_id=GCP_CONN_ID,

            # Jangan False, karena Anda butuh backfill.
            # Idempotency dijaga oleh WRITE_TRUNCATE ke partition decorator.
            force_rerun=True,
        )

        create_or_replace_current_view = BigQueryInsertJobOperator(
            task_id=f"create_or_replace_current_view_{table_name}",
            configuration={
                "query": {
                    "query": build_current_view_sql(table_name),
                    "useLegacySql": False,
                }
            },
            location=BQ_LOCATION,
            gcp_conn_id=GCP_CONN_ID,
        )

        check_files >> load_events_to_partition >> create_or_replace_current_view