"""
Airflow DAG: Create Dataproc single-node cluster, submit PySpark jobs,
then delete the cluster.

Use case:
- Development cluster
- Single node Dataproc
- PySpark job from GCS
- Optional BigQuery read/write using built-in Spark BigQuery connector
"""

from datetime import datetime

from airflow import models
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)


# =========================================================
# 1. Main Configuration
# =========================================================

DAG_ID = "dataproc_single_node_dev"

PROJECT_ID = "dbt-taxi-explore"

REGION = "us-central1"
ZONE = "us-central1-b"

CLUSTER_NAME = "cluster-3edd-{{ ds_nodash }}"

BUCKET_NAME = "dataproc-test-111"

SCRIPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/scripts"

SCRIPT_NAME_1 = "pyspark_bq_to_gcs_demo.py"
SCRIPT_NAME_2 = "pts-ark_gcs_to_bq_demo.py"


# =========================================================
# 2. Dataproc Single-Node Cluster Configuration
# =========================================================

CLUSTER_CONFIG = {
    "config_bucket": BUCKET_NAME,

    "gce_cluster_config": {
        "zone_uri": ZONE,
        "service_account_scopes": [
            "https://www.googleapis.com/auth/cloud-platform"
        ],
    },

    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n4-standard-2",
        "disk_config": {
            "boot_disk_type": "hyperdisk-balanced",
            "boot_disk_size_gb": 200,
        },
    },

    # PENTING:
    # Untuk single-node Dataproc via API/Airflow,
    # JANGAN pakai worker_config num_instances=0.
    # Gunakan property dataproc.allow.zero.workers.
    #
    # Jangan tambahkan:
    # "worker_config": {"num_instances": 0}
    #
    # Jangan tambahkan:
    # "secondary_worker_config": {"num_instances": 0}

    "software_config": {
        "image_version": "2.3.30-debian12",
        "properties": {
            # Ini yang membuat cluster menjadi single-node.
            "dataproc:dataproc.allow.zero.workers": "true",

            # Spark tuning untuk development single-node.
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.shuffle.partitions": "8",
            "spark:spark.dynamicAllocation.enabled": "false",
        },
    },

    "endpoint_config": {
        "enable_http_port_access": True,
    },
}


# =========================================================
# 3. PySpark Job Definitions
# =========================================================

PYSPARK_JOB_1 = {
    "reference": {
        "project_id": PROJECT_ID,
    },
    "placement": {
        "cluster_name": CLUSTER_NAME,
    },
    "pyspark_job": {
        "main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_1}",
        "args": [
            "--project_id", PROJECT_ID,
            "--temp_bucket", BUCKET_NAME,
        ],
    },
}


PYSPARK_JOB_2 = {
    "reference": {
        "project_id": PROJECT_ID,
    },
    "placement": {
        "cluster_name": CLUSTER_NAME,
    },
    "pyspark_job": {
        "main_python_file_uri": f"{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_2}",
        "args": [
            "--project_id", PROJECT_ID,
            "--temp_bucket", BUCKET_NAME,
        ],
    },
}


# =========================================================
# 4. DAG Definition
# =========================================================

with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dataproc", "pyspark", "development"],
) as dag:

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    pyspark_task_bq_to_gcs = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_to_gcs",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_1,
    )

    pyspark_task_gcs_to_bq = DataprocSubmitJobOperator(
        task_id="pyspark_task_gcs_to_bq",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_2,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_dataproc_cluster >> [
        pyspark_task_bq_to_gcs,
        pyspark_task_gcs_to_bq,
    ] >> delete_dataproc_cluster