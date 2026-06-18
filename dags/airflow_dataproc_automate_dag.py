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

# Bucket untuk Dataproc staging/temp dan script PySpark
BUCKET_NAME = "dataproc-test-111"

# Lokasi script PySpark di GCS
SCRIPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/scripts"

SCRIPT_NAME_1 = "pyspark_bq_to_gcs_demo.py"
SCRIPT_NAME_2 = "pts-ark_gcs_to_bq_demo.py"


# =========================================================
# 2. Dataproc Cluster Configuration
# =========================================================
# Konfigurasi ini dibuat manual agar lebih jelas dan sesuai
# dengan setting cluster kamu di GCP Console.
#
# Region      : us-central1
# Zone        : us-central1-b
# Image       : 2.3.30-debian12
# Master      : 1 node
# Worker      : 0 node
# Machine     : n4-standard-2
# Disk type   : hyperdisk-balanced
# Disk size   : 200GB
# Autoscaling : Off
# Metastore   : None

CLUSTER_CONFIG = {
    "config_bucket": BUCKET_NAME,

    "gce_cluster_config": {
        "zone_uri": ZONE,

        # Optional but recommended if PySpark needs GCS, BigQuery, etc.
        # Bisa dihapus kalau environment kamu sudah pakai custom service account
        # dengan permission yang benar.
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

    # Single-node cluster: 0 workers
    "worker_config": {
        "num_instances": 0,
    },

    "software_config": {
        "image_version": "2.3.30-debian12",

        # Untuk image Dataproc 2.1+, Spark BigQuery connector sudah pre-installed.
        # Jadi INIT_FILE connectors.sh tidak wajib.
        #
        # Kalau nanti kamu ingin force versi connector tertentu, aktifkan metadata
        # melalui gce_cluster_config["metadata"] atau submit job dengan jar.
        "properties": {
            # Development tuning: kecilkan shuffle partition agar tidak terlalu berat
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.shuffle.partitions": "8",

            # Optional: supaya dynamic allocation tidak terlalu agresif di single node
            "spark:spark.dynamicAllocation.enabled": "false",
        },

        # Kalau kamu ingin Jupyter di cluster, aktifkan ini:
        # "optional_components": ["JUPYTER"],
    },

    # Kalau pakai Jupyter / Spark UI via browser, aktifkan Component Gateway.
    # Aman untuk dev, tapi pastikan IAM/network kamu benar.
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

        # Optional arguments untuk script PySpark kamu
        # Hapus kalau script tidak butuh args.
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

        # Optional arguments untuk script PySpark kamu
        # Hapus kalau script tidak butuh args.
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

    # -----------------------------------------------------
    # Task 1: Create Dataproc Cluster
    # -----------------------------------------------------
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # -----------------------------------------------------
    # Task 2A: Submit PySpark Job 1
    # Example: BigQuery -> Transform -> GCS
    # -----------------------------------------------------
    pyspark_task_bq_to_gcs = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_to_gcs",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_1,
    )

    # -----------------------------------------------------
    # Task 2B: Submit PySpark Job 2
    # Example: GCS -> Transform -> BigQuery
    # -----------------------------------------------------
    pyspark_task_gcs_to_bq = DataprocSubmitJobOperator(
        task_id="pyspark_task_gcs_to_bq",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB_2,
    )

    # -----------------------------------------------------
    # Task 3: Delete Cluster
    # -----------------------------------------------------
    # ALL_DONE penting agar cluster tetap dihapus walaupun
    # salah satu PySpark job gagal.
    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -----------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------
    create_dataproc_cluster >> [
        pyspark_task_bq_to_gcs,
        pyspark_task_gcs_to_bq,
    ] >> delete_dataproc_cluster