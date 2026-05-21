"""
taxi_dbt_bigquery_dag.py
══════════════════════════════════════════════════════════════════════════════
DAG khusus dbt untuk project Taxi dengan target BigQuery.

Desain (Adaptasi Modern TaskFlow API):
  • Menggunakan fungsi _dbt_task (Factory) agar konfigurasi DockerOperator (DRY).
  • Kredensial menggunakan Service Account GCP yang di-mount dari host ke container.
  • Mendukung Full Refresh dan penggantian Target (dev/prod) secara dinamis
    lewat dag_run.conf di Airflow UI.
  • Pemisahan layer (Staging -> Intermediate -> Marts) menggunakan dbt build
    untuk memastikan data kotor tidak lolos ke layer atasnya.

Manual DAG run config opsional:
  {
    "full_refresh": false,
    "target": "dev",
    "threads": 4,
    "vars": "{}"
  }
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ═══════════════════════════════════════════════════════════════════════════════
# Konfigurasi Airflow / Docker / dbt / GCP
# ═══════════════════════════════════════════════════════════════════════════════

DAG_ID = "taxi_dbt_bigquery"
DBT_IMAGE = "dbt-project-ride-hailing:1.0"

# -- Path di Host Machine (Sesuaikan dengan server/VM kamu) --
DBT_PROJECT_HOST_PATH = "/home/void/ride-hailing-docker/dbt/dbt_project"
DBT_PROFILES_HOST_PATH = "/home/void/ride-hailing-docker/dbt/dbt_profiles"
SA_KEY_HOST = os.getenv("SA_KEY_HOST", "/home/void/ride-hailing-docker/credentials/service-account.json")

# -- Path di dalam Container --
DBT_PROJECT_CONTAINER_PATH = "/app"
DBT_PROFILES_CONTAINER_PATH = "/root/.dbt"
SA_KEY_CONTAINER = "/opt/gcp/service-account.json"

# -- Environment Variables untuk Container --
# Nilai seperti DBT_TARGET diambil secara dinamis dari Airflow saat DAG ditrigger
DBT_ENV: dict[str, str] = {
    "GCP_PROJECT_ID": "dbt-taxi-explore",
    "GOOGLE_APPLICATION_CREDENTIALS": SA_KEY_CONTAINER,
    "DBT_PROFILES_DIR": DBT_PROFILES_CONTAINER_PATH,
    "DBT_PROJECT_DIR": DBT_PROJECT_CONTAINER_PATH,
    "DBT_TARGET": "{{ dag_run.conf.get('target', 'dev') if dag_run and dag_run.conf else 'dev' }}",
    "DBT_THREADS": "{{ dag_run.conf.get('threads', 4) if dag_run and dag_run.conf else 4 }}",
    "DBT_FULL_REFRESH": "{{ dag_run.conf.get('full_refresh', true) if dag_run and dag_run.conf else true }}",
    "DBT_VARS": "{{ dag_run.conf.get('vars', '{}') if dag_run and dag_run.conf else '{}' }}",
}

# -- Konfigurasi Mount Volume (Host -> Container) --
DBT_MOUNTS = [
    Mount(
        target=DBT_PROJECT_CONTAINER_PATH,
        source=DBT_PROJECT_HOST_PATH,
        type="bind",
    ),
    Mount(
        target=DBT_PROFILES_CONTAINER_PATH,
        source=DBT_PROFILES_HOST_PATH,
        type="bind",
    ),
    Mount(
        target=SA_KEY_CONTAINER,
        source=SA_KEY_HOST,
        type="bind",
    ),
    # Volume untuk logs dan target (opsional, agar file tidak membebani layer image)
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/target",
        source="dbt_target_vol_taxi",
        type="volume",
    ),
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/logs",
        source="dbt_logs_vol_taxi",
        type="volume",
    ),
]


def _shell_prelude() -> str:
    """
    Shell helper untuk setup sebelum dbt dijalankan.
    Mengecek parameter opsional (seperti full-refresh) & memvalidasi file config.
    """
    return r"""
set -euo pipefail

cd "${DBT_PROJECT_DIR}"

FULL_REFRESH_VALUE="$(echo "${DBT_FULL_REFRESH:-true}" | tr '[:upper:]' '[:lower:]')"
FULL_REFRESH_FLAG=""
if [ "${FULL_REFRESH_VALUE}" = "true" ] || [ "${FULL_REFRESH_VALUE}" = "1" ] || [ "${FULL_REFRESH_VALUE}" = "yes" ]; then
  FULL_REFRESH_FLAG="--full-refresh"
fi

DBT_COMMON_FLAGS="--profiles-dir ${DBT_PROFILES_DIR} --project-dir ${DBT_PROJECT_DIR} --target ${DBT_TARGET}"
DBT_THREADS_FLAG="--threads ${DBT_THREADS:-4}"
DBT_VARS_FLAG="--vars ${DBT_VARS:-'{}'}"

echo "============================================================"
echo "dbt project dir     : ${DBT_PROJECT_DIR}"
echo "dbt profiles dir    : ${DBT_PROFILES_DIR}"
echo "dbt target          : ${DBT_TARGET}"
echo "dbt threads         : ${DBT_THREADS:-4}"
echo "dbt full refresh    : ${FULL_REFRESH_VALUE}"
echo "GCP Project ID      : ${GCP_PROJECT_ID}"
echo "GCP Auth Key Path   : ${GOOGLE_APPLICATION_CREDENTIALS}"
echo "============================================================"

# Validasi keberadaan file sebelum menembak ke BigQuery
test -f "${DBT_PROJECT_DIR}/dbt_project.yml" || { echo "dbt_project.yml not found!"; exit 1; }
test -f "${DBT_PROFILES_DIR}/profiles.yml" || { echo "profiles.yml not found!"; exit 1; }
test -f "${GOOGLE_APPLICATION_CREDENTIALS}" || { echo "GCP Service Account JSON not found!"; exit 1; }
""".strip()


def _dbt_task(task_id: str, command_body: str, retries: int = 1) -> DockerOperator:
    """
    Factory pembungkus DockerOperator untuk Airflow.
    """
    command = f"{_shell_prelude()}\n\n{command_body.strip()}"

    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=["bash", "-lc", command],
        environment=DBT_ENV,
        network_mode="host",                # Menjaga koneksi API Google yang stabil
        docker_url="unix://var/run/docker.sock",
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=DBT_MOUNTS,
        working_dir=DBT_PROJECT_CONTAINER_PATH,
        tty=True,
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=1),
    )


@dag(
    dag_id=DAG_ID,
    description="Run dbt-bigquery transformations for Taxi Data Pipeline",
    schedule=None,             # Terjadwal setiap 2 jam
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "dbt", "bigquery", "analytics"],
    doc_md=__doc__,
    default_args={
        "owner": "data-engineering",
        "email": [os.getenv("ALERT_EMAIL", "data-engineering@company.co.id")],
        "email_on_failure": True,
        "retries": 1,
    },
)
def taxi_dbt_bigquery() -> None:

    dbt_deps = _dbt_task(
        task_id="dbt_deps",
        command_body="dbt deps ${DBT_COMMON_FLAGS}",
    )

    dbt_debug = _dbt_task(
        task_id="dbt_debug",
        command_body="dbt debug ${DBT_COMMON_FLAGS}",
    )

    dbt_build_staging = _dbt_task(
        task_id="dbt_build_staging",
        command_body="""
dbt build \
  --select path:models/staging \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
    )

    dbt_build_intermediate = _dbt_task(
        task_id="dbt_build_intermediate",
        command_body="""
dbt build \
  --select path:models/intermediate \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
    )

    dbt_build_marts = _dbt_task(
        task_id="dbt_build_marts",
        command_body="""
dbt build \
  --select path:models/marts \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
    )

    dbt_docs_generate = _dbt_task(
        task_id="dbt_docs_generate",
        command_body="dbt docs generate ${DBT_COMMON_FLAGS} ${DBT_VARS_FLAG}",
        retries=1,
    )

    # Definisi urutan eksekusi layer (DAG Lineage)
    (
        dbt_deps 
        >> dbt_debug 
        >> dbt_build_staging 
        >> dbt_build_intermediate 
        >> dbt_build_marts 
        >> dbt_docs_generate
    )

# Eksekusi instansiasi DAG
taxi_dbt_bigquery()