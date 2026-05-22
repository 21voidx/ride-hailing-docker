"""
ride_hailing_dbt_bigquery_dag.py
══════════════════════════════════════════════════════════════════════════════
DAG khusus dbt-bigquery untuk project Ride-Hailing Analytics.

Project dbt yang dituju:
  • profile              : ride_hailing
  • BigQuery project     : dbt-taxi-explore
  • batch bronze dataset : dev_bronze_pg
  • CDC bronze dataset   : dev_bronze_cdc_events
  • Looker reporting     : models/reporting/looker

Manual DAG run config opsional:
  {
    "target": "dev",
    "threads": 4,
    "full_refresh": false,
    "vars": "{cdc_lookback_hours: 720, incremental_lookback_days: 3}",
    "source_freshness": true,
    "build_reporting": true,
    "docs_generate": true
  }

Catatan:
  • full_refresh default dibuat false agar scheduled run tidak selalu rebuild penuh.
  • Source freshness bisa dimatikan lewat dag_run.conf jika bronze belum stabil.
  • Seed task dibuat aman: kalau folder seeds kosong/tidak ada, task akan skip.
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

DAG_ID = "ride_hailing_dbt_bigquery"
DBT_IMAGE = os.getenv("DBT_IMAGE", "dbt-project-ride-hailing:1.0")

# Path di Host Machine. Sesuaikan jika folder project kamu berbeda.
DBT_PROJECT_HOST_PATH = os.getenv(
    "DBT_PROJECT_HOST_PATH",
    "/home/void/ride-hailing-docker/dbt/dbt_project",
)
DBT_PROFILES_HOST_PATH = os.getenv(
    "DBT_PROFILES_HOST_PATH",
    "/home/void/ride-hailing-docker/dbt/dbt_profiles",
)
SA_KEY_HOST = os.getenv(
    "SA_KEY_HOST",
    "/home/void/ride-hailing-docker/credentials/service-account.json",
)

# Path di dalam container dbt.
DBT_PROJECT_CONTAINER_PATH = os.getenv("DBT_PROJECT_CONTAINER_PATH", "/app")
DBT_PROFILES_CONTAINER_PATH = os.getenv("DBT_PROFILES_CONTAINER_PATH", "/root/.dbt")
SA_KEY_CONTAINER = os.getenv("SA_KEY_CONTAINER", "/opt/gcp/service-account.json")

# Nama profile dan project mengikuti dbt_project.yml / profiles.yml.example.
DBT_PROFILE_NAME = "ride_hailing"
GCP_PROJECT_ID = "dbt-taxi-explore"

# Environment variables yang dikirim ke container.
# Semua nilai dinamis bisa dioverride dari Airflow UI saat trigger manual.
DBT_ENV: dict[str, str] = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "GOOGLE_APPLICATION_CREDENTIALS": SA_KEY_CONTAINER,
    "DBT_PROFILES_DIR": DBT_PROFILES_CONTAINER_PATH,
    "DBT_PROJECT_DIR": DBT_PROJECT_CONTAINER_PATH,
    "DBT_PROFILE_NAME": DBT_PROFILE_NAME,
    "DBT_TARGET": "{{ dag_run.conf.get('target', 'dev') if dag_run and dag_run.conf else 'dev' }}",
    "DBT_THREADS": "{{ dag_run.conf.get('threads', 4) if dag_run and dag_run.conf else 4 }}",
    "DBT_FULL_REFRESH": "{{ dag_run.conf.get('full_refresh', false) if dag_run and dag_run.conf else false }}",
    "DBT_VARS": "{{ dag_run.conf.get('vars', '{}') if dag_run and dag_run.conf else '{}' }}",
    "DBT_RUN_SOURCE_FRESHNESS": "{{ dag_run.conf.get('source_freshness', true) if dag_run and dag_run.conf else true }}",
    "DBT_BUILD_REPORTING": "{{ dag_run.conf.get('build_reporting', true) if dag_run and dag_run.conf else true }}",
    "DBT_DOCS_GENERATE": "{{ dag_run.conf.get('docs_generate', true) if dag_run and dag_run.conf else true }}",
}

# Konfigurasi mount volume host -> container.
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
        read_only=True,
    ),
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/target",
        source="dbt_target_vol_ride_hailing",
        type="volume",
    ),
    Mount(
        target=f"{DBT_PROJECT_CONTAINER_PATH}/logs",
        source="dbt_logs_vol_ride_hailing",
        type="volume",
    ),
]


def _shell_prelude() -> str:
    """
    Setup shell sebelum dbt dijalankan.
    Bagian ini sengaja dibuat reusable agar semua task punya validasi yang sama.
    """
    return r"""
set -euo pipefail

cd "${DBT_PROJECT_DIR}"

FULL_REFRESH_VALUE="$(echo "${DBT_FULL_REFRESH:-false}" | tr '[:upper:]' '[:lower:]')"
FULL_REFRESH_FLAG=""
if [ "${FULL_REFRESH_VALUE}" = "true" ] || [ "${FULL_REFRESH_VALUE}" = "1" ] || [ "${FULL_REFRESH_VALUE}" = "yes" ]; then
  FULL_REFRESH_FLAG="--full-refresh"
fi

DBT_COMMON_FLAGS="--profiles-dir ${DBT_PROFILES_DIR} --project-dir ${DBT_PROJECT_DIR} --target ${DBT_TARGET}"
DBT_THREADS_FLAG="--threads ${DBT_THREADS:-4}"
DBT_VARS_FLAG="--vars ${DBT_VARS:-'{}'}"

RUN_SOURCE_FRESHNESS_VALUE="$(echo "${DBT_RUN_SOURCE_FRESHNESS:-true}" | tr '[:upper:]' '[:lower:]')"
BUILD_REPORTING_VALUE="$(echo "${DBT_BUILD_REPORTING:-true}" | tr '[:upper:]' '[:lower:]')"
DOCS_GENERATE_VALUE="$(echo "${DBT_DOCS_GENERATE:-true}" | tr '[:upper:]' '[:lower:]')"

echo "============================================================"
echo "dbt project dir         : ${DBT_PROJECT_DIR}"
echo "dbt profiles dir        : ${DBT_PROFILES_DIR}"
echo "dbt profile             : ${DBT_PROFILE_NAME}"
echo "dbt target              : ${DBT_TARGET}"
echo "dbt threads             : ${DBT_THREADS:-4}"
echo "dbt full refresh        : ${FULL_REFRESH_VALUE}"
echo "dbt vars                : ${DBT_VARS:-'{}'}"
echo "source freshness        : ${RUN_SOURCE_FRESHNESS_VALUE}"
echo "build reporting         : ${BUILD_REPORTING_VALUE}"
echo "docs generate           : ${DOCS_GENERATE_VALUE}"
echo "GCP Project ID          : ${GCP_PROJECT_ID}"
echo "GCP Auth Key Path       : ${GOOGLE_APPLICATION_CREDENTIALS}"
echo "============================================================"

# Validasi file wajib sebelum menjalankan dbt.
test -f "${DBT_PROJECT_DIR}/dbt_project.yml" || { echo "dbt_project.yml not found"; exit 1; }
test -f "${DBT_PROFILES_DIR}/profiles.yml" || { echo "profiles.yml not found"; exit 1; }
test -f "${GOOGLE_APPLICATION_CREDENTIALS}" || { echo "GCP Service Account JSON not found"; exit 1; }

# Validasi bahwa DAG ini memang menunjuk ke project dbt yang benar.
grep -q "name: ride_hailing" "${DBT_PROJECT_DIR}/dbt_project.yml" \
  || { echo "Wrong dbt project. Expected name: ride_hailing"; exit 1; }
grep -q "profile: ride_hailing" "${DBT_PROJECT_DIR}/dbt_project.yml" \
  || { echo "Wrong dbt profile. Expected profile: ride_hailing"; exit 1; }
""".strip()


def _dbt_task(task_id: str, command_body: str, retries: int = 1, timeout_hours: int = 1) -> DockerOperator:
    """
    Factory DockerOperator agar konfigurasi dbt tetap DRY.
    """
    command = f"{_shell_prelude()}\n\n{command_body.strip()}"

    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=["bash", "-lc", command],
        environment=DBT_ENV,
        network_mode="host",
        docker_url="unix://var/run/docker.sock",
        auto_remove="force",
        mount_tmp_dir=False,
        mounts=DBT_MOUNTS,
        working_dir=DBT_PROJECT_CONTAINER_PATH,
        tty=True,
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=timeout_hours),
    )


@dag(
    dag_id=DAG_ID,
    description="Run dbt-bigquery transformations for Ride-Hailing Analytics and Looker reporting",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ride-hailing", "dbt", "bigquery", "looker", "analytics"],
    doc_md=__doc__,
    default_args={
        "owner": "data-engineering",
        "email": [os.getenv("ALERT_EMAIL", "data-engineering@company.co.id")],
        "email_on_failure": False,
        "retries": 1,
    },
)
def ride_hailing_dbt_bigquery() -> None:
    dbt_deps = _dbt_task(
        task_id="dbt_deps",
        command_body="dbt deps ${DBT_COMMON_FLAGS}",
    )

    dbt_debug = _dbt_task(
        task_id="dbt_debug",
        command_body="dbt debug ${DBT_COMMON_FLAGS}",
    )

    dbt_source_freshness = _dbt_task(
        task_id="dbt_source_freshness",
        command_body=r"""
if [ "${RUN_SOURCE_FRESHNESS_VALUE}" = "true" ] || [ "${RUN_SOURCE_FRESHNESS_VALUE}" = "1" ] || [ "${RUN_SOURCE_FRESHNESS_VALUE}" = "yes" ]; then
  dbt source freshness \
    ${DBT_COMMON_FLAGS} \
    ${DBT_VARS_FLAG}
else
  echo "Skipping dbt source freshness because source_freshness=false"
fi
""",
        retries=1,
    )

    dbt_seed_optional = _dbt_task(
        task_id="dbt_seed_optional",
        command_body=r"""
if [ -d "${DBT_PROJECT_DIR}/seeds" ] && find "${DBT_PROJECT_DIR}/seeds" -type f -name '*.csv' | grep -q .; then
  dbt seed \
    ${DBT_COMMON_FLAGS} \
    ${DBT_THREADS_FLAG} \
    ${DBT_VARS_FLAG} \
    ${FULL_REFRESH_FLAG}
else
  echo "No CSV seed files found. Skipping dbt seed."
fi
""",
        retries=1,
    )

    dbt_build_staging = _dbt_task(
        task_id="dbt_build_staging",
        command_body=r"""
dbt build \
  --selector staging \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
    )

    dbt_build_intermediate = _dbt_task(
        task_id="dbt_build_intermediate",
        command_body=r"""
dbt build \
  --selector intermediate \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
        timeout_hours=2,
    )

    dbt_build_marts = _dbt_task(
        task_id="dbt_build_marts",
        command_body=r"""
dbt build \
  --selector marts \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG} \
  ${FULL_REFRESH_FLAG}
""",
        retries=2,
        timeout_hours=2,
    )

    dbt_build_reporting = _dbt_task(
        task_id="dbt_build_reporting",
        command_body=r"""
if [ "${BUILD_REPORTING_VALUE}" = "true" ] || [ "${BUILD_REPORTING_VALUE}" = "1" ] || [ "${BUILD_REPORTING_VALUE}" = "yes" ]; then
  dbt build \
    --selector reporting \
    ${DBT_COMMON_FLAGS} \
    ${DBT_THREADS_FLAG} \
    ${DBT_VARS_FLAG} \
    ${FULL_REFRESH_FLAG}
else
  echo "Skipping reporting layer because build_reporting=false"
fi
""",
        retries=2,
        timeout_hours=2,
    )

    dbt_test_exposure_parents = _dbt_task(
        task_id="dbt_test_exposure_parents",
        command_body=r"""
dbt test \
  --select +exposure:ride_hailing_operations_revenue_dashboard \
  ${DBT_COMMON_FLAGS} \
  ${DBT_THREADS_FLAG} \
  ${DBT_VARS_FLAG}
""",
        retries=1,
    )

    dbt_docs_generate = _dbt_task(
        task_id="dbt_docs_generate",
        command_body=r"""
if [ "${DOCS_GENERATE_VALUE}" = "true" ] || [ "${DOCS_GENERATE_VALUE}" = "1" ] || [ "${DOCS_GENERATE_VALUE}" = "yes" ]; then
  dbt docs generate \
    ${DBT_COMMON_FLAGS} \
    ${DBT_VARS_FLAG}
else
  echo "Skipping docs generate because docs_generate=false"
fi
""",
        retries=1,
    )

    (
        dbt_deps
        >> dbt_debug
        >> dbt_source_freshness
        >> dbt_seed_optional
        >> dbt_build_staging
        >> dbt_build_intermediate
        >> dbt_build_marts
        >> dbt_build_reporting
        >> dbt_test_exposure_parents
        >> dbt_docs_generate
    )


ride_hailing_dbt_bigquery()
