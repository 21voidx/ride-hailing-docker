from datetime import datetime
import logging

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Setup logger
logger = logging.getLogger("airflow.task")

@dag(
    dag_id="test_gcp_connection",
    schedule=None, # Dijalankan secara manual (trigger)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'gcp', 'infrastructure']
)
def test_gcp_connection_dag():

    @task
    def check_gcp_auth():
        """
        Task ini akan mencoba melakukan autentikasi ke GCP menggunakan 
        connection 'google_cloud_default' dan mencetak Project ID.
        """
        try:
            # Inisialisasi GCSHook
            # Secara default akan mencari Airflow Connection bernama 'google_cloud_default'
            hook = GCSHook(gcp_conn_id='google_cloud_default')
            
            # Mendapatkan Project ID dari Service Account yang dikonfigurasi
            project_id = hook.project_id
            logger.info(f"✅ Autentikasi Sukses! Terhubung ke GCP Project: {project_id}")
            
            # Opsional: Memanggil API GCP untuk memastikan permission (akses baca)
            # client = hook.get_conn()
            # buckets = list(client.list_buckets(max_results=5))
            # logger.info(f"✅ Berhasil menarik data! Ditemukan {len(buckets)} bucket.")
            
            return project_id
            
        except Exception as e:
            logger.error(f"❌ Gagal terhubung ke GCP: {str(e)}")
            raise e

    check_gcp_auth()

# Register DAG
test_gcp_connection_dag()