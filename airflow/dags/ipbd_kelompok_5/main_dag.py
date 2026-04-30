from datetime import datetime
from airflow.sdk import DAG

from ipbd_kelompok_5.to_bronze.task_group import to_bronze

with DAG(
    dag_id="ipbd-kelompok-5-pipeline",
    start_date=datetime(2015, 1, 1),
    catchup=False,
    tags=["kelompok-5", "medallion", "ipbd"],
) as dag:
    bronze = to_bronze()

    bronze
