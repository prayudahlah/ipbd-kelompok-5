from airflow.sdk import task_group
from ipbd_kelompok_5.to_bronze.fetch_and_load_ev_trend import fetch_and_load_ev_trend


@task_group(group_id="to_bronze")
def to_bronze():
    fetch_and_load_ev_trend()
