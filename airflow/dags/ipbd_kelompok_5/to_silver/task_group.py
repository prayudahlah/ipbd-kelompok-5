from airflow.sdk import task_group

from ipbd_kelompok_5.to_silver.etl_silver_oil_prices import etl_silver_oil_prices
from ipbd_kelompok_5.to_silver.etl_silver_ev_wiki_trend import (
    etl_silver_ev_wiki_trend,
)
from ipbd_kelompok_5.to_silver.etl_silver_ev_google_trend import (
    etl_silver_ev_google_trend,
)


@task_group(group_id="to_silver")
def to_silver():
    etl_silver_ev_google_trend()
    etl_silver_ev_wiki_trend()
    etl_silver_oil_prices()
