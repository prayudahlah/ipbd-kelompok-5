from airflow.sdk import task_group
from ipbd_kelompok_5.to_bronze.fetch_and_load_ev_wiki_trend import (
    fetch_and_load_ev_wiki_trend,
)
from ipbd_kelompok_5.to_bronze.fetch_and_load_ev_google_trend import (
    fetch_and_load_ev_google_trend,
)
from ipbd_kelompok_5.to_bronze.fetch_and_load_oil_prices import (
    fetch_and_load_oil_prices,
)


@task_group(group_id="to_bronze")
def to_bronze():
    fetch_and_load_ev_google_trend()
    fetch_and_load_oil_prices()
    fetch_and_load_ev_wiki_trend()
