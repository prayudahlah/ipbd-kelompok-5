from airflow.sdk import task_group

from ipbd_kelompok_5.to_gold.create_schema import create_schema
from ipbd_kelompok_5.to_gold.fact_ev_oil_monthly import ev_oil_monthly
from ipbd_kelompok_5.to_gold.fact_even_impact import impact_event
from ipbd_kelompok_5.to_gold.fact_correlation_stats import correlation_stats


@task_group(group_id="to_gold")
def to_gold():
    schema = create_schema()
    oil = ev_oil_monthly()

    schema >> oil >> [impact_event(), correlation_stats()]
