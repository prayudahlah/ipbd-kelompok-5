import logging
import polars as pl
from airflow.sdk import task

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri
from ipbd_kelompok_5.utils.get_garage_storage_options import get_garage_storage_options

logger = logging.getLogger(__name__)


@task()
def etl_silver_ev_wiki_trend(**context):
    dw_uri = get_dw_uri()
    garage_storage_options = get_garage_storage_options()
    extraction_at = context["data_interval_end"]
    path = f"s3://ipbd-kelompok-5/bronze/ev_wiki_trend/{extraction_at.strftime('%Y-%m-%d')}.parquet"

    ev_wiki_trend = pl.scan_parquet(
        path,
        storage_options=garage_storage_options,
    )

    silver_ev_wiki_trend = ev_wiki_trend.select(
        pl.col("date").str.slice(0, 8).str.strptime(pl.Date, "%Y%m%d"),
        pl.col("extraction_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%:z"),
        pl.col("article"),
        pl.col("views"),
    )

    silver_ev_wiki_trend.collect().write_database(
        table_name="silver.ev_wiki_trends",
        connection=dw_uri,
        if_table_exists="replace",
        engine="sqlalchemy",
    )
