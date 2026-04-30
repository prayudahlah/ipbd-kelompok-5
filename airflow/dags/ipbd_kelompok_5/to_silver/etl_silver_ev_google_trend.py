import logging
import polars as pl
from airflow.sdk import task

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri
from ipbd_kelompok_5.utils.get_garage_storage_options import get_garage_storage_options

logger = logging.getLogger(__name__)


@task()
def etl_silver_ev_google_trend(**context):
    dw_uri = get_dw_uri()
    garage_storage_options = get_garage_storage_options()
    extraction_at = context["data_interval_end"]
    path = f"s3://ipbd-kelompok-5/bronze/ev_google_trend/{extraction_at.strftime('%Y-%m-%d')}.parquet"

    ev_google_trend = pl.scan_parquet(
        path,
        storage_options=garage_storage_options,
    )

    silver_ev_google_trend = ev_google_trend.unpivot(
        index=["date", "extraction_at"],
        on=["Electric Vehicle", "Tesla", "Solid State Battery", "LFP Battery"],
        variable_name="keyword",
        value_name="interest",
    ).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("extraction_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%:z"),
    )

    silver_ev_google_trend.collect().write_database(
        table_name="silver.ev_google_trends",
        connection=dw_uri,
        if_table_exists="replace",
        engine="sqlalchemy",
    )
