import logging
import polars as pl
from airflow.sdk import task

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri
from ipbd_kelompok_5.utils.get_garage_storage_options import get_garage_storage_options

logger = logging.getLogger(__name__)


@task()
def etl_silver_oil_prices(**context):
    dw_uri = get_dw_uri()
    garage_storage_options = get_garage_storage_options()
    extraction_at = context["data_interval_end"]
    path = f"s3://ipbd-kelompok-5/bronze/oil_prices/{extraction_at.strftime('%Y-%m-%d')}.parquet"

    oil_prices = pl.scan_parquet(
        path,
        storage_options=garage_storage_options,
    )

    silver_oil_prices = oil_prices.select(
        pl.col("date").cast(pl.Date),
        pl.col("extraction_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%:z"),
        pl.col("ticker"),
        pl.col("Close").alias("close_price"),
    )

    silver_oil_prices.collect().write_database(
        table_name="silver.oil_prices",
        connection=dw_uri,
        if_table_exists="replace",
        engine="sqlalchemy",
    )
