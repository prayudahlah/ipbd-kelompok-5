import logging
import pandas as pd
import yfinance as yf
from airflow.sdk import task

from ipbd_kelompok_5.utils.pd_to_parquet_garage import pd_to_parquet_garage

logger = logging.getLogger(__name__)


@task()
def fetch_and_load_oil_prices(**context):
    extraction_at = context["data_interval_start"]
    extraction_at_iso = extraction_at.isoformat()
    ticker = "BZ=F"
    start_date = "2015-01-01"
    end_date = extraction_at.strftime("%Y-%m-%d")

    try:
        brent_df = yf.download(ticker, start=start_date, end=end_date, progress=False)
    except Exception as e:
        logger.error("Gagal fetch data %s dari yfinance: %s", ticker, e)
        return

    if brent_df is None or brent_df.empty:
        logger.warning("Tidak ada data untuk ticker %s.", ticker)
        return

    if isinstance(brent_df.columns, pd.MultiIndex):
        brent_df.columns = brent_df.columns.droplevel(1)
    brent_df = brent_df.reset_index()
    if "Date" in brent_df.columns and "date" not in brent_df.columns:
        brent_df = brent_df.rename(columns={"Date": "date"})

    brent_df["extraction_at"] = extraction_at_iso
    brent_df["ticker"] = ticker

    if "date" in brent_df.columns and not brent_df.empty:
        logger.info(
            "Berhasil tarik data %s: %s baris (%s s/d %s).",
            ticker,
            len(brent_df),
            brent_df["date"].min(),
            brent_df["date"].max(),
        )

    file_name = f"bronze/oil_prices/{extraction_at.strftime('%Y-%m-%d')}.parquet"

    pd_to_parquet_garage(df=brent_df, with_index=True, object_key=file_name)
