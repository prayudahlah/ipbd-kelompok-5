import time
import logging
import pandas as pd
from airflow.sdk import task
from pytrends.request import TrendReq
from ipbd_kelompok_5.utils.pd_to_parquet_garage import pd_to_parquet_garage

logger = logging.getLogger(__name__)


@task()
def fetch_and_load_ev_trend(**context):
    extraction_at = context["data_interval_start"]
    timeframe = "today 12-m"
    keywords = ["Electric Vehicle", "Tesla", "Solid State Battery", "LFP Battery"]

    df_trends = fetch_ev_trend(extraction_at, timeframe, keywords)

    if not df_trends.empty:
        file_name = f"bronze/ev_trend/{extraction_at.strftime('%Y-%m-%d')}.parquet"

        pd_to_parquet_garage(df=df_trends, with_index=True, object_key=file_name)
    else:
        logger.warning("Tidak ada data untuk diunggah.")


def fetch_ev_trend(
    extraction_at: str, timeframe: str, keywords: list[str]
) -> pd.DataFrame:
    pytrends = TrendReq(
        hl="id-ID",
        tz=420,
        timeout=(10, 25),
        retries=3,
        backoff_factor=0.5,  # type: ignore
    )

    logger.info(f"Menarik data Google Trends untuk kata kunci berikut: {keywords}...")

    dfs = []
    for kw in keywords:
        try:
            # Fetch 12 bulan ke belakang
            pytrends.build_payload([kw], timeframe=timeframe)
            df_temp = pytrends.interest_over_time()

            if df_temp.empty:
                logger.warning(f"Data untuk '{kw}' kosong!")

            # Buang kolom 'isPartial' bawaan Google Trends
            if "isPartial" in df_temp.columns:
                df_temp = df_temp.drop(columns=["isPartial"])

            dfs.append(df_temp[kw])
            logger.info(f"Data untuk '{kw}' berhasil ditarik!")

        except Exception as e:
            logger.error(f"Terjadi error: {e}")

        time.sleep(5)

    df_trends = pd.concat(dfs, axis=1)
    df_trends["extraction_at"] = str(extraction_at)

    return df_trends
