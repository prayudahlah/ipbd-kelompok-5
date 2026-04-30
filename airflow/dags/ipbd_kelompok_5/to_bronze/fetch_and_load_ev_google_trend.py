import time
import logging
import pandas as pd
from airflow.sdk import task
from pytrends.request import TrendReq
from ipbd_kelompok_5.utils.get_env_int import get_env_int
from ipbd_kelompok_5.utils.pd_to_parquet_garage import pd_to_parquet_garage

logger = logging.getLogger(__name__)

DEFAULT_SLEEP_SECONDS = 3
MAX_KEYWORD_RETRIES = 2


@task()
def fetch_and_load_ev_google_trend(**context):
    extraction_at = context["data_interval_start"]
    extraction_at_iso = extraction_at.isoformat()
    start_date = "2015-01-01"
    end_date = extraction_at.strftime("%Y-%m-%d")
    timeframe = f"{start_date} {end_date}"
    keywords = ["Electric Vehicle", "Tesla", "Solid State Battery", "LFP Battery"]

    df_trends = fetch_ev_trend(extraction_at_iso, timeframe, keywords)

    if df_trends.empty:
        logger.warning("Tidak ada data untuk diunggah.")
        return

    file_name = f"bronze/ev_google_trend/{extraction_at.strftime('%Y-%m-%d')}.parquet"
    pd_to_parquet_garage(df=df_trends, with_index=True, object_key=file_name)


def fetch_ev_trend(
    extraction_at_iso: str,
    timeframe: str,
    keywords: list[str],
) -> pd.DataFrame:
    pytrends = TrendReq(
        hl="id-ID",
        tz=420,
        timeout=(10, 25),
        retries=3,
        backoff_factor=0.5,  # type: ignore
    )

    logger.info("Menarik data Google Trends untuk kata kunci berikut: %s", keywords)

    sleep_seconds = get_env_int("GOOGLE_TRENDS_SLEEP_SECONDS", DEFAULT_SLEEP_SECONDS)
    max_keyword_retries = get_env_int(
        "GOOGLE_TRENDS_MAX_KEYWORD_RETRIES",
        MAX_KEYWORD_RETRIES,
    )

    dfs = []
    for kw in keywords:
        success = False
        for attempt in range(1, max_keyword_retries + 2):
            try:
                pytrends.build_payload([kw], timeframe=timeframe)
                df_temp = pytrends.interest_over_time()

                if df_temp.empty:
                    logger.warning("Data untuk '%s' kosong!", kw)
                    success = True
                    break

                # Buang kolom 'isPartial' bawaan Google Trends
                if "isPartial" in df_temp.columns:
                    df_temp = df_temp.drop(columns=["isPartial"])

                dfs.append(df_temp[kw])
                logger.info("Data untuk '%s' berhasil ditarik!", kw)
                success = True
                break

            except Exception as e:
                logger.error(
                    "Gagal tarik data keyword '%s' (attempt %s/%s): %s",
                    kw,
                    attempt,
                    max_keyword_retries + 1,
                    e,
                )
                time.sleep(sleep_seconds)

        if not success:
            logger.error("Semua percobaan gagal untuk keyword '%s'.", kw)

        time.sleep(sleep_seconds)

    if not dfs:
        logger.warning("Semua keyword gagal atau tidak ada data.")
        return pd.DataFrame()

    df_trends = pd.concat(dfs, axis=1)
    df_trends["extraction_at"] = extraction_at_iso
    df_trends.reset_index(inplace=True)
    if "date" not in df_trends.columns and "index" in df_trends.columns:
        df_trends = df_trends.rename(columns={"index": "date"})

    return df_trends
