import logging
import requests
from airflow.sdk import task

from ipbd_kelompok_5.utils.dict_to_parquet_garage import dict_to_parquet_garage

logger = logging.getLogger(__name__)


@task()
def fetch_and_load_ev_wiki_trend(**context):
    extraction_at = context["data_interval_start"]
    extraction_at_iso = extraction_at.isoformat()
    logger.info(
        "Menarik data Wikipedia Trends untuk Electric Vehicle pada %s",
        extraction_at,
    )

    start_date = "20150101"
    end_date = extraction_at.strftime("%Y%m%d")
    page = "Electric_vehicle"

    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/all-agents/{page}/monthly/{start_date}/{end_date}"
    headers = {
        "accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    }

    response = None
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError as e:
            logger.error(
                "Gagal parse JSON dari Wikimedia (status=%s): %s",
                response.status_code,
                e,
            )
            return

        items = data.get("items", [])
        if not items:
            logger.warning("Tidak ada data untuk diunggah.")
            return

        for item in items:
            if "timestamp" in item and "date" not in item:
                item["date"] = item.pop("timestamp")
            item["extraction_at"] = extraction_at_iso

        logger.info(
            "Data Wikipedia Trends berhasil ditarik dengan %s entri!",
            len(items),
        )
        dict_to_parquet_garage(
            data=items,
            object_key=f"bronze/ev_wiki_trend/{extraction_at.strftime('%Y-%m-%d')}.parquet",
        )
    except requests.exceptions.RequestException as e:
        logger.error(
            "Request gagal (status=%s): %s",
            response.status_code if response else "unknown",
            e,
        )
