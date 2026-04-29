import io
import logging
import pandas as pd
from ipbd_kelompok_5.utils.get_bucket_name import get_bucket_name
from ipbd_kelompok_5.utils.get_garage_client import get_garage_client

logger = logging.getLogger(__name__)


def pd_to_parquet_garage(df: pd.DataFrame, with_index: bool, object_key: str):
    garage_client = get_garage_client()
    bucket = get_bucket_name()

    logger.info("Mengkonversi DataFrame ke Parquet di Memory...")

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=with_index)

    logger.info(f"Mengunggah ke s3://{bucket}/{object_key}...")
    garage_client.put_object(
        Bucket=bucket, Key=object_key, Body=parquet_buffer.getvalue()
    )
    logger.info("Upload sukses!")
