import polars as pl
from typing import List, Dict, Any

from ipbd_kelompok_5.utils.get_bucket_name import get_bucket_name
from ipbd_kelompok_5.utils.get_garage_storage_options import (
    get_garage_storage_options,
)


def dict_to_parquet_garage(
    data: List[Dict[str, Any]],
    object_key: str,
    compression: str = "zstd",
) -> None:
    bucket = get_bucket_name()
    garage_storage_options = get_garage_storage_options()

    if not data:
        raise ValueError("Data kosong, tidak ada yang diunggah.")

    lf = pl.LazyFrame(data)
    garage_path = f"s3://{bucket}/{object_key}"

    lf.sink_parquet(
        garage_path,
        compression=compression,
        row_group_size=100_000,
        storage_options=garage_storage_options,
    )
