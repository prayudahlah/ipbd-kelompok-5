import os


def get_bucket_name() -> str:
    bucket = os.getenv("BUCKET_NAME", "ipbd-kelompok-5")

    return bucket
