import logging
import boto3
import os

logger = logging.getLogger(__name__)


def get_garage_client():
    GARAGE_ENDPOINT_URL = os.getenv("GARAGE_ENDPOINT_URL")
    GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY")
    GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY")

    if not GARAGE_ACCESS_KEY or not GARAGE_SECRET_KEY:
        raise RuntimeError("Garage credentials env needs to be set")

    garage_client = boto3.client(
        "s3",
        endpoint_url=GARAGE_ENDPOINT_URL,
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
    )

    return garage_client
