import os


def get_garage_storage_options():
    GARAGE_ENDPOINT_URL = os.getenv("GARAGE_ENDPOINT_URL")
    GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY")
    GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY")

    storage_options = {
        "aws_endpoint_url": GARAGE_ENDPOINT_URL,
        "aws_access_key_id": GARAGE_ACCESS_KEY,
        "aws_secret_access_key": GARAGE_SECRET_KEY,
        "aws_region": "garage",
    }

    return storage_options
