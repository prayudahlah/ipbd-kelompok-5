import logging
from airflow.sdk import task
from sqlalchemy import create_engine, text

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri

logger = logging.getLogger(__name__)


@task()
def create_schema():
    dw_uri = get_dw_uri()
    engine = create_engine(dw_uri)

    query = """
    CREATE SCHEMA IF NOT EXISTS gold;
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    logger.info("Gold trend_monthly created")