
import logging
from airflow.sdk import task
from sqlalchemy import create_engine, text

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri

logger = logging.getLogger(__name__)


@task()
def ev_oil_monthly():
    dw_uri = get_dw_uri()
    engine = create_engine(dw_uri)

    query = """
    DROP TABLE IF EXISTS gold.fact_ev_oil_monthly;
    CREATE TABLE gold.fact_ev_oil_monthly AS

    WITH monthly_oil AS (
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            ROUND(AVG(close_price)::numeric, 2) AS oil_avg_usd,
            ROUND(MIN(close_price)::numeric, 2) AS oil_min_usd,
            ROUND(MAX(close_price)::numeric, 2) AS oil_max_usd
        FROM silver.oil_prices
        WHERE ticker = 'BZ=F'
        GROUP BY DATE_TRUNC('month', date)
    ),

    monthly_google AS (
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            ROUND(AVG(interest)::numeric, 2)                                               AS google_interest_all,
            ROUND(AVG(CASE WHEN keyword = 'Electric Vehicle' THEN interest END)::numeric, 2) AS google_electric_vehicle,
            ROUND(AVG(CASE WHEN keyword = 'Tesla'            THEN interest END)::numeric, 2) AS google_tesla,
            ROUND(AVG(CASE WHEN keyword = 'LFP Battery'      THEN interest END)::numeric, 2) AS google_lfp_battery,
            ROUND(AVG(CASE WHEN keyword = 'Solid State Battery' THEN interest END)::numeric, 2) AS google_solid_state_battery
        FROM silver.ev_google_trends
        GROUP BY DATE_TRUNC('month', date)
    ),

    monthly_wiki AS (
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            SUM(views)                      AS wiki_views_total,
            ROUND(AVG(views)::numeric, 2)   AS wiki_views_avg_daily
        FROM silver.ev_wiki_trends
        GROUP BY DATE_TRUNC('month', date)
    )

    SELECT
        d.date                          AS month,
        d.year,
        d.quarter,
        d.month_name,

        -- oil metrics
        o.oil_avg_usd,
        o.oil_min_usd,
        o.oil_max_usd,

        -- google trends
        g.google_interest_all,
        g.google_electric_vehicle,
        g.google_tesla,
        g.google_lfp_battery,
        g.google_solid_state_battery,

        -- wiki trends
        w.wiki_views_total,
        w.wiki_views_avg_daily,

        -- normalisasi 0-100 (untuk dual-axis chart di Metabase)
        ROUND(
            (o.oil_avg_usd - MIN(o.oil_avg_usd) OVER ()) /
            NULLIF(MAX(o.oil_avg_usd) OVER () - MIN(o.oil_avg_usd) OVER (), 0) * 100
        , 2) AS oil_normalized,

        ROUND(
            (w.wiki_views_total - MIN(w.wiki_views_total) OVER ()) /
            NULLIF(MAX(w.wiki_views_total) OVER () - MIN(w.wiki_views_total) OVER (), 0) * 100
        , 2) AS wiki_normalized

    FROM silver.dim_date d
    INNER JOIN monthly_oil    o ON o.month = d.date
    INNER JOIN monthly_google g ON g.month = d.date
    INNER JOIN monthly_wiki   w ON w.month = d.date

    ORDER BY d.date;
        """

    with engine.begin() as conn:
        conn.execute(text(query))

    logger.info("Gold trend_monthly created")