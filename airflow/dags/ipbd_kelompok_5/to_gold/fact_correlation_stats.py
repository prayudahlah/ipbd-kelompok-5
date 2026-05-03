import logging
from airflow.sdk import task
from sqlalchemy import create_engine, text

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri

logger = logging.getLogger(__name__)


@task()
def correlation_stats():
    dw_uri = get_dw_uri()
    engine = create_engine(dw_uri)

    query = """
    DROP TABLE IF EXISTS gold.fact_correlation_stats;
    CREATE TABLE gold.fact_correlation_stats AS

    WITH monthly_base AS (
        SELECT
            month,
            oil_avg_usd,
            google_interest_all,
            google_electric_vehicle,
            google_tesla,
            google_lfp_battery,
            google_solid_state_battery,
            wiki_views_total
        FROM gold.fact_ev_oil_monthly
    ),

    -- 3-month rolling correlation
    corr_3m AS (
        SELECT
            month,
            3 AS window_months,

            ROUND(CORR(oil_avg_usd, google_interest_all)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_google_all,

            ROUND(CORR(oil_avg_usd, google_electric_vehicle)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_electric_vehicle,

            ROUND(CORR(oil_avg_usd, google_tesla)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_tesla,

            ROUND(CORR(oil_avg_usd, google_lfp_battery)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_lfp_battery,

            ROUND(CORR(oil_avg_usd, google_solid_state_battery)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_solid_state_battery,

            ROUND(CORR(oil_avg_usd, wiki_views_total)
                OVER (ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_wiki
        FROM monthly_base
    ),

    -- 6-month rolling correlation
    corr_6m AS (
        SELECT
            month,
            6 AS window_months,

            ROUND(CORR(oil_avg_usd, google_interest_all)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_google_all,

            ROUND(CORR(oil_avg_usd, google_electric_vehicle)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_electric_vehicle,

            ROUND(CORR(oil_avg_usd, google_tesla)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_tesla,

            ROUND(CORR(oil_avg_usd, google_lfp_battery)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_lfp_battery,

            ROUND(CORR(oil_avg_usd, google_solid_state_battery)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_solid_state_battery,

            ROUND(CORR(oil_avg_usd, wiki_views_total)
                OVER (ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_wiki
        FROM monthly_base
    ),

    -- 12-month rolling correlation
    corr_12m AS (
        SELECT
            month,
            12 AS window_months,

            ROUND(CORR(oil_avg_usd, google_interest_all)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_google_all,

            ROUND(CORR(oil_avg_usd, google_electric_vehicle)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_electric_vehicle,

            ROUND(CORR(oil_avg_usd, google_tesla)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_tesla,

            ROUND(CORR(oil_avg_usd, google_lfp_battery)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_lfp_battery,

            ROUND(CORR(oil_avg_usd, google_solid_state_battery)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_solid_state_battery,

            ROUND(CORR(oil_avg_usd, wiki_views_total)
                OVER (ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::numeric, 4)
                AS corr_oil_vs_wiki
        FROM monthly_base
    )

    SELECT * FROM corr_3m
    UNION ALL
    SELECT * FROM corr_6m
    UNION ALL
    SELECT * FROM corr_12m

ORDER BY month, window_months;
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    logger.info("Gold fact_correlation_stats created")