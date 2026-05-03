
import logging
from airflow.sdk import task
from sqlalchemy import create_engine, text

from ipbd_kelompok_5.utils.get_dw_uri import get_dw_uri

logger = logging.getLogger(__name__)


@task()
def impact_event():
    dw_uri = get_dw_uri()
    engine = create_engine(dw_uri)

    query = """
    DROP TABLE IF EXISTS gold.fact_event_impact;
    CREATE TABLE gold.fact_event_impact AS

    WITH monthly_base AS (
        SELECT
            month,
            year,
            quarter,
            month_name,
            oil_avg_usd,
            google_interest_all,
            google_electric_vehicle,
            google_tesla,
            google_lfp_battery,
            google_solid_state_battery,
            wiki_views_total
        FROM gold.fact_ev_oil_monthly
    ),

    -- hitung perubahan month-over-month (MoM) untuk setiap metrik
    mom_changes AS (
        SELECT
            month,
            year,
            quarter,
            month_name,

            -- nilai aktual
            oil_avg_usd,
            google_interest_all,
            google_electric_vehicle,
            google_tesla,
            google_lfp_battery,
            google_solid_state_battery,
            wiki_views_total,

            -- nilai bulan sebelumnya
            LAG(oil_avg_usd)                  OVER (ORDER BY month) AS oil_prev,
            LAG(google_interest_all)          OVER (ORDER BY month) AS google_all_prev,
            LAG(google_electric_vehicle)      OVER (ORDER BY month) AS google_ev_prev,
            LAG(google_tesla)                 OVER (ORDER BY month) AS google_tesla_prev,
            LAG(google_lfp_battery)           OVER (ORDER BY month) AS google_lfp_prev,
            LAG(google_solid_state_battery)   OVER (ORDER BY month) AS google_ssbattery_prev,
            LAG(wiki_views_total)             OVER (ORDER BY month) AS wiki_prev,

            -- pct change MoM
            ROUND(
                (oil_avg_usd - LAG(oil_avg_usd) OVER (ORDER BY month))
                / NULLIF(LAG(oil_avg_usd) OVER (ORDER BY month), 0) * 100
            , 2) AS oil_pct_change,

            ROUND(
                (google_interest_all - LAG(google_interest_all) OVER (ORDER BY month))
                / NULLIF(LAG(google_interest_all) OVER (ORDER BY month), 0) * 100
            , 2) AS google_all_pct_change,

            ROUND(
                (google_electric_vehicle - LAG(google_electric_vehicle) OVER (ORDER BY month))
                / NULLIF(LAG(google_electric_vehicle) OVER (ORDER BY month), 0) * 100
            , 2) AS google_ev_pct_change,

            ROUND(
                (google_tesla - LAG(google_tesla) OVER (ORDER BY month))
                / NULLIF(LAG(google_tesla) OVER (ORDER BY month), 0) * 100
            , 2) AS google_tesla_pct_change,

            ROUND(
                (google_lfp_battery - LAG(google_lfp_battery) OVER (ORDER BY month))
                / NULLIF(LAG(google_lfp_battery) OVER (ORDER BY month), 0) * 100
            , 2) AS google_lfp_pct_change,

            ROUND(
                (google_solid_state_battery - LAG(google_solid_state_battery) OVER (ORDER BY month))
                / NULLIF(LAG(google_solid_state_battery) OVER (ORDER BY month), 0) * 100
            , 2) AS google_ssbattery_pct_change,

            ROUND(
                (wiki_views_total - LAG(wiki_views_total) OVER (ORDER BY month))
                / NULLIF(LAG(wiki_views_total) OVER (ORDER BY month), 0) * 100
            , 2) AS wiki_pct_change

        FROM monthly_base
    ),

    -- unpivot manual: setiap metrik jadi baris sendiri
    unpivoted AS (

        SELECT month, year, quarter, month_name,
            'oil_price'          AS metric,
            oil_avg_usd          AS current_value,
            oil_prev             AS prev_value,
            oil_pct_change       AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'google_all'         AS metric,
            google_interest_all  AS current_value,
            google_all_prev      AS prev_value,
            google_all_pct_change AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'google_electric_vehicle'  AS metric,
            google_electric_vehicle    AS current_value,
            google_ev_prev             AS prev_value,
            google_ev_pct_change       AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'google_tesla'       AS metric,
            google_tesla         AS current_value,
            google_tesla_prev    AS prev_value,
            google_tesla_pct_change AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'google_lfp_battery' AS metric,
            google_lfp_battery   AS current_value,
            google_lfp_prev      AS prev_value,
            google_lfp_pct_change AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'google_solid_state_battery'  AS metric,
            google_solid_state_battery    AS current_value,
            google_ssbattery_prev         AS prev_value,
            google_ssbattery_pct_change   AS pct_change
        FROM mom_changes

        UNION ALL

        SELECT month, year, quarter, month_name,
            'wiki_views'         AS metric,
            wiki_views_total     AS current_value,
            wiki_prev            AS prev_value,
            wiki_pct_change      AS pct_change
        FROM mom_changes
    )

    -- filter hanya baris yang merupakan event signifikan (threshold ±15%)
    SELECT
        month,
        year,
        quarter,
        month_name,
        metric,
        ROUND(current_value::numeric, 2)  AS current_value,
        ROUND(prev_value::numeric, 2)     AS prev_value,
        ROUND(pct_change::numeric, 2)     AS pct_change,

        -- klasifikasi arah perubahan
        CASE
            WHEN pct_change >= 15  THEN 'spike_up'
            WHEN pct_change <= -15 THEN 'spike_down'
        END AS event_type,

        -- label siap pakai untuk anotasi chart di Metabase
        CONCAT(
            UPPER(LEFT(metric, 1)), LOWER(SUBSTRING(metric, 2)),
            ' ',
            CASE WHEN pct_change > 0 THEN '▲' ELSE '▼' END,
            ' ',
            ABS(ROUND(pct_change::numeric, 1)),
            '% vs bulan lalu'
        ) AS event_label

    FROM unpivoted
    WHERE ABS(pct_change) >= 15
    AND pct_change IS NOT NULL

    ORDER BY month, metric;
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    logger.info("Gold fact_event_impact created")