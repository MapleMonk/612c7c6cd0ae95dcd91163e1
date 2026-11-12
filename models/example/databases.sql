{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.meta_dod_region_wise_spends_sales AS WITH meta_base AS ( SELECT date_start AS date, CASE WHEN LOWER(TRIM(region)) = \'punjab region\' THEN \'Punjab\' ELSE TRIM(region) END AS state, SUM(spend) AS meta_spend, SUM(inline_link_clicks) AS clicks, SUM(reach) AS reach, SUM(impressions) AS impressions FROM snitch_db.maplemonk.content_dashboard_customregion_spends_data GROUP BY 1, 2 ), offline_base AS ( SELECT date, TRIM(state) AS state, SUM(sales) AS offline_revenue FROM snitch_db.maplemonk.dsr_offline_store_day WHERE state IS NOT NULL GROUP BY 1, 2 ), meta_min_date AS ( SELECT MIN(date_start) AS min_dt FROM snitch_db.maplemonk.content_dashboard_customregion_spends_data ), shopify_base AS ( SELECT DATE(order_timestamp) AS date, TRIM(state) AS state, SUM(gross_sales) AS online_revenue FROM snitch_db.maplemonk.fact_items_snitch WHERE DATE(order_timestamp) >= (SELECT min_dt FROM meta_min_date) AND DATE(order_timestamp) <= DATEADD(DAY, -1, CURRENT_DATE()) GROUP BY 1, 2 ) SELECT m.date, m.state, m.meta_spend, m.clicks, m.reach, m.impressions, COALESCE(o.offline_revenue, 0) AS offline_revenue, COALESCE(s.online_revenue, 0) AS online_revenue, COALESCE(o.offline_revenue, 0) + COALESCE(s.online_revenue, 0) AS total_revenue FROM meta_base m LEFT JOIN offline_base o ON m.date = o.date AND UPPER(TRIM(m.state)) = UPPER(TRIM(o.state)) LEFT JOIN shopify_base s ON m.date = s.date AND UPPER(TRIM(m.state)) = UPPER(TRIM(s.state)) ORDER BY m.date DESC, m.meta_spend DESC ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            