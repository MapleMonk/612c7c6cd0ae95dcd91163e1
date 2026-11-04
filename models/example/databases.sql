{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.funnel_metrics as WITH marketing_source AS ( SELECT COALESCE( SAFE.PARSE_DATE(\'%Y%m%d\', CAST(date AS STRING)), SAFE_CAST(date AS DATE) ) AS report_date, impressions, clicks, add_to_carts, INITIATE_CHECKOUTS, total_orders FROM `maplemonk.pronk_MARKETING_CONSOLIDATED` ), marketing_daily AS ( SELECT report_date, SUM(COALESCE(impressions, 0)) AS impressions, SUM(COALESCE(clicks, 0)) AS clicks, SUM(COALESCE(add_to_carts, 0)) AS add_to_carts, SUM(COALESCE(INITIATE_CHECKOUTS, 0)) AS initiate_checkouts, SUM(COALESCE(total_orders, 0)) AS total_orders FROM marketing_source WHERE report_date >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND report_date < CURRENT_DATE() GROUP BY report_date ), ga4_source AS ( SELECT COALESCE( SAFE.PARSE_DATE(\'%Y%m%d\', CAST(date AS STRING)), SAFE_CAST(date AS DATE) ) AS report_date, averageSessionDuration FROM `maplemonk.GA4_Pronk_Google_Analytics_sessions_users_by_date` ), ga4_daily AS ( SELECT report_date, AVG(averageSessionDuration) AS session_duration FROM ga4_source WHERE report_date >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND report_date < CURRENT_DATE() GROUP BY report_date ), daily AS ( SELECT COALESCE(m.report_date, g.report_date) AS report_date, COALESCE(m.impressions, 0) AS impressions, COALESCE(m.clicks, 0) AS clicks, g.session_duration, COALESCE(m.add_to_carts, 0) AS add_to_carts, COALESCE(m.initiate_checkouts, 0) AS initiate_checkouts, COALESCE(m.total_orders, 0) AS total_orders FROM marketing_daily m FULL OUTER JOIN ga4_daily g ON m.report_date = g.report_date ), mtd AS ( SELECT COALESCE(SUM(impressions), 0) AS impressions, COALESCE(SUM(clicks), 0) AS clicks, AVG(session_duration) AS session_duration, COALESCE(SUM(add_to_carts), 0) AS add_to_carts, COALESCE(SUM(add_to_carts), 0) - COALESCE(SUM(initiate_checkouts), 0) AS dropouts, COALESCE(SUM(total_orders), 0) AS total_orders, FROM daily ) SELECT 1 AS stage_order, \'Impressions\' AS funnel_stage, impressions AS funnel_value FROM mtd UNION ALL SELECT 2, \'Clicks\', clicks FROM mtd UNION ALL SELECT 3, \'Add to Carts\', add_to_carts FROM mtd UNION ALL SELECT 4, \'Total Orders\', total_orders FROM mtd ORDER BY stage_order;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            