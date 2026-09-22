{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.daily_overview_dashboard AS SELECT DATE, CAPSULE, SUM(GROSS_SALES) AS GROSS_SALES, SUM(TOTAL_QTY) AS TOTAL_QTY, SUM(TOTAL_DISCOUNT) AS TOTAL_DISCOUNT, ROUND( SUM(GROSS_SALES) / NULLIF(SUM(TOTAL_QTY),0), 2 ) AS ASP_GROSS, COUNT(DISTINCT CHANNEL) AS ACTIVE_CHANNELS FROM snitch_db.maplemonk.daily_sales_summary_dashboard GROUP BY DATE, CAPSULE ORDER BY DATE, CAPSULE;",
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
            