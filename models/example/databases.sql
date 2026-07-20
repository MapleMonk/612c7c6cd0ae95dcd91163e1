{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_sales_mix_reference AS WITH alloc_weeks AS ( SELECT branch_code, DATE_TRUNC(\'WEEK\', order_date) AS week_start, MAX(order_date) AS reference_date FROM snitch_db.maplemonk.fresh_actual_allocation GROUP BY 1,2 ), sku_sales AS ( SELECT aw.branch_code, aw.week_start, aw.reference_date, ms.category, COALESCE(UPPER(ms.style), \'SNITCH\') AS style, COALESCE(UPPER(ms.meta1), \'N/A\') AS meta1, COALESCE(UPPER(ms.meta2), \'N/A\') AS meta2, COALESCE(UPPER(ms.meta3), \'N/A\') AS meta3, SUM(r.SALES_LAST_7_DAYS) AS sales_7d, SUM(r.SALES_LAST_15_DAYS) AS sales_15d, SUM(r.SALES_LAST_30_DAYS) AS sales_30d FROM alloc_weeks aw INNER JOIN snitch_db.maplemonk.offline_master_Daily_Report_1 r ON r.BRANCH_CODE::VARCHAR = aw.branch_code AND r.DATE = aw.reference_date LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(r.SKU_GROUP) = UPPER(ms.sku_group) WHERE ms.category IS NOT NULL GROUP BY 1,2,3,4,5,6,7,8 ), mix_pct AS ( SELECT *, sales_7d / NULLIF(SUM(sales_7d) OVER (PARTITION BY branch_code, week_start), 0) AS mix_7d_pct, sales_15d / NULLIF(SUM(sales_15d) OVER (PARTITION BY branch_code, week_start), 0) AS mix_15d_pct, sales_30d / NULLIF(SUM(sales_30d) OVER (PARTITION BY branch_code, week_start), 0) AS mix_30d_pct FROM sku_sales ) SELECT branch_code, week_start, reference_date, category, style, meta1, meta2, meta3, sales_7d, sales_15d, sales_30d, mix_7d_pct, mix_15d_pct, mix_30d_pct, (COALESCE(mix_7d_pct, 0) * 0.5 + COALESCE(mix_15d_pct, 0) * 0.3 + COALESCE(mix_30d_pct, 0) * 0.2) AS sales_mix_blended_pct FROM mix_pct;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            