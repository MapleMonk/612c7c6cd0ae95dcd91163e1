{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.chart1_myntra AS WITH po AS ( SELECT PO_ID, DATE_TRUNC(\'month\', TO_DATE(PO_CREATED_DATE)) AS order_month, TOTAL_QUANTITY, INWARDED_QUANTITY, DATA_QUALITY_FLAG FROM snitch_db.maplemonk.s3_myntra_grn_test WHERE PO_CREATED_DATE IS NOT NULL AND TO_DATE(PO_CREATED_DATE) >= TO_DATE(\'2026-04-01\') ) SELECT order_month AS month, \'MYNTRA\' AS channel, COUNT(DISTINCT CASE WHEN DATA_QUALITY_FLAG IS NULL THEN PO_ID END) AS po_count, SUM(CASE WHEN DATA_QUALITY_FLAG IS NULL THEN TOTAL_QUANTITY ELSE 0 END) AS po_raised, NULL AS po_fulfilled, SUM(CASE WHEN DATA_QUALITY_FLAG IS NULL THEN INWARDED_QUANTITY ELSE 0 END) AS po_grned, NULL AS fulfilled_pct, CASE WHEN SUM(CASE WHEN DATA_QUALITY_FLAG IS NULL THEN TOTAL_QUANTITY ELSE 0 END) > 0 THEN ROUND(SUM(CASE WHEN DATA_QUALITY_FLAG IS NULL THEN INWARDED_QUANTITY ELSE 0 END) / NULLIF(SUM(CASE WHEN DATA_QUALITY_FLAG IS NULL THEN TOTAL_QUANTITY ELSE 0 END), 0) * 100, 2) ELSE NULL END AS grned_pct, COUNT(DISTINCT CASE WHEN DATA_QUALITY_FLAG IS NOT NULL THEN PO_ID END) AS po_count_excluded FROM po GROUP BY order_month ORDER BY month;",
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
            