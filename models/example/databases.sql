{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.chart1_flipkart AS WITH f_po AS ( SELECT PO_NUMBER, DATE_TRUNC(\'MONTH\', TO_DATE(ORDER_DATE)) AS order_month, COALESCE(GRN_ORDERED_UNITS, 0) AS ordered_units, COALESCE(GRN_PENDING_UNITS, 0) AS pending_units, COALESCE(GRN_RECEIVED_UNITS, 0) AS received_units, COALESCE(GRN_CANCELLED_UNITS, 0) AS cancelled_units FROM snitch_db.maplemonk.s3_flipkart_grn WHERE PO_NUMBER IS NOT NULL AND TRIM(PO_NUMBER) <> \'\' AND TO_DATE(ORDER_DATE) >= TO_DATE(\'2026-04-01\') QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(PO_NUMBER)) ORDER BY ORDER_DATE DESC NULLS LAST) = 1 ) SELECT order_month AS month, \'FLIPKART_SOR\' AS channel, COUNT(DISTINCT PO_NUMBER) AS po_count, SUM(ordered_units) AS po_raised, SUM(GREATEST(ordered_units - pending_units - cancelled_units, 0)) AS po_fulfilled, SUM(received_units) AS po_grned, CASE WHEN SUM(ordered_units) > 0 THEN ROUND(SUM(GREATEST(ordered_units - pending_units - cancelled_units, 0)) / NULLIF(SUM(ordered_units), 0) * 100, 2) ELSE NULL END AS fulfilled_pct, CASE WHEN SUM(GREATEST(ordered_units - pending_units - cancelled_units, 0)) > 0 THEN ROUND(SUM(received_units) / NULLIF(SUM(GREATEST(ordered_units - pending_units - cancelled_units, 0)), 0) * 100, 2) ELSE NULL END AS grned_pct, 0 AS po_count_excluded FROM f_po GROUP BY order_month ORDER BY month DESC;",
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
            