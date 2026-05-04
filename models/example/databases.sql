{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MAPLEMONK.SAADAA_order_funnel` AS WITH base_data AS ( SELECT DATE(Order_Date) AS order_day, FORMAT_DATE(\'%Y\', Order_Date) AS Year, FORMAT_DATE(\'%b\', Order_Date) AS Month_Name, FORMAT_DATE(\'%m\', Order_Date) AS Month_Num, DATE_TRUNC(Order_Date, MONTH) AS Month_Date, CASE WHEN OMS_order_status = \'CANCELLED\' OR OMS_order_status IS NULL THEN \'Cancelled\' WHEN dispatch_date IS NOT NULL AND DELIVERED_DATE IS NULL AND LOWER(OMS_order_status) = \'returned\' THEN \'RTO\' WHEN dispatch_date IS NOT NULL AND DELIVERED_DATE IS NOT NULL AND LOWER(OMS_order_status) = \'returned\' THEN \'Returned\' WHEN dispatch_date IS NULL THEN \'Pending to be Dispatched\' WHEN dispatch_date IS NOT NULL AND DELIVERED_DATE IS NULL THEN \'Dispatched\' WHEN dispatch_date IS NOT NULL AND DELIVERED_DATE IS NOT NULL THEN \'Delivered\' ELSE \'Others\' END AS order_stage, reference_code, QUANTITY FROM `MAPLEMONK.SAADAA_sales_consolidated` WHERE (Order_Type NOT IN (\"Stock Transfer Order\", \"Stock Transfer\") OR Order_Type IS NULL) AND Order_Date > DATE \"2026-04-01\" ), stage_summary AS ( SELECT order_day, Year, Month_Name, Month_Num, Month_Date, order_stage, COUNT(DISTINCT reference_code) AS order_qty, SUM(QUANTITY) AS item_qty, CASE order_stage WHEN \'Pending to be Dispatched\' THEN 1 WHEN \'Dispatched\' THEN 2 WHEN \'Delivered\' THEN 3 WHEN \'RTO\' THEN 4 WHEN \'Returned\' THEN 5 WHEN \'Cancelled\' THEN 6 ELSE 7 END AS stage_seq FROM base_data GROUP BY 1, 2, 3, 4, 5, 6, 9 ), final_metrics AS ( SELECT *, ROUND(order_qty * 100.0 / SUM(order_qty) OVER (PARTITION BY order_day), 2) AS pct_of_day, ROUND(order_qty * 100.0 / NULLIF(LAG(order_qty) OVER (PARTITION BY order_day ORDER BY stage_seq), 0), 2) AS pct_from_previous FROM stage_summary ) SELECT order_day, Year, Month_Name, Month_Num, Month_Date, order_stage, stage_seq, \'Order Qty\' AS Metric_Type, order_qty AS Value, pct_of_day, pct_from_previous FROM final_metrics UNION ALL SELECT order_day, Year, Month_Name, Month_Num, Month_Date, order_stage, stage_seq, \'Item Qty\' AS Metric_Type, item_qty AS Value, pct_of_day, pct_from_previous FROM final_metrics;",
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
            