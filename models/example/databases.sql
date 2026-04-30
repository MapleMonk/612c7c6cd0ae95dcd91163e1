{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MAPLEMONK.SAADAA_order_funnel` AS WITH base_data AS ( SELECT OMS_order_status, FORMAT_DATE(\'%Y\', Order_Date) AS Year, FORMAT_DATE(\'%b\', Order_Date) AS Month_Name, FORMAT_DATE(\'%m\', Order_Date) AS Month_Num, DATE_TRUNC(Order_Date, MONTH) AS Month_Date, reference_code, QUANTITY FROM `MAPLEMONK.SAADAA_sales_consolidated` ) SELECT OMS_order_status, Year, Month_Name, Month_Num, Month_Date, \'Order Qty\' AS Metric_Type, COUNT(DISTINCT reference_code) AS Value FROM base_data GROUP BY 1, 2, 3, 4, 5, 6 UNION ALL SELECT OMS_order_status, Year, Month_Name, Month_Num, Month_Date, \'Item Qty\' AS Metric_Type, SUM(QUANTITY) AS Value FROM base_data GROUP BY 1, 2, 3, 4, 5, 6;",
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
            