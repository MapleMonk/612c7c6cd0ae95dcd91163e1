{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_store_target_vs_achievement AS WITH Daily_Targets AS ( SELECT UPPER(TRIM(Store_Name)) AS Store_Name, -- Convert Store_Name to uppercase CAST(Date AS DATE) AS Date, -- Ensure Date is cast to DATE CAST(REPLACE(Target, \',\', \'\') AS FLOAT64) AS Target -- Remove commas and cast Target to FLOAT64 FROM maplemonk.Store_Level_Daywise_Target ), Daily_Achievements AS ( SELECT UPPER(TRIM(Source)) AS Store_Name, -- Convert Source to uppercase CAST(Order_Date AS DATE) AS Date, -- Ensure Order_Date is cast to DATE SUM(Selling_Price) AS Total_Achievement -- Aggregate Selling_Price FROM maplemonk.zouk_sales_consolidated WHERE LOWER(order_status) NOT LIKE \'cancelled\' -- Exclude cancelled orders GROUP BY UPPER(TRIM(Source)), CAST(Order_Date AS DATE) ) SELECT Daily_Targets.Store_Name, Daily_Targets.Date, Daily_Targets.Target, IFNULL(Daily_Achievements.Total_Achievement, 0) AS Total_Achievement FROM Daily_Targets LEFT JOIN Daily_Achievements ON Daily_Targets.Store_Name = Daily_Achievements.Store_Name AND Daily_Targets.Date = Daily_Achievements.Date ORDER BY Daily_Targets.Store_Name, Daily_Targets.Date;",
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
            