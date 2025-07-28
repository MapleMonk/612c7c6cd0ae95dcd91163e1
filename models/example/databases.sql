{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_store_target_vs_achievement AS WITH Daily_Targets AS ( SELECT UPPER(TRIM(Store_Name)) AS Store_Name, PARSE_DATE(\'%Y-%m-%d\', Date) AS Target_Date, CAST(REPLACE(Target,\',\',\'\') AS FLOAT64) AS Target, CASE WHEN CURRENT_DATE() > DATE(Date) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE(Date)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE(Date))) AS no_of_days FROM `MapleMonk.Store_Level_Daywise_Target` ), Daily_Achievements AS ( SELECT UPPER(TRIM(SOURCE)) AS Store_Name, CAST(Order_Date AS DATE) AS Date, SUM(Selling_Price) AS Total_Achievement FROM MapleMonk.zouk_sales_consolidated WHERE NOT (LOWER(IFNULL(ORDER_STATUS,\'\')) LIKE \'%cancel%\' OR LOWER(IFNULL(FINAL_SHIPPING_STATUS,\'\')) LIKE \'%cancel%\') AND DATE(Order_Date) < CURRENT_DATE() AND lower(marketplace) like \'shopify pos\' GROUP BY 1,2 ) SELECT COALESCE(t.Target_Date, a.Date) AS Date, COALESCE(t.Store_Name, a.Store_Name) AS Store_Name, t.Target, t.days_Remaining, t.no_of_days, IFNULL(a.Total_Achievement,0) AS Achievement FROM Daily_Achievements a FULL OUTER JOIN Daily_Targets t ON a.Store_Name = t.Store_Name AND a.Date = t.Target_Date ORDER BY 2,1 ;",
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
            