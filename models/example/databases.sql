{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.zouk_store_target_vs_achievement AS WITH Expanded_Calendar AS ( SELECT t.Store, m.Store_Code, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, CAST(REPLACE(t.Target,\",\",\"\") AS FLOAT64) AS Monthly_Target, CASE WHEN EXTRACT(DAYOFWEEK FROM DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)) IN (1,7) THEN \'Weekend\' ELSE \'Weekday\' END AS Day_Type, DATE(t.Start_Date) AS Start_Date, DATE(t.End_Date) AS End_Date FROM maplemonk.zouk_ebo_store_targets t left join `MapleMonk.Zouk_EBO_Store_Master` m on lower(t.Store) = lower(m.Store_Name) CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Day_Distribution AS ( SELECT Store, Start_Date, End_Date, COUNTIF(Day_Type = \'Weekend\') AS weekend_days, COUNTIF(Day_Type = \'Weekday\') AS weekday_days, ANY_VALUE(Monthly_Target) AS Monthly_Target FROM Expanded_Calendar GROUP BY Store, Start_Date, End_Date ), Monthly_Targets AS ( SELECT TRIM(UPPER(e.Store)) AS Store, TRIM(UPPER(e.Store_Code)) AS Store_Code, e.Target_Date, e.Day_Type, CASE WHEN e.Day_Type = \'Weekend\' THEN (0.5 * d.Monthly_Target) / NULLIF(d.weekend_days,0) ELSE (0.5 * d.Monthly_Target) / NULLIF(d.weekday_days,0) END AS Daily_Target, CASE WHEN CURRENT_DATE() > e.Target_Date THEN 0 ELSE DATE_DIFF(LAST_DAY(e.Target_Date), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(e.Target_Date)) AS no_of_days FROM Expanded_Calendar e JOIN Day_Distribution d ON e.Store = d.Store AND e.Start_Date = d.Start_Date AND e.End_Date = d.End_Date ), Daily_Achievements AS ( SELECT UPPER(TRIM(SOURCE)) AS Store_Name, UPPER(TRIM(Store_Code)) AS Store_Code, CAST(Order_Date AS DATE) AS Date, SUM(Selling_Price) AS Total_Achievement FROM MapleMonk.zouk_sales_consolidated WHERE NOT (LOWER(IFNULL(ORDER_STATUS,\'\')) LIKE \'%cancel%\' OR LOWER(IFNULL(FINAL_SHIPPING_STATUS,\'\')) LIKE \'%cancel%\') AND DATE(Order_Date) < CURRENT_DATE() AND lower(marketplace) like \'shopify pos\' GROUP BY 1,2,3 ) SELECT COALESCE(t.Target_Date, a.Date) AS Date, COALESCE(t.Store, a.Store_Name) AS Store_Name, COALESCE(t.Store_Code,a.Store_Code) AS Store_Code, t.Daily_Target AS Target, t.Day_Type, t.days_Remaining, t.no_of_days, IFNULL(a.Total_Achievement,0) AS Achievement FROM Daily_Achievements a FULL OUTER JOIN Monthly_Targets t ON a.Store_Name = TRIM(UPPER(t.Store)) AND a.Date = t.Target_Date ORDER BY 2,1",
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
            