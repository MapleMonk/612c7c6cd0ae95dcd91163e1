{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_ShopperStop_Target_vs_Achievement` AS WITH Store_Mapping AS ( SELECT LOWER(marketplace) AS marketplace, Store_Code, TRIM(UPPER(Store_Name)) AS Store_Name, TRIM(UPPER(Region)) AS Region, TRIM(UPPER(State)) AS State, TRIM(UPPER(City)) AS City FROM `MapleMonk.Zouk_Store_Mapping_Sheet` ) , Targets AS ( SELECT t.Store_Code, sm.Store_Name, CASE WHEN UPPER(t.Store_Code) LIKE ANY (\'THE_SCM_SILK\', \'THE_SCM_SILK_TIRUPUR\', \'THE_SCM_SILK_TUTICORIN\') THEN \'THE_CHENNAI_SILK\' WHEN UPPER(t.Store_Code) LIKE (\'STYLE_PLUS_TVM\') THEN \'STYLE_PLUS_B2B\' ELSE \'SHOPPERSSTOP\' END AS Marketplace, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, CAST(REPLACE(t.Targets,\',\',\'\') AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM `MapleMonk.Zouk_Store_Targets` t JOIN Store_Mapping sm ON t.Store_Code = sm.Store_Code CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ) , Achievement_CTE AS ( SELECT Order_Date, a.MARKETPLACE, CHANNEL, sm.Store_Code, SUM(CAST(SELLING_PRICE AS FLOAT64)) AS Achievement, SUM(QUANTITY) AS QUANTITY FROM `MapleMonk.zouk_Secondary_sales_consolidated` a LEFT JOIN Store_Mapping sm ON TRIM(LOWER(a.Channel)) = TRIM(LOWER(sm.Store_Name)) WHERE LOWER(a.marketplace) LIKE ANY (\'shoppersstop\',\'%crossword%\',\'%emami%\',\'%brands%\',\'%chennai%\',\'%style%\') GROUP BY 1,2,3,4 ) ,Combined AS ( SELECT COALESCE(t.target_date,s.Order_Date) AS Date, COALESCE(t.Store_Code,s.Store_Code) AS Store_Code, COALESCE(t.Store_name,s.Channel) AS Store_Name, COALESCE(s.Marketplace,t.Marketplace) AS Marketplace, t.Target, t.no_of_days, t.days_Remaining, CASE WHEN EXTRACT(DAYOFWEEK FROM COALESCE(t.Target_Date, s.Order_Date)) IN (1, 7) THEN \'Weekend\' ELSE \'Weekday\' END AS Day_Type, m.City, m.Region, m.State, COALESCE(Achievement,0) AS Achievement, COALESCE(Quantity,0) AS Quantity FROM Targets t FULL OUTER JOIN Achievement_CTE s ON t.Target_Date = s.Order_date AND lower(trim(t.Store_code)) =lower(trim(s.Store_Code)) LEFT JOIN Store_Mapping m ON lower(coalesce(t.Store_Code,s.Store_Code)) = lower(m.Store_Code) ) select * from combined",
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
            