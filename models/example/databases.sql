{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zouk_Secondary_Target_vs_Achievement AS WITH Targets AS ( SELECT TRIM(UPPER(t.Marketplace)) AS Marketplace, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, CAST(t.Target AS INT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM `MapleMonk.Zouk_Marketplace_Targets` t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Sales AS ( SELECT TRIM(UPPER(s.Marketplace)) AS Marketplace, DATE(s.Order_Date) AS Achievement_Date, SUM(IFNULL(s.Selling_Price, 0)) AS Achievement FROM `MapleMonk.zouk_Secondary_sales_consolidated`s WHERE NOT (LOWER(IFNULL(ORDER_STATUS,\'\')) LIKE \'%cancel%\' OR LOWER(IFNULL(FINAL_SHIPPING_STATUS,\'\')) LIKE \'%cancel%\') AND DATE(s.Order_Date) < CURRENT_DATE() GROUP BY 1, 2 ), Combined_Data AS ( SELECT COALESCE(t.Target_Date, s.Achievement_Date) AS Date, COALESCE(t.Marketplace, s.Marketplace) AS Marketplace, t.Target, t.days_Remaining, t.no_of_days, COALESCE(s.Achievement,0) AS Achievement FROM Targets t FULL OUTER JOIN Sales s ON TRIM(LOWER(t.Marketplace)) = TRIM(LOWER(s.Marketplace)) AND t.Target_Date = s.Achievement_Date ), Final_Data AS ( SELECT cd.Date, cd.Marketplace, cd.Target, cd.days_Remaining, cd.no_of_days, cd.Achievement, mm.Final_Marketplace, mm.Marketplace_Segments, mm.Channel, CASE WHEN mm.Marketplace IS NULL THEN \'Missing in Marketplace Mapping\' ELSE \'Found\' END AS Mapping_Status FROM Combined_Data cd LEFT JOIN ( SELECT * FROM maplemonk.Zouk_New_Marketplace_Mapping QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(marketplace)) ORDER BY 1) = 1 ) mm ON LOWER(TRIM(cd.Marketplace)) = LOWER(TRIM(mm.Marketplace)) ) SELECT Date, Marketplace, Marketplace_Segments, Final_Marketplace, Channel, Target, Achievement, days_Remaining, no_of_days FROM Final_Data ORDER BY Date, Marketplace ;",
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
            