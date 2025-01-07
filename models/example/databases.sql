{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Collection_Target_vs_Achievement AS WITH Collection_Level_Target AS ( SELECT UPPER(t.Marketplace) AS Marketplace, UPPER(t.Collection) AS Collection, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Target_Date, CAST(t.Target AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Target FROM maplemonk.Zouk_Collection_Level_Target t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), Daily_Achievements AS ( SELECT UPPER(s.Marketplace) AS Marketplace, UPPER(s.Collection) AS Collection, DATE(s.Order_Date) AS Achievement_Date, SUM(IFNULL(s.Selling_Price, 0)) AS Achievement FROM maplemonk.zouk_secondary_sales_Consolidated s WHERE s.Selling_Price IS NOT NULL AND UPPER(s.Order_Status) != \'CANCELLED\' GROUP BY UPPER(s.Marketplace), UPPER(s.Collection), DATE(s.Order_Date) ), Combined_Data AS ( SELECT dt.Target_Date AS Date, dt.Marketplace, dt.Collection, dt.Target, COALESCE(da.Achievement, 0) AS Achievement FROM Collection_Level_Target dt LEFT JOIN Daily_Achievements da ON dt.Marketplace = da.Marketplace AND dt.Collection = da.Collection AND dt.Target_Date = da.Achievement_Date ), Final_Data AS ( SELECT cd.Date, cd.Marketplace, cd.Collection, cd.Target, cd.Achievement, mm.Final_Marketplace, mm.Marketplace_Segments, mm.Channel, CASE WHEN mm.Marketplace IS NULL THEN \'Missing in Marketplace Mapping\' ELSE \'Found\' END AS Mapping_Status FROM Combined_Data cd LEFT JOIN maplemonk.Zouk_New_Marketplace_Mapping mm ON lower(cd.Marketplace) = lower(mm.Marketplace) ) SELECT Date, Marketplace, Collection, Target, Achievement, Final_Marketplace, Marketplace_Segments, Channel FROM Final_Data ORDER BY Date, Marketplace, Collection;",
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
            