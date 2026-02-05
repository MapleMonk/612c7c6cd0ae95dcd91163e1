{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Zouk_wh_Dispatch_Targets_Report AS WITH Targets AS ( SELECT DATE_ADD(s.Start_D, INTERVAL n DAY) AS Date, SAFE_CAST(Target AS INT64) / (DATE_DIFF(s.End_D, s.Start_D, DAY) + 1) AS Daily_Target_Qty, UPPER(TRIM(Vendor_Code)) AS Vendor, UPPER(TRIM(Category)) AS Category FROM ( SELECT *, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(Start_Date AS STRING)) AS Start_D, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(End_Date AS STRING)) AS End_D FROM `MapleMonk.Zouk_wh_Dispatch_Targets` ) s, UNNEST(GENERATE_ARRAY(0, SAFE.DATE_DIFF(s.End_D, s.Start_D, DAY))) AS n ), Dispatch AS ( SELECT SAFE.PARSE_DATE(\'%d-%b-%y\', CAST(p.Dispatched_date AS STRING)) AS Date, SAFE_CAST(p.Dispatched_Qty AS INT64) AS Actual_Quantity, UPPER(TRIM(p.Vendor_Code)) AS Vendor, UPPER(TRIM(m.Category)) AS Category, CAST(p.Status AS STRING) AS Status, CAST(p.Final_Status AS STRING) AS Final_Status FROM `MapleMonk.Zouk_wh_Print_Tracker` p LEFT JOIN ( SELECT DISTINCT UPPER(TRIM(CATEGORY)) AS CATEGORY FROM `MapleMonk.FINAL_SKU_MASTER` ) m ON UPPER(TRIM(p.Category_Name)) = m.CATEGORY WHERE p.Dispatched_date IS NOT NULL AND p.Dispatched_Qty IS NOT NULL AND p.Final_Status = \'Full Kit dispatched\' ) SELECT COALESCE(t.Date, d.Date) AS Date, COALESCE(t.Category, d.Category) AS Category, COALESCE(t.Vendor, d.Vendor) AS Vendor, SUM(IFNULL(t.Daily_Target_Qty, 0)) AS Target, SUM(IFNULL(d.Actual_Quantity, 0)) AS Actual, d.Status AS Status, FROM Targets t FULL OUTER JOIN Dispatch d ON t.Date = d.Date AND t.Vendor = d.Vendor AND t.Category = d.Category GROUP BY 1, 2, 3,6 ORDER BY Date DESC;",
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
            