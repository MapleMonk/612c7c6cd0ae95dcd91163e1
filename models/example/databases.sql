{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_RM_Dispatch_actual_vs_target AS WITH Targets AS ( WITH Parsed_dates AS ( SELECT PARSE_DATE(\'%m/%d/%Y\',Start_Date) AS Start_Date, PARSE_DATE(\'%m/%d/%Y\', End_Date) AS End_Date, CAST(Target AS FLOAT64) AS Target, UPPER(Vendor) AS Vendor, UPPER(Product_Category) AS Product_Category FROM `MapleMonk.Zouk_RM_Dispatch_Target` ), Date_Series AS ( SELECT Start_date, End_Date, Target, Vendor, Product_Category, DATE_ADD(Start_Date, INTERVAL day_offset DAY) AS daily_date FROM Parsed_dates, UNNEST(GENERATE_ARRAY(0, DATE_DIFF(End_Date, Start_Date, DAY))) AS day_offset ) SELECT daily_date AS Date, Target / (DATE_DIFF(End_Date, Start_Date, DAY) + 1) AS Daily_Target, Vendor, Product_Category FROM Date_Series ORDER BY Date ), Dispatch AS ( SELECT PARSE_DATE(\'%m/%d/%Y\', Date) AS Date, SAFE_CAST(Actual_Quantity AS FLOAT64) AS Actual_Quantity, SAFE_CAST(Quantity_Planned AS FLOAT64) AS Quantity_Planned, CAST(Status AS STRING) AS Status, UPPER(Vendor) AS Vendor, UPPER(Product_Category) AS Product_Category FROM `MapleMonk.Zouk_RM_Dispatch` WHERE Date IS NOT NULL AND Date != \'#REF!\' ) SELECT COALESCE(d.Date,t.Date) AS Date, UPPER(COALESCE(d.Product_Category,t.Product_Category)) AS Product_Category, UPPER(COALESCE(d.Vendor,t.Vendor)) AS Vendor, IFNULL(d.Actual_Quantity,0) AS Actual_Quantity, IFNULL(d.Quantity_Planned,0) AS Quantity_Planned, IFNULL(t.Daily_Target,0) AS Target, d.Status FROM Dispatch d FULL OUTER JOIN Targets t ON LOWER(d.Product_Category) = LOWER(t.Product_Category) AND CAST(d.Date AS Date) = CAST(t.Date AS Date) AND LOWER(d.Vendor) = LOWER(t.Vendor) ;",
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
            