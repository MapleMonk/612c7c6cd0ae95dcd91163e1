{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_wh_Order_Processing_Report AS WITH OP AS ( SELECT DATE_ADD(s.Start_D, INTERVAL n DAY) AS Date, UPPER(TRIM(Channel)) AS Channel, SAFE_DIVIDE( SAFE_CAST(Order_Projection AS INT64), (DATE_DIFF(s.End_D, s.Start_D, DAY) + 1) ) AS Daily_Order_Projection FROM ( SELECT *, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(Start_Date AS STRING)) AS Start_D, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(End_Date AS STRING)) AS End_D FROM `MapleMonk.Zouk_wh_Order_Processing` ) s, UNNEST(GENERATE_ARRAY(0, SAFE.DATE_DIFF(s.End_D, s.Start_D, DAY))) AS n ), Channel_Distribution AS ( SELECT DATE(Order_Date) AS Date, CASE WHEN UPPER(TRIM(Mode_Of_Business1)) LIKE \'OFFLINE%\' THEN \'OFFLINE\' WHEN UPPER(TRIM(Mode_of_Business1)) LIKE \'D2C\' THEN \'D2C\' WHEN UPPER(TRIM(Mode_Of_Business1)) LIKE \'%B2C\' THEN \'B2C\' WHEN UPPER(TRIM(Mode_Of_Business1)) LIKE \'%B2B\' THEN \'B2B\' ELSE NULL END AS Channel, COUNT(DISTINCT Order_id) AS Orders FROM `MapleMonk.zouk_UNICOMMERCE_FACT_ITEMS` GROUP BY 1, 2 ), Final AS ( SELECT d.Date, d.Channel, d.Orders, COALESCE(o.Daily_Order_Projection, 0) AS Daily_Order_Projection FROM Channel_Distribution d LEFT JOIN OP o ON d.Date = o.Date AND d.Channel = o.Channel ) SELECT Date, Channel, Orders, Daily_Order_Projection FROM Final ORDER BY 1, 2",
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
            