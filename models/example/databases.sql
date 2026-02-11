{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_wh_Warehouse_Cost_Report AS WITH Wc AS ( SELECT DATE_ADD(s.Start_D, INTERVAL n DAY) AS Date, UPPER(TRIM(Channel)) AS Channel, SAFE_DIVIDE( SAFE_CAST(REPLACE(CAST(Warehouse_Cost AS STRING), \',\', \'\') AS NUMERIC), (DATE_DIFF(s.End_D, s.Start_D, DAY) + 1) ) AS Daily_Warehouse_Cost FROM ( SELECT *, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(Start_Date AS STRING)) AS Start_D, SAFE.PARSE_DATE(\'%Y-%m-%d\', CAST(End_Date AS STRING)) AS End_D FROM `MapleMonk.Zouk_wh_Warehouse_Cost` ) s, UNNEST(GENERATE_ARRAY(0, SAFE.DATE_DIFF(s.End_D, s.Start_D, DAY))) AS n ), Order_Metrics AS ( SELECT DATE(Order_Date) AS Date, CASE WHEN UPPER(TRIM(marketplace)) LIKE ANY (\'%EBO%\',\'%SHOPIFY%\') THEN \'D2C\' ELSE Mode_Of_Business2 END AS Channel, COUNT(DISTINCT Order_Id) AS Orders, COUNT(Order_Id) AS Quantity FROM `MapleMonk.Zouk_Unicommerce_FACT_ITEMS` GROUP BY 1, 2 ), Final_Data AS ( SELECT o.Date, o.Channel, o.Orders, o.Quantity, COALESCE(w.Daily_Warehouse_Cost, 0) AS Daily_Warehouse_Cost, FROM Order_Metrics o LEFT JOIN Wc w ON o.Date = w.Date AND o.Channel = w.Channel ) SELECT Date, Channel, Orders, Quantity, Daily_Warehouse_Cost, FROM Final_Data ORDER BY Date DESC",
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
            