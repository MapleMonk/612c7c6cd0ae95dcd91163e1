{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_REED_Analysis_Report` AS WITH Demand_Day_Level AS ( SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\', Start_Date) AS Start_Date, SAFE.PARSE_DATE(\'%Y-%m-%d\', End_Date) AS End_Date, SKU, UPPER(Channel) as Channel, SUM(SAFE_CAST(Demand AS INT64)) / SUM(DATE_DIFF(SAFE.PARSE_DATE(\'%Y-%m-%d\', End_Date), SAFE.PARSE_DATE(\'%Y-%m-%d\', Start_Date), DAY) + 1) AS Daily_Demand, GENERATE_DATE_ARRAY(MIN(SAFE.PARSE_DATE(\'%Y-%m-%d\', Start_Date)),MAX(SAFE.PARSE_DATE(\'%Y-%m-%d\', End_Date))) AS Demand_Dates FROM `MapleMonk.Zouk_Demand` GROUP BY 1,2,3,4 ), Expanded_Demand AS ( SELECT SKU, UPPER(Channel) as Channel, DATE_ADD(Start_Date, INTERVAL n DAY) AS Demand_Date, Daily_Demand AS Daily_Demand FROM Demand_Day_Level t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(End_Date, Start_Date, DAY))) AS n ), Unicommerce_Sales AS ( SELECT DATE(order_date) AS order_date, commonsku AS SKU, UPPER(MARKETPLACE_SEGMENT) AS Channel, COUNT(saleOrderItemCode) AS Sold FROM `MapleMonk.zouk_UNICOMMERCE_FACT_ITEMS` WHERE LOWER(ORDER_STATUS) NOT LIKE \'%cancelled%\' GROUP BY 1, 2, 3 ) SELECT COALESCE(e.Demand_Date, u.order_date) AS Date, COALESCE(e.SKU, u.SKU) AS SKU, UPPER(COALESCE(e.Channel, u.Channel)) AS Channel, IFNULL(e.Daily_Demand, 0) AS Demand, IFNULL(u.Sold, 0) AS Sold_Quantity, fsm.Name, fsm.Product_Type, fsm.Collection, fsm.Category, fsm.Category_Code FROM Unicommerce_Sales u FULL OUTER JOIN Expanded_Demand e ON LOWER(u.SKU) = LOWER(e.SKU) AND LOWER(u.Channel) = LOWER(e.Channel) AND u.order_date = e.Demand_Date LEFT JOIN ( SELECT * FROM `MapleMonk.Final_SKU_Master` qualify ROW_NUMBER() OVER (PARTITION BY upper(COMMONSKU) ORDER BY length(ifnull(CATEGORY,\'\'))) = 1 )fsm on lower(fsm.COMMONSKU) = lower(COALESCE(u.SKU,e.SKU)) ;",
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
            