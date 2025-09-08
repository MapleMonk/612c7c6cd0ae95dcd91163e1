{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Demand_Analysis AS WITH Demand_Reed AS ( SELECT TRIM(UPPER(t.Channel)) AS Channel, TRIM(UPPER(t.SKU)) AS SKU, DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) AS Demand_Date, CAST(REPLACE(t.Demand,\',\',\'\') AS FLOAT64) / (DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY) + 1) AS Demand, CASE WHEN CURRENT_DATE() > DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY) THEN 0 ELSE DATE_DIFF(LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY)), CURRENT_DATE(), DAY) + 1 END AS days_Remaining, EXTRACT(DAY FROM LAST_DAY(DATE_ADD(DATE(t.Start_Date), INTERVAL n DAY))) AS no_of_days FROM `MapleMonk.Zouk_Demand_Forecast` t CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE(t.End_Date), DATE(t.Start_Date), DAY))) AS n ), SALES_cte AS ( SELECT Order_Date AS Date, COMMONSKU, FINAL_MARKETPLACE, SUM(IFNULL(Selling_Price,0)) AS Sales, SUM(IFNULL(Quantity,0)) AS Quantity_Sold FROM `MapleMonk.zouk_sales_consolidated` WHERE Order_Date < CURRENT_DATE() GROUP BY 1,2,3 ), Combined AS ( SELECT COALESCE(d.Demand_Date, s.Date) AS Date, COALESCE(d.SKU,s.COMMONSKU) AS SKU, COALESCE(d.Channel, s.Final_Marketplace) AS Marketplace, d.Demand, s.Sales, s.Quantity_Sold FROM Demand_Reed d FULL OUTER JOIN Sales_cte s ON d.Demand_Date = s.Date AND LOWER(TRIM(d.SKU)) = LOWER(TRIM(s.COMMONSKU)) AND LOWER(TRIM(Channel)) = LOWER(TRIM(Final_Marketplace)) ), Inventory AS ( SELECT DATA_FETCH_DATE, COMMONSKU AS SKU, SUM(Available_Inventory) AS Available_Inventory FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` WHERE DATE(DATA_FETCH_DATE) = CURRENT_DATE() GROUP BY 1,2 ) SELECT c.Date, c.SKU, c.Marketplace, c.Demand, c.Sales, c.Quantity_Sold, fsm.Collection, fsm.Category AS Product_Category, fsm.Print, fsm.Name AS Product_Name_Final, i.Available_Inventory FROM Combined c LEFT JOIN Inventory i ON LOWER(TRIM(c.SKU)) = LOWER(TRIM(i.SKU)) AND c.Date = Date(i.DATA_FETCH_DATE) LEFT JOIN ( SELECT * FROM `MapleMonk.FINAL_SKU_MASTER` QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku) ORDER BY 1) = 1 ) fsm ON LOWER(c.SKU) = LOWER(fsm.commonsku)",
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
            