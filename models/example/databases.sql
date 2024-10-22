{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table MapleMonk.Inventory_WIP_Only AS SELECT DATETIME_TRUNC(`DATA_FETCH_DATE`, DAY) AS `DATA_FETCH_DATE`, `Category` AS `Category`, SKU, Product_Final_Name, COLOR, SUB_CATEGORY, COLLECTION, PRINT, PRODUCT_TYPE, BAU_ONLINE, sum(ifnull(AVAILABLE_INVENTORY, 0)) AS Total_Inventory, sum(ifnull(WIP_INVENTORY, 0)) AS WIP_Inventory FROM `MapleMonk`.`ZOUK_INVENTORY_FACT_ITEMS` GROUP BY 1,2,3,4,5,6,7,8,9,10",
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
            