{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_Tranzact_Inventory_Table` AS WITH Inventory AS ( SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\', `Date`) AS Inventory_Date, SAFE_CAST(RM_SKU AS STRING) AS RM_SKU, SAFE_CAST(Location AS STRING) AS Location, SAFE_CAST(REPLACE(Stock_Value, \',\', \'\') AS FLOAT64) AS Stock_Value, SAFE_CAST(REPLACE(Stock_Available, \',\', \'\') AS FLOAT64) AS Stock_Available FROM `MapleMonk.Zouk_Tranzact_Inventory_Report` ), New_Inventory AS ( SELECT DATE(_airbyte_emitted_at) AS Inventory_Date, SAFE_CAST(itemid AS STRING) AS RM_SKU, \'Bhiwandi RM Main Stock Store\' AS Location, SAFE_CAST(cal_final_stock_cost AS FLOAT64) AS Stock_Value, SAFE_CAST(cal_final_stock AS FLOAT64) AS Stock_Available FROM `MapleMonk.Zouk_stpl_get_product_price_and_inventory` ), SKU_Master AS ( SELECT CAST(RM_SKU AS STRING) AS RM_SKU, CAST(RM_Name AS STRING) AS RM_Name, CAST(RM_Type AS STRING) AS RM_Type, CAST(RM_UOM AS STRING) AS RM_UOM, CAST(RM_Catgeory AS STRING) AS RM_Category, CAST(RM_Product_Bucket AS STRING) AS RM_Product_Bucket, CAST(RM_HSN AS STRING) AS RM_HSN, CAST(RM_TAX AS INT64) AS RM_TAX, SAFE_CAST(RM_Unit_Cost AS FLOAT64) AS RM_Unit_Cost FROM `MapleMonk.Zouk_RM_SKU_Master` ) SELECT all_inv.Inventory_Date, all_inv.RM_SKU, all_inv.Location, all_inv.Stock_Value, all_inv.Stock_Available, s.RM_Name, s.RM_Type, s.RM_UOM, s.RM_Category, s.RM_Product_Bucket, s.RM_HSN, s.RM_TAX, s.RM_Unit_Cost FROM ( Select * from Inventory UNION ALL Select * from NEW_INVENTORY ) all_inv LEFT JOIN SKU_Master s ON all_inv.RM_SKU = s.RM_SKU",
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
            