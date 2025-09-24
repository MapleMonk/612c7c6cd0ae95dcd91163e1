{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_Tranzact_Inventory_Table` AS WITH Inventory AS ( SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\', `Date`) AS Inventory_Date, SAFE_CAST(RM_SKU AS STRING) AS RM_SKU, SAFE_CAST(Location AS STRING) AS Location, SAFE_CAST(REPLACE(Stock_Value, \',\', \'\') AS FLOAT64) AS Stock_Value, SAFE_CAST(REPLACE(Stock_Available, \',\', \'\') AS FLOAT64) AS Stock_Available FROM `MapleMonk.Zouk_Tranzact_Inventory_Report` ), SKU_Master AS ( SELECT CAST(RM_SKU AS STRING) AS RM_SKU, CAST(RM_Name AS STRING) AS RM_Name, CAST(RM_Type AS STRING) AS RM_Type, CAST(RM_UOM AS STRING) AS RM_UOM, CAST(RM_Catgeory AS STRING) AS RM_Category, CAST(RM_Product_Bucket AS STRING) AS RM_Product_Bucket, CAST(RM_HSN AS STRING) AS RM_HSN, CAST(RM_TAX AS INT64) AS RM_TAX, SAFE_CAST(RM_Unit_Cost AS FLOAT64) AS RM_Unit_Cost FROM `MapleMonk.Zouk_RM_SKU_Master` ) SELECT i.Inventory_Date, i.RM_SKU, i.Location, i.Stock_Value, i.Stock_Available, s.RM_Name, s.RM_Type, s.RM_UOM, s.RM_Category, s.RM_Product_Bucket, s.RM_HSN, s.RM_TAX, s.RM_Unit_Cost FROM Inventory i LEFT JOIN SKU_Master s ON i.RM_SKU = s.RM_SKU;",
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
            