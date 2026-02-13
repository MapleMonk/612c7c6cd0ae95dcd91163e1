{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Sirona_zepto_fact_items AS SELECT CAST(EAN AS STRING) AS EAN, CAST(MRP AS FLOAT64) AS MRP, CONCAT(CAST(Date AS STRING),\'_\',City,\'_\',SKU_Number,\'_\',Manufacturer_ID) AS order_id, CAST(City AS STRING) AS City, CAST(Date AS TIMESTAMP) AS Date, CAST(SKU_Name AS STRING) AS SKU_Name, CAST(Brand_Name AS STRING) AS Brand_Name, CAST(SKU_Number AS STRING) AS SKU_Number, CAST(SKU_Category AS STRING) AS SKU_Category, CAST(Selling_Price AS FLOAT64) AS Selling_Price, CAST(Manufacturer_ID AS STRING) AS Manufacturer_ID, CAST(SKU_Sub_Category AS STRING) AS SKU_Sub_Category, CAST(Manufacturer_Name AS STRING) AS Manufacturer_Name, CAST(Gross_Selling_Value AS FLOAT64) AS Gross_Selling_Value, CAST(Sales__Qty____Units AS INT64) AS Sales_Qty_Units, CAST(Gross_Merchandise_Value AS FLOAT64) AS Gross_Merchandise_Value FROM `MAPLEMONK.Sirona_Zepto_db_sales`;",
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
            