{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE IFB_DB.MAPLEMONK.IFB_zepto_fact_items_intermediate AS SELECT EAN, CONCAT(EAN, \"SKU Number\", city, CAST(date AS STRING)) AS order_item, CAST(MRP AS FLOAT) AS MRP, CITY, CAST(Date AS TIMESTAMP_NTZ) AS Order_Date, \"SKU Name\" AS Product_name, \"Brand Name\" AS Brand, \"SKU Number\" as SKU_Number, \"SKU Category\" as sku_category, CAST(\"Gross Merchandise Value\" AS FLOAT) AS selling_price, \"Manufacturer ID\" as manufacturer_id, \"SKU Sub Category\" as SKU_Sub_Category, CAST(\"Gross Merchandise Value\" AS FLOAT) AS Gross_Selling_Value, CAST(\"Sales (Qty) - Units\" AS INT) AS quantity, CAST(\"Gross Merchandise Value\" AS FLOAT) AS Gross_Merchandise_Value FROM IFB_DB.MAPLEMONK.IFB_Zepto_sales; CREATE OR REPLACE TABLE IFB_DB.MAPLEMONK.IFB_zepto_fact_items AS SELECT z.* ,UPPER(REPLACE(LOWER(CAST(z.Product_name AS STRING)), \'IFB \', \'\')) AS product_name_final ,UPPER(CAST(z.sku_category AS STRING)) AS product_category ,UPPER(z.SKU_Sub_Category) AS sub_category FROM IFB_DB.MAPLEMONK.IFB_zepto_fact_items_intermediate z ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from IFB_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            