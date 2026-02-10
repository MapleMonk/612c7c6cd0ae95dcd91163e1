{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS maplemonk.zepto_SKU_Master AS SELECT CAST(NULL AS STRING) AS EAN, skucode, category, sub_category, CAST(NULL AS NUMBER) AS GST_Rate, name, CAST(NULL AS STRING) AS HSN, CAST(NULL AS DATE) AS Product_Launch_date, CAST(NULL AS STRING) AS product_image_url, CAST(NULL AS NUMBER) AS cogs, marketplace_sku FROM maplemonk.final_sku_master; CREATE TABLE IF NOT EXISTS maplemonk.gs_qc_sku_mapping ( EAN STRING, SKU STRING, Zepto_SKU STRING, SP_from_22_Sep NUMBER, SP_till_21_Sep NUMBER, Swiggy_item_id STRING, Blinkit_item_id STRING, _airbyte_ab_id STRING, _airbyte_emitted_at TIMESTAMP, _airbyte_normalized_at TIMESTAMP, _airbyte_GS_QC_SKU_MAPPING_hashid STRING ); CREATE OR REPLACE TABLE maplemonk.sleepycat_zepto_fact_items_intermediate AS SELECT EAN, CONCAT(EAN, \"SKU Number\", city, date) AS order_item, CAST(MRP AS FLOAT) AS MRP, CITY, CAST(Date AS TIMESTAMP) AS Order_Date, \"SKU Name\" AS Product_name, \"Brand Name\" AS Brand, \"SKU Number\" AS sku_number, \"SKU Category\" AS sku_category, CAST(\"Gross Merchandise Value\" AS FLOAT) AS selling_price, \"Manufacturer ID\" AS manufacturer_id, \"SKU Sub Category\" AS SKU_Sub_Category, CAST(\"Gross Merchandise Value\" AS FLOAT) AS Gross_Selling_Value, CAST(\"Sales (Qty) - Units\" AS NUMBER) AS quantity, CAST(\"Gross Merchandise Value\" AS FLOAT) AS Gross_Merchandise_Value FROM maplemonk.zepto_sleepycat_sales ; CREATE OR REPLACE TABLE maplemonk.sleepycat_zepto_fact_items AS SELECT z.*, selling_price AS Selling_Price_final, UPPER( COALESCE( p.name , REPLACE(LOWER(z.Product_name), \'Sleepycat \', \'\') ) ) AS product_name_final, COALESCE( UPPER(CAST(p.CATEGORY AS STRING)), UPPER(CAST(z.sku_category AS STRING)) ) AS product_category, COALESCE( UPPER(CAST(p.sub_category AS STRING)), UPPER(z.SKU_Sub_Category) ) AS sub_category, p.skucode as commonsku, p.EAN AS EAN_sku_master, p.GST_Rate, p.HSN, p.product_image_url, p.cogs FROM maplemonk.sleepycat_zepto_fact_items_intermediate z LEFT JOIN ( SELECT * FROM maplemonk.gs_qc_sku_mapping WHERE zepto_sku <> \'-\' QUALIFY ROW_NUMBER() OVER (PARTITION BY zepto_sku ORDER BY 1) = 1 ) qc ON qc.zepto_sku = z.sku_number LEFT JOIN ( SELECT * FROM ( SELECT EAN, skucode, category, sub_category, GST_Rate, name, HSN, Product_Launch_date, product_image_url, cogs, ROW_NUMBER() OVER ( PARTITION BY ean ORDER BY LENGTH(COALESCE(ean, \'\')) DESC ) AS rw FROM maplemonk.zepto_SKU_Master ) WHERE rw = 1 ) p ON LOWER(qc.ean) = LOWER(p.ean) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SLEEPYCAT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            