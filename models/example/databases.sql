{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `maplemonk.Freakins_WH_FINAL_SKU_MASTER` AS WITH listing AS ( SELECT * FROM ( SELECT TRIM(UPPER(SKU_Code)) AS COMMONSKU, UPPER(Seller_SKU_on_Channel) AS MARKETPLACE_SKU, UPPER(Channel_Code) AS MARKETPLACE, ROW_NUMBER() OVER ( PARTITION BY UPPER(Channel_Code), UPPER(Seller_SKU_on_Channel) ORDER BY 1 ) AS rw FROM `maplemonk.Freakins_db_get_product_listing` ) WHERE rw = 1 ), product_master_SKU AS ( SELECT * FROM ( SELECT UPPER(Name) AS NAME, UPPER(COLOR) AS COLOUR, UPPER(BRAND) AS BRAND, UPPER(SIZE) AS SIZE, UPPER(Category_Name) AS CATEGORY, TRIM(UPPER(Product_Code)) AS COMMONSKU, ROW_NUMBER() OVER ( PARTITION BY TRIM(UPPER(Product_Code)) ORDER BY 1 ) AS rw FROM `maplemonk.unicommerce_Freakins_get_product_master` ) WHERE rw = 1 ) SELECT l.COMMONSKU AS skucode, l.MARKETPLACE_SKU, l.MARKETPLACE, pm.NAME, pm.CATEGORY, pm.COLOUR, FROM listing l LEFT JOIN product_master_SKU pm ON l.COMMONSKU = pm.COMMONSKU; CREATE OR REPLACE TABLE `MAPLEMONK.FREAKINS_FINALL_SKU_MASTER` AS SELECT Variant_option_1_value size, Product_metafield_value_at_custom_gender Gender, replace(product_id,\',\',\'\') as Product_id, replace(Variant_ID,\',\',\'\') as Variant_ID, Product_metafield_value_at_custom_style_code style_code, Variant_SKU, Product_type, Product_title, Product_status, Product_vendor, Product_metafield_value_at_custom_gender vertical, sm.category as product_category, sm.colour as product_colour FROM `freakins-wh.MAPLEMONK.Hextom_Sku_master_csv_from_mail` h LEFT JOIN (select skucode, marketplace_sku, category, colour from `maplemonk.Freakins_WH_FINAL_SKU_MASTER` qualify row_number() over (partition by skucode order by 1)=1 ) sm ON lower(sm.skucode) = lower(h.variant_sku) QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(Variant_SKU) ORDER BY Variant_ID) = 1;",
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
            