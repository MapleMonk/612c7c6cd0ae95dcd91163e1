{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS maplemonk.sku_swiggy_mapping( EAN STRING, commonsku STRING, category STRING, sub_category STRING, GST_Rate FLOAT64, Product_name STRING, HSN STRING, Product_Launch_date DATE, product_image_url STRING, cogs FLOAT64 ) ; Create Table If Not Exists maplemonk.swiggy_qc_sku_mapping( EAN String, SKU String, Zepto_SKU String, Swiggy_item_id String, Blinkit_item_id String, _airbyte_ab_id String, _airbyte_emitted_at Timestamp, _airbyte_normalized_at Timestamp, _airbyte_swiggy_qc_sku_mapping_hashid String ) ; CREATE OR REPLACE TABLE MAPLEMONK.sirona_SWIGGY_SALES_FACT_ITEMS AS SELECT distinct concat(Store_id,\'-\',item_code,Area_name,\'-\',upper(city),\'-\',variant,\'-\',CAST(UNITS_SOLD as float64),GMV,ordered_date) as order_id, CAST(GMV AS FLOAT64) AS selling_price, DATE(TIMESTAMP(ordered_Date)) AS order_date, upper(city) as CITY, VARIANT, CAST(BASE_MRP as float64) as MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD as float64) as quantity, COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(cast(fi.L2_category as string))) AS product_category, COALESCE(UPPER(cast(p.sub_category as string)),UPPER(cast(fi.L2_category as string))) AS product_sub_category, p.commonsku, p.EAN, p.GST_Rate, p.HSN, p.product_image_url, p.cogs, upper(fi.PRODUCT_NAME) as PRODUCT_NAME, CAST(BASE_MRP as float64) as Selling_Price_final FROM MapleMonk.swiggy_sirona_sales fi left join (select * from maplemonk.swiggy_qc_sku_mapping where swiggy_item_id <> \'-\' qualify row_number() over (partition by swiggy_item_id order by 1) =1 ) qc on qc.swiggy_item_id = fi.item_code left join (select * from (select EAN, commonsku, category, sub_category, GST_Rate, Product_name, HSN, Product_Launch_date, product_image_url, cogs, row_number()over (partition by EAN order by length(ifnull(EAN,\'\')) desc) rw from maplemonk.sku_swiggy_mapping) where rw = 1) p on lower(qc.EAN) = lower(p.EAN) ;",
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
            