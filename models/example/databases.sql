{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE if not exists `maplemonk.eatanytime_SKU_Master` ( EAN STRING, commonsku STRING, category STRING, sub_category STRING, GST_Rate NUMERIC, Product_name STRING, New_HSN_from_17_Sept_25 STRING, Product_Launch_date DATE, product_image_url STRING, cogs NUMERIC ); CREATE TABLE if not exists `maplemonk.gs_qc_sku_mapping` ( EAN STRING, SKU STRING, Zepto_SKU STRING, SP_from_22_Sep NUMERIC, SP_till_21_Sep NUMERIC, Swiggy_item_id STRING, Blinkit_item_id STRING, _airbyte_ab_id STRING, _airbyte_emitted_at TIMESTAMP, _airbyte_normalized_at TIMESTAMP, _airbyte_GS_QC_SKU_MAPPING_hashid STRING ); create or replace table maplemonk.eatanytime_zepto_fact_items_intermediate as select EAN, concat(EAN,sku_number,city,date) as order_item, CAST(MRP AS FLOAT64) AS MRP, CITY, CAST(Date AS timestamp) as Order_Date, SKU_NAME AS Product_name, Brand_Name as Brand, sku_number, sku_category, cast(Gross_Merchandise_Value as float64) as selling_price, manufacturer_id, SKU_Sub_Category, cast(Gross_Merchandise_Value as float64) as Gross_Selling_Value, cast(Sales__Qty____Units as int64) as quantity, cast(Gross_Merchandise_Value as float64) as Gross_Merchandise_Value from `MAPLEMONK.zepto_sales_sales` ; create or replace table maplemonk.eatanytime_zepto_fact_items as select z.* ,(case when Order_Date < \'2025-09-22\' then cast(SP_till_21_Sep as float64) else cast(SP_from_22_Sep as float64) end) * quantity as Selling_Price_final ,UPPER(COALESCE(p.Product_name,replace(lower(z.Product_name),\'eat anytime \',\'\'))) AS product_name_final ,COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(cast(z.sku_category as string))) AS product_category ,COALESCE(UPPER(cast(p.sub_category as string)),upper(z.SKU_Sub_Category)) AS sub_category ,p.commonsku ,p.EAN EAN_sku_master ,p.GST_Rate ,p.New_HSN_from_17_Sept_25 as HSN ,p.product_image_url ,p.cogs from maplemonk.eatanytime_zepto_fact_items_intermediate z left join (select * from maplemonk.gs_qc_sku_mapping where zepto_sku <> \'-\' qualify row_number() over (partition by zepto_sku order by 1) =1 ) qc on qc.zepto_sku = z.sku_number left join (select * from (select EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, row_number()over (partition by ean order by length(ifnull(ean,\'\')) desc) rw from maplemonk.eatanytime_SKU_Master ) where rw = 1) p on lower(qc.ean) = lower(p.ean);",
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
            