{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.neshanka_zepto_fact_items_intermediate as select EAN, concat(EAN,sku_number,city,date) as order_item, CAST(MRP AS FLOAT64) AS MRP, CITY, CAST(Date AS timestamp) as Order_Date, SKU_NAME AS Product_name, Brand_Name as Brand, sku_number, sku_category, cast(Gross_Merchandise_Value as float64) as selling_price, manufacturer_id, SKU_Sub_Category as sub_category, cast(Gross_Merchandise_Value as float64) as Gross_Selling_Value, cast(Sales__Qty____Units as int64) as quantity, cast(Gross_Merchandise_Value as float64) as Gross_Merchandise_Value from `MAPLEMONK.Zepto_neshanka_sales` ; create or replace table maplemonk.neshanka_zepto_fact_items as select z.* ,COALESCE(p.Product_name,replace(lower(z.Product_name),\'ritualistic \',\'\')) AS product_name_final ,COALESCE(UPPER(cast(p.CATEGORY as string)), UPPER(cast(z.sku_category as string))) AS product_category ,COALESCE(UPPER(cast(p.sub_category as string)),upper(z.sub_category)) AS sub_category ,p.commonsku ,p.EAN ,p.GST_Rate ,p.New_HSN_from_17_Sept_25 as HSN ,p.product_image_url ,p.cogs from maplemonk.neshanka_zepto_fact_items_intermediate z left join (select * from (select EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, row_number()over (partition by commonsku order by length(ifnull(commonsku,\'\')) desc) rw from neshanka-wh.maplemonk.Neshanka_SKU_Master ) where rw = 1) p on lower(replace(z.sku_number,\' \',\'\')) = lower(p.commonsku);",
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
            