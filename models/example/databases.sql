{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.tuco_kids_zepto_sales_fact_items_intermediate; create table public.tuco_kids_zepto_sales_fact_items_intermediate as select EAN, EAN || \"sku number\" || upper(coalesce(s.city)) || date as order_item, CAST(MRP AS DOUBLE PRECISION) AS MRP, upper(coalesce(s.city)) as CITY, cast(null as varchar) as State, CAST(Date AS date) as Order_Date, \"sku name\" AS Product_name, \"brand name\" as Brand, \"sku number\" as sku_number, \"sku category\" as sku_category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as selling_price, \"manufacturer id\" as manufacturer_id, \"sku sub category\" as SKU_Sub_Category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Selling_Value, cast(\"sales (qty) - units\" as bigint) as quantity, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Merchandise_Value from public.zepto_tuco_kids_sales s ; drop table if exists public.tuco_kids_zepto_sales_fact_items; create table public.tuco_kids_zepto_sales_fact_items as select z.*, sm.commonsku, sm.product_name as product_name_final, sm.category as product_category, sm.product_type, sm.size, sm.color, sm.mrp, sm.cogs, sm.image from (select * from (select *, row_number() over (partition by Order_Date,sku_number,sku_category,city order by 1,2,3,4) as rw from public.tuco_kids_zepto_sales_fact_items_intermediate ) where rw=1 ) z left join (select * from ( select replace(marketplace_sku,\' \',\'\') as marketplace_sku, commonsku, product_name, category, product_type, size, color, mrp::DOUBLE PRECISION as mrp, cogs::DOUBLE PRECISION as cogs, image, row_number() over (partition by replace(marketplace_sku,\' \',\'\') order by 1) as rw from public.TUCO_KIDS_FINAL_SKU_MASTER where lower(marketplace) like \'%zepto%\' ) where rw=1 ) sm on lower(replace(sm.marketplace_sku,\' \',\'\')) = lower(replace(z.sku_number,\' \',\'\')) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            