{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_zepto_sales_fact_items_intermediate; create table public.anveshan_zepto_sales_fact_items_intermediate as select EAN, EAN || \"sku number\" || city || date as order_item, CAST(MRP AS DOUBLE PRECISION) AS MRP, CITY, CAST(Date AS date) as Order_Date, \"sku name\" AS Product_name, \"brand name\" as Brand, \"sku number\" as sku_number, \"sku category\" as sku_category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as selling_price, \"manufacturer id\" as manufacturer_id, \"sku sub category\" as SKU_Sub_Category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Selling_Value, cast(\"sales (qty) - units\" as bigint) as quantity, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Merchandise_Value from public.zepto_anveshan_sales ; drop table if exists public.anveshan_zepto_sales_fact_items; create table public.anveshan_zepto_sales_fact_items as select z.* ,UPPER(COALESCE(replace(lower(z.Product_name),\'anveshan \',\'\'))) AS product_name_final ,COALESCE(UPPER(cast(z.sku_category as varchar))) AS product_category ,COALESCE(upper(z.SKU_Sub_Category)) AS sub_category ,cast(null as varchar) as commonsku from public.anveshan_zepto_sales_fact_items_intermediate z",
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
            