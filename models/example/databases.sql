{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_zepto_sales_fact_items_intermediate; create table public.anveshan_zepto_sales_fact_items_intermediate as select EAN, EAN || \"sku number\" || upper(coalesce(lm.city,s.city)) || date as order_item, CAST(MRP AS DOUBLE PRECISION) AS MRP, upper(coalesce(lm.city,s.city)) as CITY, lm.state as State, CAST(Date AS date) as Order_Date, \"sku name\" AS Product_name, \"brand name\" as Brand, \"sku number\" as sku_number, \"sku category\" as sku_category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as selling_price, \"manufacturer id\" as manufacturer_id, \"sku sub category\" as SKU_Sub_Category, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Selling_Value, cast(\"sales (qty) - units\" as bigint) as quantity, cast(\"gross merchandise value\" as DOUBLE PRECISION) as Gross_Merchandise_Value from public.zepto_anveshan_sales s LEFT JOIN (select * FROM ( select upper(city::varchar) as City, upper(trim(state::varchar)) as State, upper(trim(\"quick commerce city\"::varchar)) as qc_city_name, row_number() over (partition by upper(trim(\"quick commerce city\"::varchar)) order by 1) as rw FROM public.anveshan_qc_location_mapping ) where rw = 1 ) lm ON lower(lm.qc_city_name) = lower(cast(s.city as varchar)) ; drop table if exists public.anveshan_zepto_sales_fact_items; create table public.anveshan_zepto_sales_fact_items as select z.* ,UPPER(COALESCE(p.product_name,replace(lower(z.Product_name),\'anveshan \',\'\'))) AS product_name_final ,COALESCE(UPPER(p.category),UPPER(cast(z.sku_category as varchar))) AS product_category ,p.cogs ,p.tax_rate as pm_tax_rate ,cast(p.commonsku as varchar) as commonsku from public.anveshan_zepto_sales_fact_items_intermediate z LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, zepto_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY zepto_sku ORDER BY LENGTH(COALESCE(zepto_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(z.sku_number::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ;",
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
            