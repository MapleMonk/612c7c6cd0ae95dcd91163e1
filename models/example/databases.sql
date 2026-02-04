{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.Tuco_Kids_SWIGGY_SALES_FACT_ITEMS; CREATE TABLE public.Tuco_Kids_SWIGGY_SALES_FACT_ITEMS AS SELECT (Store_id || \'-\' || item_code || \'-\' || Area_name || \'-\' || UPPER(coalesce(fi.city)) || \'-\' || variant || \'-\' || UNITS_SOLD || \'-\' || GMV || \'-\' || ordered_date)::text AS order_id, CAST(GMV AS NUMERIC) AS selling_price, CASE WHEN TRIM(ordered_date) LIKE \'____-__-__\' THEN CAST(TRIM(ordered_date) AS DATE) WHEN ordered_date::varchar SIMILAR TO \'[0-9]{2}-[0-9]{2}-[0-9]{4}\' THEN TO_DATE(ordered_date::varchar, \'DD-MM-YYYY\') ELSE TO_DATE(TRIM(ordered_date), \'DD/MM/YY\') END AS order_date, ordered_date, UPPER(coalesce(fi.city)) AS CITY, cast(null as varchar) as State, VARIANT, CAST(BASE_MRP AS NUMERIC) AS MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD AS NUMERIC) AS quantity, COALESCE(UPPER(p.CATEGORY), UPPER(fi.L2_category)) AS product_category, p.commonsku, p.size, p.product_type, p.color, p.mrp as mrp_sku_master, p.cogs, UPPER(fi.PRODUCT_NAME) AS PRODUCT_NAME, COALESCE(UPPER(p.Product_name), REPLACE(UPPER(fi.PRODUCT_NAME), \'Tuco_Kids \', \'\')) AS product_name_final, cast(gmv as double PRECISION) AS Selling_Price_final FROM public.Swiggy_sales fi left join (select * from ( select replace(marketplace_sku,\' \',\'\') as marketplace_sku, commonsku, product_name, category, product_type, size, color, mrp::DOUBLE PRECISION as mrp, cogs::DOUBLE PRECISION as cogs, image, row_number() over (partition by replace(marketplace_sku,\' \',\'\') order by 1) as rw from public.TUCO_KIDS_FINAL_SKU_MASTER where lower(marketplace) like \'%swiggy%\' ) where rw=1 ) p ON LOWER(REPLACE(REPLACE(fi.item_code::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ;",
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
            