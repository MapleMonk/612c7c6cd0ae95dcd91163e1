{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.Tuco_Kids_SWIGGY_SALES_FACT_ITEMS; CREATE TABLE public.Tuco_Kids_SWIGGY_SALES_FACT_ITEMS AS SELECT (Store_id || \'-\' || item_code || \'-\' || Area_name || \'-\' || UPPER(coalesce(fi.city)) || \'-\' || variant || \'-\' || UNITS_SOLD || \'-\' || GMV || \'-\' || ordered_date)::text AS order_id, CAST(GMV AS NUMERIC) AS selling_price, CASE WHEN TRIM(ordered_date) LIKE \'____-__-__\' THEN CAST(TRIM(ordered_date) AS DATE) WHEN ordered_date::varchar SIMILAR TO \'[0-9]{2}-[0-9]{2}-[0-9]{4}\' THEN TO_DATE(ordered_date::varchar, \'DD-MM-YYYY\') ELSE TO_DATE(TRIM(ordered_date), \'DD/MM/YY\') END AS order_date, ordered_date, UPPER(coalesce(fi.city)) AS CITY, cast(null as varchar) as State, VARIANT, CAST(BASE_MRP AS NUMERIC) AS MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD AS NUMERIC) AS quantity, COALESCE(UPPER(sm.CATEGORY), UPPER(fi.L2_category)) AS product_category, coalesce(p.commonsku,p1.commonsku) as commonsku, sm.size, sm.product_type, sm.mrp as mrp_sku_master, sm.cogs, UPPER(fi.PRODUCT_NAME) AS PRODUCT_NAME, COALESCE(UPPER(sm.Product_name), REPLACE(UPPER(fi.PRODUCT_NAME), \'Tuco_Kids \', \'\')) AS product_name_final, cast(gmv as double PRECISION) AS Selling_Price_final FROM public.tuco_kids_sales fi left join (select * from ( select replace(identifier::varchar,\' \',\'\') as product_id, coalesce(replace(master_sku::varchar,\' \',\'\'),replace(marketplace_sku::varchar,\' \',\'\')) as commonsku, row_number() over (partition by replace(identifier::varchar,\' \',\'\') order by 1 desc) as rw from public.unbottle_sku_master where upper(marketplace) like \'%SWIGGY%\' ) where rw=1 ) p on lower(replace(p.product_id::varchar,\'\"\',\'\')) = lower(replace(fi.ITEM_CODE::varchar,\'\"\',\'\')) left join (select * from ( select replace(id::varchar,\' \',\'\') as product_id, coalesce(replace(\"master sku\"::varchar,\' \',\'\'),replace(\"marketplace sku\"::varchar,\' \',\'\')) as commonsku, row_number() over (partition by replace(id::varchar,\' \',\'\') order by 1 desc) as rw from public.unbottle_sku_listing_master where upper(marketplace) like \'%SWIGGY%\' ) where rw=1 ) p1 on lower(replace(p1.product_id::varchar,\'\"\',\'\')) = lower(replace(fi.ITEM_CODE::varchar,\'\"\',\'\')) left join (select * from ( select replace(replace(replace(commonsku::varchar,\'_fbaa\',\'\'),\'_fba\',\'\'),\'_mfn\',\'\') as commonsku, product_name, category, product_type, image, product_image_url, mrp, cogs, size, row_number() over (partition by replace(replace(replace(commonsku::varchar,\'_fbaa\',\'\'),\'_fba\',\'\'),\'_mfn\',\'\') order by 1 desc) as rw from public.TUCO_KIDS_FINAL_SKU_MASTER ) where rw=1 ) sm on coalesce(p.commonsku::varchar,p1.commonsku::varchar) = replace(sm.commonsku::varchar,\'\"\',\'\') ;",
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
            