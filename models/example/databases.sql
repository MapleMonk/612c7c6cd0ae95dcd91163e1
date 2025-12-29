{{ config(
            materialized='table',
                post_hook={
                    "sql": "drop table if exists public.anveshan_SWIGGY_SALES_FACT_ITEMS; CREATE TABLE public.anveshan_SWIGGY_SALES_FACT_ITEMS AS SELECT DISTINCT (Store_id || \'-\' || item_code || \'-\' || Area_name || \'-\' || UPPER(city) || \'-\' || variant || \'-\' || UNITS_SOLD || \'-\' || GMV || \'-\' || ordered_date)::text AS order_id, CAST(GMV AS NUMERIC) AS selling_price, CASE WHEN TRIM(ordered_date) LIKE \'____-__-__\' THEN CAST(TRIM(ordered_date) AS DATE) WHEN ordered_date::varchar SIMILAR TO \'[0-9]{2}-[0-9]{2}-[0-9]{4}\' THEN TO_DATE(ordered_date::varchar, \'DD-MM-YYYY\') ELSE TO_DATE(TRIM(ordered_date), \'DD/MM/YY\') END AS order_date, ordered_date, UPPER(city) AS CITY, VARIANT, CAST(BASE_MRP AS NUMERIC) AS MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD AS NUMERIC) AS quantity, COALESCE(UPPER(p.CATEGORY), UPPER(fi.L2_category)) AS product_category, p.commonsku, p.tax_Rate as pm_tax_rate, p.cogs, UPPER(fi.PRODUCT_NAME) AS PRODUCT_NAME, COALESCE(UPPER(p.Product_name), REPLACE(UPPER(fi.PRODUCT_NAME), \'anveshan \', \'\')) AS product_name_final, cast(gmv as double PRECISION) AS Selling_Price_final FROM public.anveshan_swiggy_instamart_anveshan_s3_instamart fi LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, swiggy_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY swiggy_sku ORDER BY LENGTH(COALESCE(swiggy_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(fi.item_code::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ;",
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
            