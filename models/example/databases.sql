{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.gs_qc_sku_mapping; CREATE TABLE public.gs_qc_sku_mapping ( Blinkit_item_id VARCHAR(100), ean VARCHAR(20), zepto_sku VARCHAR(100), swiggy_item_id varchar(100) ); drop table if exists public.anveshan_SWIGGY_SALES_FACT_ITEMS; CREATE TABLE public.anveshan_SWIGGY_SALES_FACT_ITEMS AS SELECT DISTINCT (Store_id || \'-\' || item_code || \'-\' || Area_name || \'-\' || UPPER(city) || \'-\' || variant || \'-\' || UNITS_SOLD || \'-\' || GMV || \'-\' || ordered_date)::text AS order_id, CAST(GMV AS NUMERIC) AS selling_price, TO_DATE(TRIM(ordered_date), \'DD/MM/YY\') AS order_date, UPPER(city) AS CITY, VARIANT, CAST(BASE_MRP AS NUMERIC) AS MRP, STORE_ID, AREA_NAME, ITEM_CODE, CAST(UNITS_SOLD AS NUMERIC) AS quantity, COALESCE(UPPER(p.CATEGORY), UPPER(fi.L2_category)) AS product_category, COALESCE(UPPER(p.sub_category), UPPER(fi.L2_category)) AS product_sub_category, p.commonsku, p.EAN, p.GST_Rate, p.New_HSN_from_17_Sept_25 AS HSN, p.product_image_url, p.cogs, UPPER(fi.PRODUCT_NAME) AS PRODUCT_NAME, COALESCE(UPPER(p.Product_name), REPLACE(UPPER(fi.PRODUCT_NAME), \'anveshan \', \'\')) AS product_name_final, cast(gmv as double PRECISION) AS Selling_Price_final FROM public.anveshan_swiggy_instamart_anveshan_s3_instamart fi LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY swiggy_item_id ORDER BY 1) AS rn FROM public.gs_qc_sku_mapping WHERE swiggy_item_id <> \'-\' ) subq WHERE rn = 1 ) qc ON qc.swiggy_item_id = fi.item_code LEFT JOIN ( SELECT * FROM ( SELECT EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, ROW_NUMBER() OVER (PARTITION BY ean ORDER BY LENGTH(COALESCE(ean, \'\')) DESC) AS rw FROM public.anveshan_sku_master ) subq2 WHERE rw = 1 ) p ON LOWER(qc.ean) = LOWER(p.ean);",
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
            