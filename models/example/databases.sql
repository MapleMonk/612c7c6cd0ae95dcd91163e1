{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.Tuco_Kids_Blinkit_sales_Fact_items_intermediate; CREATE TABLE public.Tuco_Kids_Blinkit_sales_Fact_items_intermediate AS SELECT bs.mrp || \'-\' || item_id || City_name || \'-\' || city_id || \'-\' || \'-\' || CAST(CAST(qty_sold AS DOUBLE PRECISION) AS VARCHAR) || date AS order_id, CAST(date AS DATE) AS order_date, CAST(bs.mrp AS DOUBLE PRECISION) AS mrp, upper(coalesce(city_name)) AS city, null::varchar AS state, item_id AS product_id, CAST(REPLACE(qty_sold, \'.0\', \'\') AS BIGINT) AS quantity, item_name AS product_name, p.commonsku, COALESCE(sm.category, bs.category) AS product_category, sm.Product_name AS Product_name_final, sm.size, sm.image, sm.mrp as mrp_sku_master, sm.cogs, upper(sm.product_type) as product_type, CAST(bs.mrp AS DOUBLE PRECISION)/quantity AS Selling_Price_final FROM public.blinkit_Tuco_sales_partner_biz bs left join (select * from ( select replace(identifier::varchar,\' \',\'\') as product_id, coalesce(replace(master_sku::varchar,\' \',\'\'),replace(marketplace_sku::varchar,\' \',\'\')) as commonsku, row_number() over (partition by replace(identifier::varchar,\' \',\'\') order by 1 desc) as rw from public.tk_gs_final_main_sku_master where upper(marketplace) like \'%BLINKIT%\' and len(product_id) < 7 ) where rw=1 ) p on lower(replace(p.product_id::varchar,\'\"\',\'\')) = lower(replace(bs.item_id::varchar,\'\"\',\'\')) left join (select * from ( select replace(replace(replace(commonsku::varchar,\'_fbaa\',\'\'),\'_fba\',\'\'),\'_mfn\',\'\') as commonsku, product_name, category, product_type, image, product_image_url, mrp, cogs, size, row_number() over (partition by replace(replace(replace(commonsku::varchar,\'_fbaa\',\'\'),\'_fba\',\'\'),\'_mfn\',\'\') order by 1 desc) as rw from public.TUCO_KIDS_FINAL_SKU_MASTER ) where rw=1 ) sm on p.commonsku::varchar = replace(sm.commonsku::varchar,\'\"\',\'\') ; DROP TABLE IF EXISTS public.Tuco_Kids_Blinkit_sales_Fact_items; CREATE TABLE public.Tuco_Kids_Blinkit_sales_Fact_items AS select * FROM ( select *, ROW_NUMBER() OVER (PARTITION BY order_date,mrp,city,product_id order by 1 desc) as rw from Tuco_Kids_Blinkit_sales_Fact_items_intermediate ) where rw=1 ;",
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
            