{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.Tuco_Kids_Blinkit_sales_Fact_items_intermediate; CREATE TABLE public.Tuco_Kids_Blinkit_sales_Fact_items_intermediate AS SELECT bs.mrp || \'-\' || item_id || City_name || \'-\' || city_id || \'-\' || \'-\' || CAST(CAST(qty_sold AS DOUBLE PRECISION) AS VARCHAR) || date AS order_id, CAST(date AS DATE) AS order_date, CAST(bs.mrp AS DOUBLE PRECISION) AS mrp, upper(coalesce(city_name)) AS city, null::varchar AS state, item_id AS product_id, CAST(REPLACE(qty_sold, \'.0\', \'\') AS BIGINT) AS quantity, item_name AS product_name, p.commonsku, COALESCE(p.category, bs.category) AS product_category, p.Product_name AS Product_name_final, p.size, p.color, p.image, p.mrp as mrp_sku_master, p.cogs, CAST(bs.mrp AS DOUBLE PRECISION)/quantity AS Selling_Price_final FROM public.blinkit_Tuco_sales_partner_biz bs left join (select * from ( select replace(marketplace_sku,\' \',\'\') as marketplace_sku, commonsku, product_name, category, product_type, size, color, mrp::DOUBLE PRECISION as mrp, cogs::DOUBLE PRECISION as cogs, image, row_number() over (partition by replace(marketplace_sku,\' \',\'\') order by 1) as rw from public.TUCO_KIDS_FINAL_SKU_MASTER where lower(marketplace) like \'%blinkit%\' ) where rw=1 ) p ON LOWER(REPLACE(REPLACE(bs.item_id::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ; DROP TABLE IF EXISTS public.Tuco_Kids_Blinkit_sales_Fact_items; CREATE TABLE public.Tuco_Kids_Blinkit_sales_Fact_items AS select * FROM ( select *, ROW_NUMBER() OVER (PARTITION BY order_date,mrp,city,product_id order by 1 desc) as rw from Tuco_Kids_Blinkit_sales_Fact_items_intermediate ) where rw=1;",
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
            