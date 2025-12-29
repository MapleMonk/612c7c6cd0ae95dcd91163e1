{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.gs_qc_sku_mapping; CREATE TABLE public.gs_qc_sku_mapping ( Blinkit_item_id VARCHAR(100), ean VARCHAR(20), zepto_sku VARCHAR(100), swiggy_item_id varchar(100) ); DROP TABLE IF EXISTS public.anveshan_Blinkit_sales_Fact_items_intermediate; CREATE TABLE public.anveshan_Blinkit_sales_Fact_items_intermediate AS SELECT mrp || \'-\' || item_id || City_name || \'-\' || city_id || \'-\' || \'-\' || CAST(CAST(qty_sold AS DOUBLE PRECISION) AS VARCHAR) || date AS order_id, CAST(date AS DATE) AS order_date, CAST(mrp AS DOUBLE PRECISION) AS mrp, city_name AS city, item_id AS product_id, CAST(REPLACE(qty_sold, \'.0\', \'\') AS BIGINT) AS quantity, item_name AS product_name, p.commonsku, COALESCE(p.category, bs.category) AS product_category, p.tax_rate as pm_tax_Rate, p.Product_name AS Product_name_final, p.cogs, CAST(mrp AS DOUBLE PRECISION)/quantity AS Selling_Price_final FROM public.blinkit_anveshan_sales_partner_biz bs LEFT JOIN ( SELECT * FROM ( SELECT master_sku as commonsku, blinkit_sku as marketplace_sku, parent_category as category, tax_rate, product_name, parent_mrp, cogs, ROW_NUMBER() OVER (PARTITION BY blinkit_sku ORDER BY LENGTH(COALESCE(blinkit_sku, \'\')) DESC) rw FROM public.anveshan_sku_master ) WHERE rw = 1 ) p ON LOWER(REPLACE(REPLACE(bs.item_id::varchar, \' \', \'\'),\'\"\',\'\')) = LOWER(p.marketplace_sku) ; DROP TABLE IF EXISTS public.anveshan_Blinkit_sales_Fact_items; CREATE TABLE public.anveshan_Blinkit_sales_Fact_items AS select * FROM ( select *, ROW_NUMBER() OVER (PARTITION BY order_date,mrp,city,product_id order by 1 desc) as rw from anveshan_Blinkit_sales_Fact_items_intermediate ) where rw=1;",
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
            