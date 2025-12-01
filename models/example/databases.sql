{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.gs_qc_sku_mapping; CREATE TABLE public.gs_qc_sku_mapping ( Blinkit_item_id VARCHAR(100), ean VARCHAR(20), zepto_sku VARCHAR(100) ); DROP TABLE IF EXISTS public.anveshan_Blinkit_sales_Fact_items; CREATE TABLE public.anveshan_Blinkit_sales_Fact_items AS SELECT mrp || \'-\' || item_id || City_name || \'-\' || city_id || \'-\' || \'-\' || CAST(CAST(qty_sold AS DOUBLE PRECISION) AS VARCHAR) || date AS order_id, CAST(date AS DATE) AS order_date, CAST(mrp AS DOUBLE PRECISION) AS mrp, city_name AS city, item_id AS product_id, CAST(REPLACE(qty_sold, \'.0\', \'\') AS BIGINT) AS quantity, item_name AS product_name, p.commonsku, COALESCE(p.category, bs.category) AS product_category, p.sub_category, p.GST_Rate, p.EAN, p.Product_name AS Product_name_final, p.New_HSN_from_17_Sept_25, p.Product_Launch_date, p.product_image_url, p.cogs, CAST(mrp AS DOUBLE PRECISION)/quantity AS Selling_Price_final FROM public.blinkit_anveshan_sales_partner_biz bs LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY Blinkit_item_id ORDER BY 1) AS rn FROM public.gs_qc_sku_mapping WHERE Blinkit_item_id <> \'-\' ) WHERE rn = 1 ) qc ON qc.Blinkit_item_id = bs.item_id LEFT JOIN ( SELECT * FROM ( SELECT EAN, commonsku, category, sub_category, GST_Rate, Product_name, New_HSN_from_17_Sept_25, Product_Launch_date, product_image_url, cogs, ROW_NUMBER() OVER (PARTITION BY ean ORDER BY LENGTH(COALESCE(ean, \'\')) DESC) AS rw FROM public.anveshan_SKU_Master ) WHERE rw = 1 ) p ON LOWER(qc.ean) = LOWER(p.ean);",
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
            