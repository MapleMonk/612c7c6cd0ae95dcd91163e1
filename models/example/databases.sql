{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.Tuco_Kids_nykaa_sales_fact_items; CREATE TABLE public.Tuco_Kids_nykaa_sales_fact_items AS SELECT sku_code AS product_id, sku_name AS product_name, brand AS brand, seller_code, company_name AS company_name, seller_type AS seller_type, platform AS platform, CAST(date AS TIMESTAMP) AS order_date, CAST(MRP AS double precision) AS MRP, CAST(display_price AS double precision) AS display_price, CAST(Selling_Price AS double precision) AS selling_price, CAST(replace(Total_Qty::varchar,\'.0\',\'\') AS int4) AS quantity, CAST(Total_Orders AS int4) AS total_orders, CAST(Total_Customers AS int4) AS total_customers, Category_L1 AS category_l1, Category_L2 AS category_l2, Category_L3 AS category_l, sm.commonsku, upper(pm.product_name) as product_name_final, upper(pm.CATEGORY) as product_category, upper(pm.PRODUCT_TYPE) as product_type, upper(pm.Image) as image, upper(pm.size) as size FROM public.nykaa_sales_report as n left join (select * from ( select upper(case when replace(\"Master_SKU\",\'`\',\'\') = \'NA\' then replace(\"Marketplace_SKU\",\'`\',\'\') else replace(\"Master_SKU\",\'`\',\'\') end) commonsku, upper(replace(Identifier2,\' \',\'\')) product_id, row_number() over (partition by upper(replace(Identifier2,\' \',\'\')) order by length(upper(replace(Identifier2,\' \',\'\')) ) desc) as rw from public.unbottle_sku_master where lower(marketplace) like \'%nykaa%\' ) where rw = 1 ) sm ON LOWER(REPLACE(CAST(n.sku_code AS VARCHAR), \'\"\', \'\')) = LOWER(REPLACE(CAST(sm.product_id AS VARCHAR), \' \', \'\')) LEFT JOIN ( SELECT * FROM ( SELECT LOWER(REPLACE(REPLACE(CAST(SKU AS VARCHAR), \'`\', \'\'), \' \', \'\')) AS sku, UPPER(product_name) AS PRODUCT_NAME, UPPER(category_name) AS CATEGORY, UPPER(product_type) AS PRODUCT_TYPE, UPPER(SIZE) AS SIZE, \'<img src=\"\'|| product_image_url || \'\" width=\"70\">\' AS Image, ROW_NUMBER() OVER ( PARTITION BY LOWER(REPLACE(REPLACE(CAST(SKU AS VARCHAR), \'`\', \'\'), \' \', \'\')) ORDER BY product_image_url DESC ) AS rw FROM public.Easyecom_Tuco_Kids_product_master ) WHERE rw = 1 ) pm ON LOWER(REPLACE(REPLACE(CAST(sm.commonsku AS VARCHAR), \' \', \'\'), \'`\', \'\')) = pm.sku qualify row_number() over (partition by n.date, n.SKU_Code, n.platform order by n.date desc) = 1 ;",
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
            