{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.knot_fact_item as WITH knot_sales AS ( SELECT UPPER(TRIM(SKU)) AS sku, SPLIT_PART(UPPER(TRIM(SKU)), \'-\', 1) || \'-\' || SPLIT_PART(UPPER(TRIM(SKU)), \'-\', 2) AS sku_group, SIZE, TO_DATE(\"Sold On\", \'DD-MM-YYYY\') AS order_date, \"QUANTITY\"::int AS TOTAL_QUANTITY, \"Selling Price\"::int AS selling_price, \"Product Name\" AS product_name, \"Color Family\" AS color_family, \"Product URL\" AS product_url FROM snitch_db.maplemonk.s3_knot_sales ), agg_returns AS ( SELECT UPPER(TRIM(SKU)) AS sku, SPLIT_PART(UPPER(TRIM(SKU)), \'-\', 1) || \'-\' || SPLIT_PART(UPPER(TRIM(SKU)), \'-\', 2) AS sku_group, MAX(TO_DATE(\"Returned on\", \'DD-Mon-YY\')) AS latest_return_date, SUM(\"Qty Returned\"::int) AS RETURN_QUANTITY, MAX(\"Selling Price\"::int) AS return_price, MAX(\"NAME\") AS return_product_name, MAX(\"Product Link\") AS return_product_url FROM snitch_db.maplemonk.s3_knot_returns GROUP BY 1, 2 ), metafields AS ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY status DESC) AS rn FROM snitch_db.maplemonk.metafields_data ) mf WHERE rn = 1 ) SELECT COALESCE(s.sku, r.sku) AS sku, COALESCE(s.sku_group, r.sku_group) AS sku_group, COALESCE(s.SIZE, REGEXP_SUBSTR(COALESCE(s.sku, r.sku), \'[^-]+$\')) AS SIZE, COALESCE(s.order_date, r.latest_return_date) AS order_date, COALESCE(s.TOTAL_QUANTITY, 0) + COALESCE(r.RETURN_QUANTITY, 0) AS TOTAL_QUANTITY, COALESCE(s.selling_price, r.return_price, 0) AS selling_price, COALESCE(s.product_name, r.return_product_name, \'-\') AS product_name, COALESCE(s.color_family, \'-\') AS color_family, COALESCE(s.product_url, r.return_product_url, \'-\') AS product_url, r.latest_return_date, COALESCE(r.RETURN_QUANTITY, 0) AS RETURN_QUANTITY, COALESCE(r.return_price, s.selling_price, 0) AS return_price, COALESCE(r.return_product_name, s.product_name, \'-\') AS return_product_name, COALESCE(r.return_product_url, s.product_url, \'-\') AS return_product_url, m.category, m.title, m.material_new, m.print_design, m.fit, m.sleeve_type, m.collar_new FROM knot_sales s FULL OUTER JOIN agg_returns r ON s.sku = r.sku AND s.order_date = r.latest_return_date LEFT JOIN metafields m ON COALESCE(s.sku_group, r.sku_group) = m.sku_group;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            