{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.Zilo_fact_item as WITH raw_data_cleaning AS ( SELECT *, UPPER(TRIM(\"Lineitem sku\")) AS raw_sku, CONTAINS(UPPER(TRIM(\"Lineitem sku\")), \'_\') AS has_underscore FROM snitch_db.maplemonk.S3_ZILO_SALES_RETURNS ), sales AS ( SELECT CASE WHEN has_underscore THEN SPLIT_PART(raw_sku, \'_\', 1) || \'-\' || SPLIT_PART(raw_sku, \'_\', -1) ELSE raw_sku END AS sku, REGEXP_REPLACE( CASE WHEN has_underscore THEN SPLIT_PART(raw_sku, \'_\', 1) || \'-\' || SPLIT_PART(raw_sku, \'_\', -1) ELSE raw_sku END, \'-[^-]*$\', \'\' ) AS sku_group, CASE WHEN has_underscore THEN SPLIT_PART(raw_sku, \'_\', -1) ELSE SPLIT_PART(raw_sku, \'-\', -1) END AS size, COALESCE(TRY_CAST(\"QTY\" AS INT), 0) AS total_quantity, CASE WHEN UPPER(TRIM(\"Order Status\")) IN (\'RETURN\', \'RETURNS\', \'RTO\', \'RTO1\') THEN COALESCE(TRY_CAST(\"QTY\" AS INT), 0) ELSE 0 END AS return_quantity, UPPER(TRIM(\"Order Status\")) AS order_status, COALESCE(TRY_CAST(\"SP\" AS FLOAT), 0) AS total_amount, COALESCE(TRY_CAST(\"MRP\" AS INT), 0) AS mrp, CASE WHEN TRY_CAST(\"QTY\" AS INT) > 0 THEN COALESCE(TRY_CAST(\"SP\" AS FLOAT), 0) / TRY_CAST(\"QTY\" AS INT) ELSE 0 END AS sp, GREATEST( (COALESCE(TRY_CAST(\"MRP\" AS INT), 0) - (CASE WHEN TRY_CAST(\"QTY\" AS INT) > 0 THEN COALESCE(TRY_CAST(\"SP\" AS FLOAT), 0) / TRY_CAST(\"QTY\" AS INT) ELSE 0 END)), 0 ) AS discount, TRY_TO_DATE(\"Order Date\", \'DD-MM-YYYY\') AS order_date FROM raw_data_cleaning WHERE \"Order Date\" IS NOT NULL ), prod_details AS ( SELECT sku_group, product_name, sku_class, category, title, print_design, collar_new, material_new, fit, color, sleeve_type FROM snitch_db.maplemonk.metafields_data QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY status DESC) = 1 ) SELECT a.sku, a.sku_group, a.size, a.sp, a.mrp, a.discount, a.order_date, a.total_amount, a.total_quantity, CASE WHEN a.return_quantity > 0 THEN a.order_date ELSE NULL END AS return_date, CASE WHEN a.return_quantity > 0 THEN a.order_status ELSE NULL END AS return_status, a.return_quantity, CASE WHEN a.return_quantity > 0 THEN a.order_status ELSE NULL END AS return_reason, CASE WHEN a.return_quantity > 0 THEN a.order_status ELSE NULL END AS categorized_return_reason, c.product_name, c.sku_class, c.category, c.title, c.print_design, c.collar_new, c.material_new, c.fit, c.color, c.sleeve_type FROM sales a LEFT JOIN prod_details c ON a.sku_group = c.sku_group;",
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
            