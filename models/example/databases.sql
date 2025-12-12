{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.Mnow_fact_item as WITH sales AS ( SELECT UPPER(TRIM(VENDOR_ARTICLE_NUMBER)) AS sku, REGEXP_REPLACE(UPPER(TRIM(VENDOR_ARTICLE_NUMBER)), \'-[^-]*$\', \'\') AS sku_group, SIZE AS size, COALESCE( CASE WHEN TRY_CAST(NULLIF(TRIM(SALES), \'\') AS INT) > 0 THEN TRY_CAST(NULLIF(TRIM(REVENUE), \'\') AS INT) / TRY_CAST(NULLIF(TRIM(SALES), \'\') AS INT) ELSE 0 END, 0) AS sp, COALESCE(TRY_CAST(NULLIF(TRIM(ARTICLE_MRP), \'\') AS INT), 0) AS mrp, GREATEST( ( COALESCE(TRY_CAST(NULLIF(TRIM(ARTICLE_MRP), \'\') AS INT), 0) - COALESCE( CASE WHEN TRY_CAST(NULLIF(TRIM(SALES), \'\') AS INT) > 0 THEN TRY_CAST(NULLIF(TRIM(REVENUE), \'\') AS INT) / TRY_CAST(NULLIF(TRIM(SALES), \'\') AS INT) ELSE 0 END, 0) ), 0 ) AS discount, TRY_TO_DATE(ORDER_CREATED_DATE, \'DD-MM-YYYY\') AS order_date, COALESCE(TRY_CAST(NULLIF(TRIM(REVENUE), \'\') AS INT), 0) AS total_amount, COALESCE(TRY_CAST(NULLIF(TRIM(SALES), \'\') AS INT), 0) AS total_quantity FROM snitch_db.maplemonk.s3_mnow_sales where ORDER_DATE is not null ), prod_details AS ( SELECT sku_group, product_name, sku_class, category, title, print_design, collar_new, material_new, fit, color, sleeve_type FROM snitch_db.maplemonk.metafields_data QUALIFY ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY status DESC) = 1 ) SELECT a.sku, a.sku_group, a.size, a.sp, a.mrp, a.discount, a.order_date, a.total_amount, a.total_quantity, NULL::DATE AS return_date, NULL::VARCHAR AS return_status, 0 AS return_quantity, NULL::VARCHAR AS return_reason, NULL::VARCHAR AS categorized_return_reason, c.product_name, c.sku_class, c.category, c.title, c.print_design, c.collar_new, c.material_new, c.fit, c.color, c.sleeve_type FROM sales a LEFT JOIN prod_details c ON a.sku_group = c.sku_group;",
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
            