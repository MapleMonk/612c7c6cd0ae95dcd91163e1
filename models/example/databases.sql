{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE top_bottom_styles_dashboard AS WITH sales_45 AS ( SELECT sku_group, TYPE, SUM(gross_sales) AS total_sales_45, SUM(gross_quantity) AS total_quantity_45 FROM horizontal_sales_categories WHERE date >= DATEADD(day, -60, CURRENT_DATE) AND date < DATEADD(day, -15, CURRENT_DATE) GROUP BY sku_group, TYPE ), sales_15 AS ( SELECT sku_group, TYPE, SUM(gross_sales) AS total_sales_15, SUM(gross_quantity) AS total_quantity_15 FROM horizontal_sales_categories WHERE date >= DATEADD(day, -30, CURRENT_DATE) AND date < DATEADD(day, -15, CURRENT_DATE) GROUP BY sku_group, TYPE ), sales_30 AS ( SELECT sku_group, TYPE, SUM(gross_quantity) AS total_quantity_30 FROM horizontal_sales_categories WHERE date >= DATEADD(day, -30, CURRENT_DATE) AND date < CURRENT_DATE GROUP BY sku_group, TYPE ), category_map AS ( SELECT sku_group, MAX(category) AS category FROM category_overall_data GROUP BY sku_group ), l1_map AS ( SELECT sku_group, CASE WHEN LEFT(sku_group,4) = \'4MBG\' THEN \'Plus\' WHEN LOWER(style) LIKE \'%luxe%\' THEN \'Luxe\' ELSE \'Snitch\' END AS l1_category FROM base_product ) SELECT s45.sku_group, s45.TYPE, c.category, s45.total_sales_45, s45.total_quantity_45, s15.total_sales_15, s15.total_quantity_15, b.image_url, b.offline_inventory, b.online_inventory, b.days_since_live, l1.l1_category, s30.total_quantity_30, s30.total_quantity_30 * 1.0 / NULLIF( CASE WHEN s45.TYPE IN (\'Shopify\', \'Marketplace\') THEN b.online_inventory + s30.total_quantity_30 WHEN s45.TYPE = \'Store\' THEN b.offline_inventory + s30.total_quantity_30 END, 0) AS STR_30 FROM sales_45 s45 LEFT JOIN category_map c ON s45.sku_group = c.sku_group LEFT JOIN base_product b ON s45.sku_group = b.sku_group LEFT JOIN sales_30 s30 ON s45.sku_group = s30.sku_group AND s45.TYPE = s30.TYPE LEFT JOIN sales_15 s15 ON s45.sku_group = s15.sku_group AND s45.TYPE = s15.TYPE LEFT JOIN l1_map l1 ON s45.sku_group = l1.sku_group;",
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
            