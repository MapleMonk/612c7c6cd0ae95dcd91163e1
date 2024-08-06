{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.bucket_class_category_clicks_impressions AS WITH class_category_data AS ( SELECT order_date, sku_group, sku_class, category, SUM(suborder_quantity) as gross_sales FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE order_date >= \'2024-03-01\' GROUP BY 1, 2, 3, 4 ), impressions_click_data AS ( SELECT DATE, sku_group, SUM(impressions) AS impressions, SUM(clicks) AS clicks FROM snitch_db.maplemonk.final_ga_clicks_impressions_by_itemid WHERE DATE >= \'2024-03-01\' GROUP BY 1, 2 ) SELECT cnc.order_date, cnc.sku_group, cnc.sku_class, cnc.category, ic.impressions, ic.clicks, cnc.gross_sales FROM class_category_data cnc LEFT JOIN impressions_click_data ic ON cnc.sku_group = ic.sku_group AND cnc.order_date = ic.DATE; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.class_cat_sales_inventory as WITH inventory AS ( SELECT _airbyte_emitted_at::DATE AS date, REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(\"Item Type skuCode\")) + 1)) AS sku_group, COUNT(DISTINCT \"Item Code\") AS Inventory FROM snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day GROUP BY 1, 2 ), class_category_data AS ( SELECT sku_group, sku_class, category FROM snitch_db.maplemonk.availability_master ), sales_data AS ( SELECT order_timestamp::DATE AS date, sku_group, SUM(quantity) AS Sold_Qty FROM snitch_db.maplemonk.fact_items_snitch GROUP BY 1, 2 ) SELECT i.date, i.sku_group, c.sku_class, c.category, i.Inventory, COALESCE(s.Sold_qty, 0) AS Sold_qty FROM inventory i LEFT JOIN class_category_data c ON i.sku_group = c.sku_group LEFT JOIN sales_data s ON i.date = s.date AND i.sku_group = s.sku_group; create or replace table snitch_db.maplemonk.linen_product_count_dod as ( select date(order_timestamp::date) as date, sum(gross_sales)as gross_sales, count(order_name) as order_count from snitch_db.maplemonk.fact_items_snitch where lower(product_tags) like \'%linen shirt%\' or lower(product_name) like \'%linen shirt%\' group by 1 )",
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
            