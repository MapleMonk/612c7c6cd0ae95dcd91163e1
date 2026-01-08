{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items_intermediate` AS WITH sales_45_day AS ( SELECT JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, SUM(CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64)) as Sold_Quantity_45_Days FROM `neshanka-wh.maplemonk.Shopify_All_orders` o LEFT JOIN UNNEST(LINE_ITEMS) AS line_item WHERE DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) AND o.created_at IS NOT NULL GROUP BY SKU ) SELECT p.id as product_id, p.title as product_name, JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, p.product_type as category, p.vendor as sub_category, DATE(o.created_at) as order_date, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as quantity, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) as price, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as line_value, o.id as order_id, o.order_number, o.total_price, o.financial_status, o.fulfillment_status, inv.Available_Inventory as WH_inv, sales_45.Sold_Quantity_45_Days, ROUND(inv.Available_Inventory / NULLIF(sales_45.Sold_Quantity_45_Days / 45, 0), 2) as DOC_45D, CURRENT_TIMESTAMP() as _load_timestamp FROM `neshanka-wh.maplemonk.Shopify_All_orders` o LEFT JOIN UNNEST(LINE_ITEMS) AS line_item LEFT JOIN `neshanka-wh.maplemonk.Shopify_all_products` p ON CAST(JSON_EXTRACT_SCALAR(line_item, \'$.product_id\') AS INT64) = p.id LEFT JOIN `neshanka-wh.maplemonk.neshanka_INVENTORY_FACT_ITEMS` inv ON LOWER(JSON_EXTRACT_SCALAR(line_item, \'$.sku\')) = LOWER(REPLACE(inv.sku, \' \', \'\')) LEFT JOIN sales_45_day sales_45 ON LOWER(JSON_EXTRACT_SCALAR(line_item, \'$.sku\')) = LOWER(sales_45.SKU) WHERE o.created_at IS NOT NULL AND DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY); CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items` AS SELECT SKU, product_name, category, sub_category, DATE(order_date) as order_date, SUM(quantity) as total_quantity, SUM(line_value) as total_line_value, MAX(WH_inv) as WH_inv, MAX(DOC_45D) as DOC_45D, COUNT(DISTINCT order_id) as order_count FROM `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items_intermediate` WHERE order_date IS NOT NULL GROUP BY SKU, product_name, category, sub_category, DATE(order_date);",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            