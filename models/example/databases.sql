{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items` AS WITH sales_45_day AS ( SELECT JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, SUM(CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64)) as Sold_Quantity_45_Days FROM `neshanka-wh.maplemonk.Shopify_All_orders` o LEFT JOIN UNNEST(LINE_ITEMS) AS line_item WHERE DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) AND o.created_at IS NOT NULL GROUP BY JSON_EXTRACT_SCALAR(line_item, \'$.sku\') ), sales_agg AS ( SELECT JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, p.title as product_name, p.product_type as product_type, p.vendor as vendor, DATE(o.created_at) as order_date, SUM(CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64)) as total_quantity, SUM(CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64)) as total_line_value FROM `neshanka-wh.maplemonk.Shopify_All_orders` o LEFT JOIN UNNEST(LINE_ITEMS) AS line_item LEFT JOIN `neshanka-wh.maplemonk.Shopify_all_products` p ON CAST(JSON_EXTRACT_SCALAR(line_item, \'$.product_id\') AS INT64) = p.id WHERE o.created_at IS NOT NULL AND DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) GROUP BY SKU, p.title, p.product_type, p.vendor, DATE(o.created_at) ) SELECT sa.SKU, sa.product_name, sa.product_type, sa.vendor, sa.order_date, sa.total_quantity, sa.total_line_value, inv.Available_Inventory as WH_inv, sales_45.Sold_Quantity_45_Days, ROUND(inv.Available_Inventory / NULLIF(sales_45.Sold_Quantity_45_Days / 45, 0), 2) as DOC_45D, CURRENT_TIMESTAMP() as _load_timestamp FROM sales_agg sa LEFT JOIN ( select distinct sku, Available_Inventory, data_fetch_date from `neshanka-wh.maplemonk.neshanka_INVENTORY_FACT_ITEMS` ) inv ON LOWER(sa.SKU) = LOWER(REPLACE(inv.sku, \' \', \'\')) and order_date = data_fetch_date LEFT JOIN sales_45_day sales_45 ON LOWER(sa.SKU) = LOWER(sales_45.SKU);",
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
            