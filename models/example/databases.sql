{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_inventory_fact_items` AS SELECT p.id as product_id, p.title as product_name, JSON_EXTRACT_SCALAR(line_item, \'$.sku\') AS SKU, p.product_type as category, p.vendor as sub_category, DATE(o.created_at) as order_date, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as quantity, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) as price, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as line_value, o.id as order_id, o.order_number, o.total_price, o.financial_status, o.fulfillment_status, inv.Available_Inventory as WH_inv, ROUND(inv.Available_Inventory / NULLIF(inv.Sold_Quantity_30_Days / 30, 0), 2) as DOC_45D, CURRENT_TIMESTAMP() as _load_timestamp FROM `neshanka-wh.maplemonk.Shopify_All_orders` o LEFT JOIN UNNEST(LINE_ITEMS) AS line_item LEFT JOIN `neshanka-wh.maplemonk.Shopify_all_products` p ON CAST(JSON_EXTRACT_SCALAR(line_item, \'$.product_id\') AS INT64) = p.id LEFT JOIN `neshanka-wh.maplemonk.neshanka_INVENTORY_FACT_ITEMS` inv ON LOWER(JSON_EXTRACT_SCALAR(line_item, \'$.sku\')) = LOWER(REPLACE(inv.sku, \' \', \'\')) WHERE o.created_at IS NOT NULL and DATE(o.created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);",
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
            