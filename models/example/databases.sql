{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `neshanka-wh.maplemonk.shopify_sales_dataset` AS SELECT p.id as product_id, p.title as product_name, pv.sku, p.product_type as category, p.vendor as sub_category, DATE(o.created_at) as order_date, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as quantity, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) as price, CAST(JSON_EXTRACT_SCALAR(line_item, \'$.price\') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(line_item, \'$.quantity\') AS INT64) as line_value, o.id as order_id, o.order_number, o.total_price, o.financial_status, o.fulfillment_status, CURRENT_TIMESTAMP() as _load_timestamp FROM `neshanka-wh.maplemonk.Shopify_apartment18_orders` o CROSS JOIN UNNEST(o.line_items) as line_item LEFT JOIN `neshanka-wh.maplemonk.Shopify_apartment18_products` p ON CAST(JSON_EXTRACT_SCALAR(line_item, \'$.product_id\') AS INT64) = p.id LEFT JOIN `neshanka-wh.maplemonk.Shopify_apartment18_products_variants` pv ON CAST(JSON_EXTRACT_SCALAR(line_item, \'$.variant_id\') AS INT64) = pv.id WHERE o.created_at IS NOT NULL;",
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
            