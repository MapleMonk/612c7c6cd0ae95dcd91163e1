{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create Or Replace Table MapleMonk.iambydollyjain_Shopaccino_Fact_items as select customer_id, order_no, PARSE_TIMESTAMP(\'%d %b %Y %I:%M %p\', order_date) AS order_timestamp, cast(PARSE_TIMESTAMP(\'%d %b %Y %I:%M %p\', order_date) as DATE) AS order_date, currency_code, discount_code, payment_method as payment_method_Code, payment_status as payment_status_Code, cancelled_date cancelled_date, delivery_method as delivery_method_Code, delivery_status as delivery_status_Code, store_address_id, order_status_name, payment_status_name, 0 as IS_REFUND, shipping_cost, COALESCE(JSON_VALUE(billing_information, \'$.billing_city\'), JSON_VALUE(shipping_information, \'$.shipping_city\')) AS city, COALESCE(JSON_VALUE(billing_information, \'$.billing_phone\'), JSON_VALUE(shipping_information, \'$.shipping_phone\')) AS phone, COALESCE(JSON_VALUE(billing_information, \'$.billing_postal_code\'), JSON_VALUE(shipping_information, \'$.shipping_postal_code\')) AS postal_code, COALESCE(JSON_VALUE(billing_information, \'$.billing_state\'), JSON_VALUE(shipping_information, \'$.shipping_state\')) AS state, COALESCE(JSON_VALUE(billing_information, \'$.billing_email\'), JSON_VALUE(shipping_information, \'$.shipping_email\')) AS email, COALESCE(JSON_VALUE(billing_information, \'$.billing_gstin\'), JSON_VALUE(shipping_information, \'$.shipping_gstin\')) AS gstin, COALESCE(JSON_VALUE(billing_information, \'$.billing_country\'), JSON_VALUE(shipping_information, \'$.shipping_country\')) AS country, COALESCE(JSON_VALUE(billing_information, \'$.billing_address\'), JSON_VALUE(shipping_information, \'$.shipping_address\')) AS address, COALESCE(customer_name,JSON_VALUE(billing_information, \'$.billing_name\'), JSON_VALUE(shipping_information, \'$.shipping_name\')) AS customer_name, cast(JSON_EXTRACT_SCALAR(B, \'$.maximum_retail_price\') as float64)as MRP, cast(JSON_EXTRACT_SCALAR(B, \'$.product_price\') as float64) as product_price, JSON_EXTRACT_SCALAR(B, \'$.product_id\') as product_id, JSON_EXTRACT_SCALAR(B, \'$.sku\') as sku, cast(JSON_EXTRACT_SCALAR(B, \'$.tax\') as float64) as tax, JSON_EXTRACT_SCALAR(B, \'$.product_name\') as product_name, cast(JSON_EXTRACT_SCALAR(B, \'$.order_quantity\') as float64)as order_quantity, JSON_EXTRACT_SCALAR(B, \'$.product_variant_id\') as product_variant_id from maplemonk.test_v_get_orders LEFT JOIN UNNEST(order_products) B ;",
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
            