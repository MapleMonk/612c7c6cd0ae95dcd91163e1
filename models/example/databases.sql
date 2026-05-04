{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `beastlife-wh-474411.maplemonk.gokwik_parsed_orders` AS WITH base AS ( SELECT payload, JSON_VALUE(payload, \'$.request_id\') AS order_id, JSON_VALUE(payload, \'$.token\') AS token, JSON_VALUE(payload, \'$.currency\') AS currency, TIMESTAMP(JSON_VALUE(payload, \'$.created_at\')) AS created_at, TIMESTAMP(JSON_VALUE(payload, \'$.updated_at\')) AS updated_at, JSON_VALUE(payload, \'$.customer.email\') AS customer_email, JSON_VALUE(payload, \'$.customer.phone\') AS customer_phone, JSON_VALUE(payload, \'$.customer.firstname\') AS first_name, JSON_VALUE(payload, \'$.customer.lastname\') AS last_name, JSON_VALUE(payload, \'$.address.city\') AS city, JSON_VALUE(payload, \'$.address.state\') AS state, JSON_VALUE(payload, \'$.address.country\') AS country, JSON_VALUE(payload, \'$.address.pincode\') AS pincode, SAFE_CAST(JSON_VALUE(payload, \'$.total_price\') AS FLOAT64) AS total_price, SAFE_CAST(JSON_VALUE(payload, \'$.item_count\') AS INT64) AS item_count, SAFE_CAST(JSON_VALUE(payload, \'$.cod_charges\') AS FLOAT64) AS cod_charges, JSON_VALUE(payload, \'$.is_abandoned\') AS is_abandoned, JSON_VALUE(payload, \'$.drop_stage\') AS drop_stage, JSON_VALUE(payload, \'$.rto_risk_flag\') AS rto_risk, JSON_VALUE(payload, \'$.store_platform\') AS platform, ARRAY_TO_STRING( ARRAY( SELECT JSON_VALUE(pm) FROM UNNEST(JSON_QUERY_ARRAY(payload, \'$.payment_methods\')) pm ), \', \' ) AS payment_methods FROM `beastlife-wh-474411.webhook_data_f.gokwik_abc` ) SELECT order_id, token, currency, created_at, updated_at, customer_email, customer_phone, first_name, last_name, city, state, country, pincode, total_price, item_count, cod_charges, is_abandoned, drop_stage, rto_risk, platform, payment_methods, STRING_AGG(JSON_VALUE(item, \'$.product_title\'), \' | \') AS product_names, STRING_AGG(JSON_VALUE(item, \'$.sku\'), \' | \') AS skus, STRING_AGG(CAST(JSON_VALUE(item, \'$.quantity\') AS STRING), \' | \') AS quantities, STRING_AGG( CAST(SAFE_CAST(JSON_VALUE(item, \'$.price\') AS FLOAT64)/100 AS STRING), \' | \' ) AS prices, ANY_VALUE(payload) AS payload FROM base, UNNEST(JSON_QUERY_ARRAY(payload, \'$.items\')) AS item GROUP BY order_id, token, currency, created_at, updated_at, customer_email, customer_phone, first_name, last_name, city, state, country, pincode, total_price, item_count, cod_charges, is_abandoned, drop_stage, rto_risk, platform, payment_methods;",
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
            