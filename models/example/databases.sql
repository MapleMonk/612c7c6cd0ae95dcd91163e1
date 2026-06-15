{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE prd_db.justherbs.dwh_upsell_data AS WITH parsed_orders AS ( SELECT o.NAME, o.ORDER_NUMBER, o.CREATED_AT, o.EMAIL, o.TOTAL_PRICE, li.value AS line_item, prop.value AS property FROM prd_db.justherbs.dwh_Shopify_All_orders o, LATERAL FLATTEN(input => TRY_PARSE_JSON(o.LINE_ITEMS)) li, LATERAL FLATTEN(input => li.value:properties) prop ), filtered_orders AS ( SELECT NAME, ORDER_NUMBER, CREATED_AT, EMAIL, TOTAL_PRICE, line_item:id::STRING AS line_item_id, line_item:name::STRING AS product_name, line_item:sku::STRING AS sku, line_item:quantity::INT AS quantity, line_item:price::FLOAT AS unit_price, line_item:discount_allocations[0]:amount::FLOAT AS discount_amount, property:name::STRING AS property_name, property:value::STRING AS property_value FROM parsed_orders WHERE property:name::STRING IN (\'cart_upsell_button\', \'__pdp_usually_pair_button\', \'shop_the_look_button\', \'_checkout_upsell\', \'monsoon_store\') AND property:value::STRING = \'true\' ), final AS ( SELECT f.NAME, f.ORDER_NUMBER, f.CREATED_AT, f.EMAIL, f.TOTAL_PRICE, f.line_item_id, f.product_name, f.sku, f.quantity, f.unit_price, f.discount_amount, (f.unit_price * f.quantity) - COALESCE(f.discount_amount, 0) AS net_sales, f.property_name, f.property_value, CASE property_name WHEN \'cart_upsell_button\' THEN \'Cart Upsell Button\' WHEN \'__pdp_usually_pair_button\' THEN \'Usually Pair PDP\' WHEN \'shop_the_look_button\' THEN \'Shop the look\' WHEN \'_checkout_upsell\' THEN \'GoKwik Checkout Upsell\' WHEN \'monsoon_store\' THEN \'Monsoon Store\' END AS property_label, fi.FINAL_UTM_CHANNEL FROM filtered_orders f LEFT JOIN prd_db.justherbs.dwh_SHOPIFY_FACT_ITEMS fi ON f.NAME = fi.ORDER_NAME AND f.line_item_id = fi.LINE_ITEM_ID ) SELECT * FROM final ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            