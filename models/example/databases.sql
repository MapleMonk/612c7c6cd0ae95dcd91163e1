{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.prolicious_gokwik_abandoned_carts_fact_items as with split_products as ( select id as cart_id, cart_date, customer_name, phone_number, email_id, cart_amount as cart_value, cart_discount, cart_abandonment_stage, source, utm_campaign, utm_medium, dsicount_code, cart_mrp_value, regexp_replace(line_items, r\'\*[0-9]+(?:\.[0-9]+)?(?: \| |$)\', \'|||\') as cleaned_line from maplemonk.prolicious_gokwik_abandoned_carts_fact_items_intermediate where line_items is not null ), extracted_products as ( select cart_id, cart_date, customer_name, phone_number, email_id, cart_value, cart_discount, cart_abandonment_stage, source, utm_campaign, utm_medium, dsicount_code, cart_mrp_value, if(regexp_contains(trim(item), r\'^#[^(]+\((.+)\)$\'), regexp_extract(trim(item), r\'^#[^(]+\((.+)\)$\'), trim(item)) as product from split_products, unnest(split(cleaned_line, \'|||\')) as item where trim(item) != \'\' ), distinct_cart_products as ( select distinct cart_id, cart_date, customer_name, phone_number, email_id, cart_value, cart_discount, cart_abandonment_stage, source, utm_campaign, utm_medium, dsicount_code, cart_mrp_value, product from extracted_products ) select cart_id, cart_date, customer_name, phone_number, email_id, cart_value, cart_discount, cart_abandonment_stage, source, utm_campaign, utm_medium, dsicount_code, cart_mrp_value, array_to_string(array_agg(product order by product), \' + \') as product_combination, count(product) as products_in_combo from distinct_cart_products group by 1,2,3,4,5,6,7,8,9,10,11,12,13",
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
            