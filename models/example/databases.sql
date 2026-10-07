{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.clickstream_metabase as with flat as ( select a.request_date, a.source, f.value::string as shopify_product_id, a.user_type, a.source_widget, a.source_widget_id, a.product_click, a.product_impression, a.add_to_cart, a.initiate_payment, a.purchase from snitch_db.maplemonk.s3_clickstream a, lateral flatten(input => try_parse_json(a._ab_additional_properties::string)) f where f.key = \'shopify_product_id\' ) select a.request_date, dayname(a.request_date::date) as day, a.source, a.shopify_product_id, a.user_type, a.source_widget, a.source_widget_id, a.product_click::int as clicks, a.product_impression::int as impression, a.add_to_cart::int as add_to_cart, a.initiate_payment::int as payment, a.purchase::int as purchase, b.sku_group, b.category from flat a left join snitch_db.maplemonk.base_product b on a.shopify_product_id = b.id::string ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            