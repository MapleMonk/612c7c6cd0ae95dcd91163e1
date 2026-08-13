{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table goodtribe-wh.maplemonk.gift_Card_consolidated as select a.*, b.phone, b.email from goodtribe-wh.maplemonk.shopify_gift_Cards a left join (select id, phone, email from goodtribe-wh.maplemonk.Shopify_All_customers) b on cast(a.customer_id as string) = cast(b.id as string) ;",
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
            