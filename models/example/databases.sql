{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.products_to_add as ( with good_products as ( select sku_group from snitch_db.maplemonk.sku_group_ad_inventory_check where ga_date = dateadd(day,-1,current_date) and clicks >= 20 ) select * from snitch_db.maplemonk.availability_master_v2 where sku_group not in (select sku_group from good_products) )",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            