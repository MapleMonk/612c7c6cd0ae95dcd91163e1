{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.unicommerce_availability_merge as select * from snitch_db.maplemonk.unicommerce_fact_items_snitch fa left join snitch_db.maplemonk.availability_master am on fa.sku_group = am.sku_group",
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
                        