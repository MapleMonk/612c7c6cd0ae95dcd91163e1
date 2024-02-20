{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day as select * from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day union select * from snitch_db.maplemonk.unicommerce_inventory_aging",
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
                        