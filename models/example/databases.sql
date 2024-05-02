{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day as select * from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day union select * from snitch_db.maplemonk.unicommerce_inventory_aging; create or replace table snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day as select * from ( select *,dense_rank() over(partition by \"Item Type skuCode\",_airbyte_emitted_at::date order by _airbyte_emitted_at asc)rw from snitch_db.maplemonk.unicommerce_inventory_aging_day_on_day ) where rw=1;",
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
                        