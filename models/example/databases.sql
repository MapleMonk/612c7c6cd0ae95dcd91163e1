{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.snitch_inventory_aging_line_item_level as select * from ( select *,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',_airbyte_normalized_at :: DATETIME)::date as date ,dense_rank() over(partition by lower(facility),\"Item Type skuCode\",date order by _airbyte_normalized_at::date asc)rw1 from snitch_db.maplemonk.unicommerce_inventory_aging ) where rw1 = 1",
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
                        