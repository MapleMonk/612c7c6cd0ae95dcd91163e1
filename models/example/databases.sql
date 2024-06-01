{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.snitch_final_inventory_wh2 as select * from ( select *,CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',_airbyte_normalized_at :: DATETIME)::date as date ,row_number() over( partition by \"Item SkuCode\",date order by _airbyte_normalized_at) rw from snitch_db.maplemonk.snitch_inventory_wh2 ) where rw=1",
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
                        