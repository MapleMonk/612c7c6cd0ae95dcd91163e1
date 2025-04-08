{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ras_luxury_marketing_consolidated as select * from `MAPLEMONK.moody_store_marketing_consolidated` union all select * from `MAPLEMONK.ras_dot_in_marketing_consolidated` union all select * from `MAPLEMONK.ras_marketing_consolidated` union all select * from `MAPLEMONK.ras_mini_marketing_consolidated`;",
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
            