{{ config(
            materialized='table',
                post_hook={
                    "sql": "select * from MAPLEMONK_DEV.MAPLEMONK.RETURNS;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MAPLEMONK_DEV.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            