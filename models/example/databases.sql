{{ config(
            materialized='table',
                post_hook={
                    "sql": "select * from maplemonk limit 100",
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
            