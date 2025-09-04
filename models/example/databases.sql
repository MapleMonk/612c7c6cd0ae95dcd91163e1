{{ config(
            materialized='table',
                post_hook={
                    "sql": "select 33040848 / 9856 ;",
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
            