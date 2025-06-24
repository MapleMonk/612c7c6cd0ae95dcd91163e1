{{ config(
            materialized='table',
                post_hook={
                    "sql": "selct * from",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from BUMMER_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            