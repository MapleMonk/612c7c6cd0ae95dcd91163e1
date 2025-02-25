{{ config(
            materialized='table',
                post_hook={
                    "sql": "vamsi vamsi vamsi",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from GLADFUL_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            