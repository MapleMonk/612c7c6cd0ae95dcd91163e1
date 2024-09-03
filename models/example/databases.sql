{{ config(
            materialized='table',
                post_hook={
                    "sql": "select concat(\'vamsi\',\'A\',null) as stringq",
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
            