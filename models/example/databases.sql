{{ config(
            materialized='table',
                post_hook={
                    "sql": "select REVERSE(SUBSTRING(REVERSE(null), 1, POSITION(\'-\', REVERSE(null)) - 1)) AS size;",
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
            