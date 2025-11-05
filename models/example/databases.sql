{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table ga4_plus_size_data as SELECT *, TO_DATE(CAST(DATE AS STRING), \'YYYYMMDD\') AS DATE_ FROM snitch_db.maplemonk.plus_size_event_count;",
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
            