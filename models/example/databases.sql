{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create OR REPLACE issues_factory_sorted AS select CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(CREATED_AT / 1000) ) AS Created_At_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(LAST_UPDATED_AT / 1000) ) AS Updated_at_date, * from issues_factory",
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
            