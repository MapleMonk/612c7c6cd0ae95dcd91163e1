{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE issues_factory_sorted AS SELECT CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(CREATED_AT / 1000)) AS Created_At_date, CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(LAST_UPDATED_AT / 1000)) AS Updated_at_date, CASE WHEN LEFT(EVIDENCE, 6) = \'https:\' THEN EVIDENCE ELSE \'https:\' || EVIDENCE END AS EVIDENCE_WITH_HTTPS, t.* FROM issues_factory t;",
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
            