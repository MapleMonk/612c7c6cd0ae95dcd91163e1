{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE issues_factory_sorted AS SELECT CAST(CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(CREATED_AT / 1000)) AS DATE) AS Created_At_date, CAST(CONVERT_TIMEZONE(\'America/Los_Angeles\', \'Asia/Kolkata\', TO_TIMESTAMP_LTZ(LAST_UPDATED_AT / 1000)) AS DATE) AS Updated_at_date, CASE WHEN LEFT(EVIDENCE, 6) = \'https:\' THEN EVIDENCE ELSE \'https:\' || EVIDENCE END AS EVIDENCE_WITH_HTTPS, t.* FROM factory_issues__factory t;",
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
            