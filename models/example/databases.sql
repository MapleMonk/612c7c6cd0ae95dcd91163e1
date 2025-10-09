{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE issues_factory_sorted AS SELECT *,CASE WHEN LEFT(EVIDENCE, 6) = \'https:\' THEN EVIDENCE ELSE \'https:\' || EVIDENCE END AS EVIDENCE_WITH_HTTPS, FROM issues_factory_sorted;",
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
            