{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gs_review_escalations AS SELECT CASE WHEN DATE LIKE \'%/%/%\' THEN TO_TIMESTAMP(DATE, \'DD/MM/YYYY\') WHEN DATE LIKE \'%-%-%\' THEN TO_TIMESTAMP(DATE, \'DD-MM-YYYY\') ELSE NULL END AS \"DATE\", UPPER(TRIM(\"issue\")) AS \"ISSUE\", UPPER(TRIM(ACTION)) AS \"ACTION\", UPPER(TRIM(SOURCE)) AS \"SOURCE\", UPPER(TRIM(STATUS)) AS \"STATUS\", INITCAP(TRIM(AGENT_NAME)) AS \"AGENT_NAME\", TRIM(\"cx comment\") AS \"CX_COMMENT\" FROM snitch_db.maplemonk.gs__reviews_escalation;",
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
            