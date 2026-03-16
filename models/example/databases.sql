{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE prd_db.justherbs.page_speed AS SELECT DATEADD(\'minute\', 330, PARSE_JSON(LIGHTHOUSERESULT):fetchTime::TIMESTAMP_NTZ) AS fetch_date, STRATEGY, ROUND(PARSE_JSON(LIGHTHOUSERESULT):audits.\"speed-index\".numericValue::FLOAT / 1000, 2) AS speed FROM DATALAKE_DB.JUSTHERBS.justherbs_psi__pagespeed;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PRD_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            