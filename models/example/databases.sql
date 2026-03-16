{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE DATALAKE_DB.KAYA.PAGE_SPEED AS SELECT DATEADD(\'minute\', 330, PARSE_JSON(LIGHTHOUSERESULT):fetchTime::TIMESTAMP_NTZ) AS fetch_date, STRATEGY, ROUND(PARSE_JSON(LIGHTHOUSERESULT):audits.\"speed-index\".numericValue::FLOAT / 1000, 2) AS speed FROM DATALAKE_DB.KAYA.KS_PSI_PAGESPEED;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DATALAKE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            