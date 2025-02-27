{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.offline_footfall_delopt1 AS SELECT DATE_TRUNC(\'minute\', CASE WHEN COUNT__DATETIME LIKE \'__-__-____ __:__\' THEN TO_TIMESTAMP(COUNT__DATETIME, \'MM-DD-YYYY HH24:MI\') WHEN COUNT__DATETIME LIKE \'__/__/____ __:__\' THEN TO_TIMESTAMP(COUNT__DATETIME, \'MM/DD/YYYY HH24:MI\') WHEN COUNT__DATETIME LIKE \'_/__/____ __:__\' THEN TO_TIMESTAMP(COUNT__DATETIME, \'MM/DD/YYYY HH24:MI\') ELSE NULL END ) AS CUSTOM_DATE ,* from snitch_db.maplemonk.offline_footfall_delopt",
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
            