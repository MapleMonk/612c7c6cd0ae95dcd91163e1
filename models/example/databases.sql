{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.return_reason_summary AS SELECT COALESCE(NULLIF(TRIM(REASON), \'\'), \'Other\') AS RETURN_REASON, COUNT(*) AS RETURN_INSTANCES, SUM(COALESCE(RETURN_QTY_REAL, 0)) AS RETURN_QTY, ROUND( 100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2 ) AS RETURN_SHARE_PCT FROM snitch_db.maplemonk.return_fact_items WHERE ( SKU_GROUP LIKE \'4MBG%\' OR SKU_GROUP LIKE \'4B%\' ) AND LOWER(TRIM(REASON)) <> \'not_returned\' GROUP BY 1 ORDER BY RETURN_INSTANCES DESC;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            