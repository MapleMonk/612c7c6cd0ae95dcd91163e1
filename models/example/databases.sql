{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE shoot_type_monthly_summary AS WITH base AS ( SELECT TRY_TO_TIMESTAMP( NULLIF(NULLIF(TRIM(shoot_date), \'\'), \'null\'), \'DD-MM-YYYY HH24:MI:SS\' ) AS parsed_shoot_date, CASE WHEN TRIM(shoot_type) = \'\' THEN NULL WHEN LOWER(TRIM(shoot_type)) = \'null\' THEN NULL ELSE shoot_type END AS clean_shoot_type FROM pslj_final_new_pslj_table ) SELECT TO_DATE(parsed_shoot_date) AS shoot_date, TO_VARCHAR(parsed_shoot_date, \'YYYY-MM\') AS shoot_month, COUNT(*) AS total_records, COUNT(clean_shoot_type) AS shoot_type_filled, ROUND( 100.0 * COUNT(clean_shoot_type) / NULLIF(COUNT(*), 0), 2 ) AS shoot_type_percentage FROM base WHERE parsed_shoot_date IS NOT NULL AND parsed_shoot_date >= \'2026-01-01\' GROUP BY 1, 2 ORDER BY 1;",
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
            