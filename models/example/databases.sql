{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.mtd_target_clean AS WITH cleaned AS ( SELECT TRIM(\"Branch \") AS branch_code, CASE WHEN TRIM(\"Date \") LIKE \'%/%/%\' THEN TRY_TO_DATE(TRIM(\"Date \"), \'DD/MM/YYYY\') WHEN TRIM(\"Date \") LIKE \'%-%-%\' THEN TRY_TO_DATE(TRIM(\"Date \"), \'DD-MM-YYYY\') ELSE TRY_TO_DATE(TRIM(\"Date \")) END AS target_date, TRY_TO_DECIMAL(REPLACE(REPLACE(TRIM(\"Target \"), \',\', \'\'), \'\"\', \'\'), 18, 2) AS daily_target, TRY_TO_DECIMAL(REPLACE(REPLACE(TRIM(\"MTD \"), \',\', \'\'), \'\"\', \'\'), 18, 2) AS cumulative_target_mtd FROM snitch_db.maplemonk.mtd_target WHERE \"Branch \" IS NOT NULL AND TRIM(\"Branch \") != \'\' AND TRY_TO_NUMBER(TRIM(\"Branch \")) IS NOT NULL AND \"Date \" IS NOT NULL AND TRIM(\"Date \") != \'\' ) SELECT branch_code, target_date, DATE_TRUNC(\'MONTH\', target_date) AS target_month, daily_target, cumulative_target_mtd FROM cleaned WHERE target_date IS NOT NULL QUALIFY ROW_NUMBER() OVER ( PARTITION BY branch_code, DATE_TRUNC(\'MONTH\', target_date) ORDER BY target_date DESC ) = 1;",
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
            