{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.mtd_target_clean AS WITH cleaned AS ( SELECT TRIM(\"Branch \") AS branch_code, COALESCE( TRY_TO_DATE(TRIM(\"Date \"), \'DD/MM/YYYY\'), TRY_TO_DATE(TRIM(\"Date \"), \'DD-MM-YYYY\'), TRY_TO_DATE(TRIM(\"Date \"), \'YYYY-MM-DD\'), TRY_TO_DATE(TRIM(\"Date \")) ) AS target_date, TRY_TO_DECIMAL(REPLACE(REPLACE(TRIM(\"Target \"), \',\', \'\'), \'\"\', \'\'), 18, 2) AS mtd_target, TRY_TO_DECIMAL(REPLACE(REPLACE(TRIM(\"MTD \"), \',\', \'\'), \'\"\', \'\'), 18, 2) AS mtd_achieved FROM snitch_db.maplemonk.MTD_TARGET WHERE \"Branch \" IS NOT NULL AND TRIM(\"Branch \") != \'\' AND \"Date \" IS NOT NULL AND TRIM(\"Date \") != \'\' ) SELECT branch_code, target_date, DATE_TRUNC(\'MONTH\', target_date) AS target_month, mtd_target, mtd_achieved FROM cleaned WHERE target_date IS NOT NULL QUALIFY ROW_NUMBER() OVER ( PARTITION BY branch_code, DATE_TRUNC(\'MONTH\', target_date) ORDER BY target_date DESC ) = 1;",
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
            