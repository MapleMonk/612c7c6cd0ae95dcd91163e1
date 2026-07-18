{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.mtd_target_clean AS SELECT \"Branch \"::VARCHAR AS branch_code, TO_DATE(\"Date \", \'DD/MM/YYYY\') AS target_date, DATE_TRUNC(\'MONTH\', TO_DATE(\"Date \", \'DD/MM/YYYY\')) AS target_month, TRY_TO_DECIMAL(REPLACE(REPLACE(\"Target \", \',\', \'\'), \'\"\', \'\'), 18, 2) AS mtd_target, TRY_TO_DECIMAL(REPLACE(REPLACE(\"MTD \", \',\', \'\'), \'\"\', \'\'), 18, 2) AS mtd_achieved FROM snitch_db.maplemonk.MTD_TARGET QUALIFY ROW_NUMBER() OVER ( PARTITION BY \"Branch \", DATE_TRUNC(\'MONTH\', TO_DATE(\"Date \", \'DD/MM/YYYY\')) ORDER BY TO_DATE(\"Date \", \'DD/MM/YYYY\') DESC ) = 1;",
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
            