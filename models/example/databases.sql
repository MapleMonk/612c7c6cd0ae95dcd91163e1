{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.store_meta_master_setup AS WITH store_zone AS ( SELECT s.branch_code::VARCHAR AS branch_code, s.branch_name::VARCHAR AS store_name, UPPER(s.zone) AS zone, m.cluster::VARCHAR AS cluster_name, ROW_NUMBER() OVER (PARTITION BY s.branch_code::VARCHAR ORDER BY UPPER(s.zone)) AS rn FROM snitch_db.maplemonk.store_category_delta s LEFT JOIN snitch_db.maplemonk.offline_master m ON s.branch_code::VARCHAR = m.branch_code::VARCHAR WHERE UPPER(s.zone) IN (\'NORTH\', \'SOUTH\') ), latest_month_target AS ( SELECT TRIM(\"Branch \")::VARCHAR AS branch_code, DATE_TRUNC(\'MONTH\', TRY_TO_DATE(TRIM(\"Date \"))) AS target_month, TRY_TO_DECIMAL(REPLACE(REPLACE(TRIM(\"MTD \"), \',\', \'\'), \'\"\', \'\'), 18, 2) AS cumulative_target_mtd FROM snitch_db.maplemonk.mtd_target WHERE \"Branch \" IS NOT NULL AND TRIM(\"Branch \") != \'\' QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(\"Branch \") ORDER BY TRY_TO_DATE(TRIM(\"Date \")) DESC) = 1 ), tiered_stores AS ( SELECT branch_code, target_month, cumulative_target_mtd, NTILE(3) OVER (ORDER BY cumulative_target_mtd DESC) AS target_tertile FROM latest_month_target ) SELECT sz.branch_code, sz.store_name, sz.zone, sz.cluster_name, ts.cumulative_target_mtd AS current_month_target, CASE WHEN ts.target_tertile = 1 THEN \'High Priority\' WHEN ts.target_tertile = 2 THEN \'Medium Priority\' ELSE \'Low Priority\' END AS store_priority_tier FROM store_zone sz LEFT JOIN tiered_stores ts ON sz.branch_code = ts.branch_code WHERE sz.rn = 1;",
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
            