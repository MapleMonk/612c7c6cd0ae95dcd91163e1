{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.store_priority_tier AS WITH latest_month AS ( SELECT branch_code, target_month, mtd_target, mtd_achieved FROM snitch_db.maplemonk.mtd_target_clean QUALIFY ROW_NUMBER() OVER (PARTITION BY branch_code ORDER BY target_month DESC) = 1 ), tiered AS ( SELECT *, NTILE(3) OVER (ORDER BY mtd_target DESC) AS target_tertile FROM latest_month WHERE mtd_target IS NOT NULL ) SELECT branch_code, target_month AS current_month, mtd_target AS current_month_target, mtd_achieved, CASE WHEN target_tertile = 1 THEN \'High Priority\' WHEN target_tertile = 2 THEN \'Medium Priority\' ELSE \'Low Priority\' END AS store_priority_tier FROM tiered;",
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
            