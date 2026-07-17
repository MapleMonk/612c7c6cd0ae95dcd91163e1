{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.replen_scorecard AS SELECT branch_code, order_date, ANY_VALUE(weighted_mix_deviation) AS weighted_mix_deviation, ANY_VALUE(mix_score) AS composite_score, COUNT_IF(warehouse_constrained_flag = \'Yes\') AS combos_warehouse_constrained, COUNT_IF(warehouse_data_missing = \'Yes\') AS combos_missing_warehouse_data, COUNT(*) AS total_combos FROM snitch_db.maplemonk.replen_mix_score GROUP BY branch_code, order_date ORDER BY branch_code, order_date;",
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
            