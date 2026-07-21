{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_scorecard AS WITH latest_allocation AS ( SELECT branch_code, MAX(order_date) AS latest_order_date FROM snitch_db.maplemonk.fresh_mix_score GROUP BY branch_code ), base AS ( SELECT f.* FROM snitch_db.maplemonk.fresh_mix_score f INNER JOIN latest_allocation la ON f.branch_code = la.branch_code AND f.order_date = la.latest_order_date ), rolled AS ( SELECT branch_code, order_date, MAX(day_total_options) AS total_actual_options, MAX(day_total_combos) AS total_combos, MAX(weighted_mix_deviation) AS weighted_mix_deviation, MAX(mix_score) AS composite_score FROM base GROUP BY branch_code, order_date ), with_zone_priority AS ( SELECT r.*, z.zone, pt.store_priority_tier, pt.current_month_target FROM rolled r LEFT JOIN snitch_db.maplemonk.store_zone_map z ON r.branch_code = z.branch_code LEFT JOIN snitch_db.maplemonk.store_priority_tier pt ON r.branch_code = pt.branch_code ), top_offender AS ( SELECT branch_code, order_date, category AS top_offending_category, mix_deviation AS top_offending_deviation, achievable_target_pct AS target_allocation_pct, alloc_mix_pct AS actual_allocation_pct FROM ( SELECT branch_code, order_date, category, mix_deviation, achievable_target_pct, alloc_mix_pct, ROW_NUMBER() OVER (PARTITION BY branch_code, order_date ORDER BY mix_deviation DESC) AS rn FROM base ) WHERE rn = 1 ), final AS ( SELECT w.*, mf.\"STORE NAME\" AS branch_name, t.top_offending_category, t.top_offending_deviation, t.target_allocation_pct, t.actual_allocation_pct FROM with_zone_priority w LEFT JOIN snitch_db.maplemonk.master_file mf ON w.branch_code = mf.\"STORE CODE\"::VARCHAR LEFT JOIN top_offender t ON w.branch_code = t.branch_code AND w.order_date = t.order_date ) SELECT branch_code, branch_name, zone, order_date, total_actual_options, total_combos, target_allocation_pct, actual_allocation_pct, weighted_mix_deviation, composite_score, store_priority_tier, current_month_target, CASE WHEN composite_score NOT IN (\'Perfect\', \'Okay\', \'Poor\') THEN composite_score WHEN composite_score = \'Poor\' AND store_priority_tier = \'High Priority\' THEN \'Critical\' WHEN composite_score = \'Poor\' AND store_priority_tier = \'Medium Priority\' THEN \'Concern\' WHEN composite_score = \'Poor\' AND store_priority_tier = \'Low Priority\' THEN \'Low Concern\' WHEN composite_score = \'Okay\' AND store_priority_tier = \'High Priority\' THEN \'Watch\' ELSE composite_score END AS severity_adjusted_score, top_offending_category, top_offending_deviation, RANK() OVER ( PARTITION BY store_priority_tier ORDER BY CASE WHEN composite_score IN (\'Perfect\',\'Okay\',\'Poor\') THEN weighted_mix_deviation END ASC NULLS LAST ) AS store_rank_within_tier FROM final ORDER BY branch_code;",
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
            