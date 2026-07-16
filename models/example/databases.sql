{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.replen_qty_score AS WITH row_level AS ( SELECT branch_code, sku, sku_group, size_mapped, actual_qty, recommended_qty, pareto, final_ros, demand_weight, ABS(actual_qty - recommended_qty) AS abs_deviation, CASE WHEN recommended_qty = 0 THEN NULL ELSE ABS(actual_qty - recommended_qty) / recommended_qty END AS qty_deviation_pct, CASE WHEN actual_qty > recommended_qty THEN \'Over-allocated\' WHEN actual_qty < recommended_qty THEN \'Under-allocated\' ELSE \'On target\' END AS direction FROM snitch_db.maplemonk.merch_allocation_tagged ), store_rollup AS ( SELECT branch_code, SUM(abs_deviation * demand_weight) / NULLIF(SUM(recommended_qty * demand_weight), 0) AS weighted_qty_deviation FROM row_level GROUP BY branch_code ) SELECT r.*, sr.weighted_qty_deviation, CASE WHEN sr.weighted_qty_deviation <= 0.10 THEN \'Perfect\' WHEN sr.weighted_qty_deviation <= 0.25 THEN \'Okay\' ELSE \'Poor\' END AS qty_score FROM row_level r LEFT JOIN store_rollup sr ON r.branch_code = sr.branch_code;",
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
            