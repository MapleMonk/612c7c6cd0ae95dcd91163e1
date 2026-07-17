{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_mix_score AS WITH alloc_mix AS ( SELECT branch_code, order_date, UPPER(category) AS category, COALESCE(UPPER(style), \'SNITCH\') AS style, COALESCE(UPPER(meta1), \'N/A\') AS meta1, COALESCE(UPPER(meta2), \'N/A\') AS meta2, COALESCE(UPPER(meta3), \'N/A\') AS meta3, SUM(actual_qty) AS actual_units FROM snitch_db.maplemonk.fresh_allocation_tagged WHERE category IS NOT NULL GROUP BY 1,2,3,4,5,6,7 ), alloc_mix_pct AS ( SELECT *, actual_units / NULLIF(SUM(actual_units) OVER (PARTITION BY branch_code, order_date), 0) AS alloc_mix_pct FROM alloc_mix ), compared AS ( SELECT a.branch_code, a.order_date, a.category, a.style, a.meta1, a.meta2, a.meta3, a.actual_units, a.alloc_mix_pct, COALESCE(s.sales_mix_blended_pct, 0) AS sales_mix_target_pct, COALESCE(w.warehouse_mix_pct, 0) AS warehouse_mix_pct, LEAST(COALESCE(s.sales_mix_blended_pct, 0), COALESCE(w.warehouse_mix_pct, 0)) AS achievable_target_pct, CASE WHEN COALESCE(w.warehouse_mix_pct, 0) < COALESCE(s.sales_mix_blended_pct, 0) THEN \'Yes\' ELSE \'No\' END AS warehouse_constrained_flag, CASE WHEN w.warehouse_mix_pct IS NULL THEN \'Yes\' ELSE \'No\' END AS warehouse_data_missing FROM alloc_mix_pct a LEFT JOIN snitch_db.maplemonk.fresh_sales_mix_reference s ON a.branch_code = s.branch_code AND a.order_date = s.as_of_date AND a.category = s.category AND a.style = s.style AND a.meta1 = s.meta1 AND a.meta2 = s.meta2 AND a.meta3 = s.meta3 LEFT JOIN snitch_db.maplemonk.warehouse_availability_mix w ON a.category = w.category AND a.style = w.style AND a.meta1 = w.meta1 AND a.meta2 = w.meta2 AND a.meta3 = w.meta3 ), scored AS ( SELECT *, ABS(alloc_mix_pct - achievable_target_pct) AS mix_deviation FROM compared ), store_rollup AS ( SELECT branch_code, order_date, SUM(mix_deviation * achievable_target_pct) / NULLIF(SUM(achievable_target_pct), 0) AS weighted_mix_deviation FROM scored GROUP BY branch_code, order_date ) SELECT sc.*, sr.weighted_mix_deviation, CASE WHEN sr.weighted_mix_deviation IS NULL THEN \'No Data\' WHEN sr.weighted_mix_deviation <= 0.05 THEN \'Perfect\' WHEN sr.weighted_mix_deviation <= 0.15 THEN \'Okay\' ELSE \'Poor\' END AS mix_score FROM scored sc LEFT JOIN store_rollup sr ON sc.branch_code = sr.branch_code AND sc.order_date = sr.order_date;",
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
            