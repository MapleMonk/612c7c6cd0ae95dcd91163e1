{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.replen_mix_score AS WITH actual_mix AS ( SELECT branch_code, UPPER(category) AS category, COALESCE(UPPER(style), \'SNITCH\') AS style, COALESCE(UPPER(meta1), \'N/A\') AS meta1, COALESCE(UPPER(meta2), \'N/A\') AS meta2, COALESCE(UPPER(meta3), \'N/A\') AS meta3, SUM(actual_qty) AS actual_units FROM snitch_db.maplemonk.merch_allocation_tagged WHERE category IS NOT NULL GROUP BY 1,2,3,4,5,6 ), actual_mix_pct AS ( SELECT *, actual_units / NULLIF(SUM(actual_units) OVER (PARTITION BY branch_code), 0) AS actual_mix_pct FROM actual_mix ), ideal_latest AS ( SELECT * FROM snitch_db.maplemonk.OFFLINE_METAFIELD_IDEAL_MIX_V2_DAILY QUALIFY ROW_NUMBER() OVER ( PARTITION BY branch_code, category, style, meta_1, meta_2, meta_3 ORDER BY snapshot_date DESC ) = 1 ), compared AS ( SELECT a.branch_code, a.category, a.style, a.meta1, a.meta2, a.meta3, a.actual_units, a.actual_mix_pct, COALESCE(i.metafield_ideal_mix, 0) AS ideal_mix_pct, ABS(a.actual_mix_pct - COALESCE(i.metafield_ideal_mix, 0)) AS mix_deviation FROM actual_mix_pct a LEFT JOIN ideal_latest i ON a.branch_code = i.branch_code AND a.category = i.category AND a.style = i.style AND a.meta1 = i.meta_1 AND a.meta2 = i.meta_2 AND a.meta3 = i.meta_3 ), store_rollup AS ( SELECT branch_code, SUM(mix_deviation * ideal_mix_pct) / NULLIF(SUM(ideal_mix_pct), 0) AS weighted_mix_deviation FROM compared GROUP BY branch_code ) SELECT c.*, sr.weighted_mix_deviation, CASE WHEN sr.weighted_mix_deviation <= 0.05 THEN \'Perfect\' WHEN sr.weighted_mix_deviation <= 0.15 THEN \'Okay\' ELSE \'Poor\' END AS mix_score FROM compared c LEFT JOIN store_rollup sr ON c.branch_code = sr.branch_code;",
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
            