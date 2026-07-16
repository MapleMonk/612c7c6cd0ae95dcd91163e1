{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.merch_allocation_tagged AS SELECT a.order_date, a.branch_code, a.sku, a.sku_group, a.size_mapped, a.actual_qty, ms.category, ms.style, ms.meta1, ms.meta2, ms.meta3, COALESCE(r.REPLEN_UNITS, 0) AS recommended_qty, r.PARETO AS pareto, r.SKU_CLASS AS sku_class, r.PRIORITY AS store_priority, s2.final_ros, CASE WHEN COALESCE(r.PARETO, 100) <= 30 AND COALESCE(s2.final_ros, 0) >= 1.0 THEN 3 WHEN COALESCE(r.PARETO, 100) <= 60 OR COALESCE(s2.final_ros, 0) >= 0.6 THEN 2 ELSE 1 END AS demand_weight FROM snitch_db.maplemonk.merch_actual_allocation a LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(a.sku_group) = UPPER(ms.sku_group) LEFT JOIN snitch_db.maplemonk.store_replen_3_May2025 r ON a.sku = r.sku_code AND a.branch_code = r.branch_code LEFT JOIN snitch_db.maplemonk.store_replen_2_May2025 s2 ON a.sku = s2.logicusercode AND a.branch_code = s2.branch_code;",
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
            