{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_availability_mix AS WITH wh_sku AS ( SELECT CASE WHEN UPPER(sku) LIKE \'4C%\' THEN SPLIT_PART(sku, \'-\', 1) || \'-\' || SPLIT_PART(sku, \'-\', 2) || \'-\' || SPLIT_PART(sku, \'-\', 3) WHEN POSITION(\'-\' IN sku) > 0 THEN SPLIT_PART(sku, \'-\', 1) || \'-\' || SPLIT_PART(sku, \'-\', 2) ELSE sku END AS sku_group, SUM(units_on_hand) AS units_on_hand FROM snitch_db.maplemonk.LIVE_INV_WAREHOUSE_offline_replen GROUP BY 1 ), wh_tagged AS ( SELECT ms.category, COALESCE(UPPER(ms.style), \'SNITCH\') AS style, COALESCE(UPPER(ms.meta1), \'N/A\') AS meta1, COALESCE(UPPER(ms.meta2), \'N/A\') AS meta2, COALESCE(UPPER(ms.meta3), \'N/A\') AS meta3, SUM(w.units_on_hand) AS wh_units FROM wh_sku w LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(w.sku_group) = UPPER(ms.sku_group) WHERE ms.category IS NOT NULL GROUP BY 1,2,3,4,5 ) SELECT category, style, meta1, meta2, meta3, wh_units, wh_units / NULLIF(SUM(wh_units) OVER (), 0) AS warehouse_mix_pct FROM wh_tagged;",
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
            