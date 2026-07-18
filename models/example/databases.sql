{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_availability_mix AS WITH wh_sku AS ( SELECT facility, CASE WHEN UPPER(sku) LIKE \'4C%\' THEN SPLIT_PART(sku, \'-\', 1) || \'-\' || SPLIT_PART(sku, \'-\', 2) || \'-\' || SPLIT_PART(sku, \'-\', 3) WHEN POSITION(\'-\' IN sku) > 0 THEN SPLIT_PART(sku, \'-\', 1) || \'-\' || SPLIT_PART(sku, \'-\', 2) ELSE sku END AS sku_group, units_on_hand FROM snitch_db.maplemonk.LIVE_INV_WAREHOUSE_offline_replen ), wh_zoned AS ( SELECT sku_group, CASE WHEN facility = \'SAPL-NORTH-TAURU\' THEN \'NORTH\' WHEN facility IN (\'SAPL-WH1\', \'SAPL-WH2\') THEN \'SOUTH\' ELSE \'OTHER\' END AS wh_zone, units_on_hand FROM wh_sku ), wh_tagged AS ( SELECT w.wh_zone, ms.category, COALESCE(UPPER(ms.style), \'SNITCH\') AS style, COALESCE(UPPER(ms.meta1), \'N/A\') AS meta1, COALESCE(UPPER(ms.meta2), \'N/A\') AS meta2, COALESCE(UPPER(ms.meta3), \'N/A\') AS meta3, SUM(w.units_on_hand) AS wh_units FROM wh_zoned w LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(w.sku_group) = UPPER(ms.sku_group) WHERE ms.category IS NOT NULL AND w.wh_zone <> \'OTHER\' GROUP BY 1,2,3,4,5,6 ), zone_specific AS ( SELECT wh_zone AS scope, category, style, meta1, meta2, meta3, wh_units, wh_units / NULLIF(SUM(wh_units) OVER (PARTITION BY wh_zone), 0) AS warehouse_mix_pct FROM wh_tagged ), company_wide AS ( SELECT \'ALL\' AS scope, category, style, meta1, meta2, meta3, SUM(wh_units) AS wh_units, SUM(wh_units) / NULLIF(SUM(SUM(wh_units)) OVER (), 0) AS warehouse_mix_pct FROM wh_tagged GROUP BY category, style, meta1, meta2, meta3 ) SELECT * FROM zone_specific UNION ALL SELECT * FROM company_wide;",
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
            