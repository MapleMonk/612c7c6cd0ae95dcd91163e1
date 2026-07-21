{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_availability_mix AS WITH wh_options AS ( SELECT UPPER(a.sku_group) AS sku_group, UPPER(a.warehouse_location) AS wh_zone, CASE WHEN UPPER(a.sku_group) LIKE \'4MBG%\' OR UPPER(a.sku_group) LIKE \'%4BJ002%\' OR UPPER(a.sku_group) LIKE \'4BRN%\' OR UPPER(a.sku_group) LIKE \'4BPL%\' OR UPPER(a.sku_group) LIKE \'4BTR%\' OR UPPER(a.sku_group) LIKE \'4BCH%\' OR UPPER(a.sku_group) LIKE \'4BJE%\' OR UPPER(a.sku_group) LIKE \'4BJK%\' OR UPPER(a.sku_group) LIKE \'4BSW%\' OR UPPER(a.sku_group) LIKE \'4BHD%\' OR UPPER(a.sku_group) LIKE \'4BOS%\' OR UPPER(a.sku_group) LIKE \'4BSHS%\' OR UPPER(a.sku_group) LIKE \'4BSFS%\' OR UPPER(a.sku_group) LIKE \'4BSH%\' OR UPPER(a.sku_group) LIKE \'4BBL%\' OR UPPER(a.sku_group) LIKE \'4BCA%\' OR UPPER(a.sku_group) LIKE \'4BSWT%\' OR UPPER(a.sku_group) LIKE \'4BCD%\' OR UPPER(a.sku_group) LIKE \'4BJO%\' THEN GREATEST(LEAST( ROUND((a.XL3_UNITS - 1) / 3), ROUND((a.XL4_UNITS - 1) / 3), ROUND((a.XL5_UNITS - 1) / 2), ROUND((a.XL6_UNITS - 1) / 2) ), 0) ELSE GREATEST(LEAST( ROUND((a.S_UNITS - 1) / 3), ROUND((a.M_UNITS - 1) / 4), ROUND((a.L_UNITS - 1) / 4), ROUND((a.XL_UNITS - 1) / 3) ), 0) END AS full_size_options FROM snitch_db.maplemonk.AVAILABILITY_MASTER_V2_MERGED a ), wh_zoned AS ( SELECT sku_group, wh_zone, full_size_options FROM wh_options WHERE full_size_options >= 1 AND wh_zone IN (\'NORTH\', \'SOUTH\') ), wh_tagged AS ( SELECT w.wh_zone, ms.category, COALESCE(UPPER(ms.style), \'SNITCH\') AS style, COALESCE(UPPER(ms.meta1), \'N/A\') AS meta1, COALESCE(UPPER(ms.meta2), \'N/A\') AS meta2, COALESCE(UPPER(ms.meta3), \'N/A\') AS meta3, COUNT(DISTINCT w.sku_group) AS wh_option_count FROM wh_zoned w LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(w.sku_group) = UPPER(ms.sku_group) WHERE ms.category IS NOT NULL GROUP BY 1,2,3,4,5,6 ), zone_specific AS ( SELECT wh_zone AS scope, category, style, meta1, meta2, meta3, wh_option_count, wh_option_count / NULLIF(SUM(wh_option_count) OVER (PARTITION BY wh_zone), 0) AS warehouse_mix_pct FROM wh_tagged ), company_wide AS ( SELECT \'ALL\' AS scope, category, style, meta1, meta2, meta3, SUM(wh_option_count) AS wh_option_count, SUM(wh_option_count) / NULLIF(SUM(SUM(wh_option_count)) OVER (), 0) AS warehouse_mix_pct FROM wh_tagged GROUP BY category, style, meta1, meta2, meta3 ) SELECT * FROM zone_specific UNION ALL SELECT * FROM company_wide;",
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
            