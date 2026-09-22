{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.plus_inventory_cut_sizes AS WITH latest_plus_data AS ( SELECT category, sku_group, final_total_qty, total_remark FROM snitch_db.maplemonk.full_size_inv_avail WHERE date = ( SELECT MAX(date) FROM snitch_db.maplemonk.full_size_inv_avail ) AND ( UPPER(sku_group) LIKE \'4MBG%\' OR UPPER(sku_group) LIKE \'4B%\' ) ), sku_level AS ( SELECT category, sku_group, MAX(final_total_qty) AS final_total_qty, MAX(total_remark) AS total_remark FROM latest_plus_data GROUP BY category, sku_group ), category_summary AS ( SELECT category, COUNT(*) AS total_styles, COUNT_IF(final_total_qty > 0) AS live_styles, COUNT_IF(total_remark = \'Full Size Inv\') AS full_size_styles, COUNT(*) - COUNT_IF(total_remark = \'Full Size Inv\') AS cut_size_styles, SUM( CASE WHEN total_remark = \'Full Size Inv\' THEN COALESCE(final_total_qty, 0) ELSE 0 END ) AS full_size_inventory_qty, SUM( CASE WHEN total_remark <> \'Full Size Inv\' OR total_remark IS NULL THEN COALESCE(final_total_qty, 0) ELSE 0 END ) AS cut_size_inventory_qty, SUM(COALESCE(final_total_qty, 0)) AS total_inventory_qty FROM sku_level GROUP BY category ) SELECT category, total_styles, live_styles, cut_size_styles, full_size_styles, cut_size_inventory_qty, full_size_inventory_qty, total_inventory_qty FROM category_summary ORDER BY live_styles DESC;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            