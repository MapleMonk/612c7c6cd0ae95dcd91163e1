{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.plus_inventory_cut_sizes AS WITH daily_sku_snapshot AS ( SELECT CAST(SNAPSHOT_TS AS DATE) AS DATE, UPPER(TRIM(SKU_GROUP)) AS SKU_GROUP, CATEGORY, CUT_SIZE, OFFLINE_INVENTORY FROM snitch_db.maplemonk.inventory_daily_snapshot_latest WHERE UPPER(TRIM(SKU_GROUP)) LIKE \'4MBG%\' OR UPPER(TRIM(SKU_GROUP)) LIKE \'4B%\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY CAST(SNAPSHOT_TS AS DATE), UPPER(TRIM(SKU_GROUP)) ORDER BY SNAPSHOT_TS DESC ) = 1 ), category_summary AS ( SELECT DATE, CATEGORY, COUNT(*) AS TOTAL_STYLES, COUNT_IF( COALESCE(OFFLINE_INVENTORY, 0) > 0 ) AS LIVE_STYLES, COUNT_IF( LOWER(TRIM(CUT_SIZE)) = \'cut\' ) AS CUT_SIZE_STYLES, COUNT_IF( LOWER(TRIM(CUT_SIZE)) = \'non_cut\' ) AS FULL_SIZE_STYLES, SUM( CASE WHEN LOWER(TRIM(CUT_SIZE)) = \'cut\' THEN COALESCE(OFFLINE_INVENTORY, 0) ELSE 0 END ) AS CUT_SIZE_INVENTORY_QTY, SUM( CASE WHEN LOWER(TRIM(CUT_SIZE)) = \'non_cut\' THEN COALESCE(OFFLINE_INVENTORY, 0) ELSE 0 END ) AS FULL_SIZE_INVENTORY_QTY, SUM( CASE WHEN COALESCE(OFFLINE_INVENTORY, 0) > 0 THEN OFFLINE_INVENTORY ELSE 0 END ) AS LIVE_INVENTORY_QTY, SUM( COALESCE(OFFLINE_INVENTORY, 0) ) AS TOTAL_INVENTORY_QTY FROM daily_sku_snapshot GROUP BY DATE, CATEGORY ) SELECT DATE, CATEGORY, CUT_SIZE_INVENTORY_QTY, CUT_SIZE_STYLES, FULL_SIZE_INVENTORY_QTY, FULL_SIZE_STYLES, LIVE_INVENTORY_QTY, LIVE_STYLES, TOTAL_INVENTORY_QTY, TOTAL_STYLES FROM category_summary ORDER BY DATE DESC, LIVE_STYLES DESC;",
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
            