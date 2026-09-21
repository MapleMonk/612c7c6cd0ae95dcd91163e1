{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.prepack_so_tracker_uc_sku AS WITH item_master AS ( SELECT sku, SPLIT_PART(sku, \'-\', 2) || \'-\' || SPLIT_PART(sku, \'-\', 3) AS sku_group FROM snitch_db.maplemonk.uc_final_item_master WHERE name ILIKE \'kit%\' AND sku ILIKE \'PP%\' AND NOT ( ( SPLIT_PART(sku, \'-\', 2) || \'-\' || SPLIT_PART(sku, \'-\', 3) ) = \'4JE131-03\' AND REGEXP_LIKE( sku, \'^PP16-4JE131-03-[0-9]{4}-[0-9]+$\', \'i\' ) ) ), item_master_with_warehouse AS ( SELECT im.sku, im.sku_group, u.facility, u.inventory FROM item_master im LEFT JOIN snitch_db.maplemonk.unicommerce_live_inventory u ON im.sku = u.\"Item SkuCode\" ), prepack_count AS ( SELECT sku_group, facility, COUNT(DISTINCT sku) AS total_prepacks, COUNT( DISTINCT CASE WHEN inventory > 0 THEN sku END ) AS prepacks_left FROM item_master_with_warehouse GROUP BY sku_group, facility ), base AS ( SELECT SPLIT_PART(u.\"Item SkuCode\", \'-\', 2) || \'-\' || SPLIT_PART(u.\"Item SkuCode\", \'-\', 3) AS sku_group, CASE WHEN ( SPLIT_PART(u.\"Item SkuCode\", \'-\', 2) || \'-\' || SPLIT_PART(u.\"Item SkuCode\", \'-\', 3) ) ILIKE \'%MSQ%\' THEN NULL WHEN u.\"Item SkuCode\" ILIKE \'PP-%\' THEN TRY_TO_NUMBER( SPLIT_PART(u.\"Item SkuCode\", \'-\', 4) ) WHEN REGEXP_LIKE( SPLIT_PART(u.\"Item SkuCode\", \'-\', 1), \'^PP[0-9]+$\', \'i\' ) THEN TRY_TO_NUMBER( REGEXP_SUBSTR( SPLIT_PART(u.\"Item SkuCode\", \'-\', 1), \'[0-9]+\' ) ) ELSE NULL END AS depth, u.updated, u.facility, u.inventory FROM snitch_db.maplemonk.unicommerce_live_inventory u WHERE u.\"Item SkuCode\" ILIKE \'PP%\' AND u.inventory > 0 ), sku_group_agg AS ( SELECT b.sku_group, b.facility, MAX(UPPER(m.category)) AS category, MAX(b.depth) AS depth, MAX(b.updated) AS updated, SUM(b.inventory) AS inventory, ROUND( SUM( COALESCE(b.depth, 0) * b.inventory ) ) AS depth_inventory FROM base b LEFT JOIN snitch_db.maplemonk.meta_mapping_cogs_sku m ON b.sku_group = m.sku_group GROUP BY b.sku_group, b.facility ) SELECT a.sku_group, a.category, a.depth, a.updated, a.facility, a.inventory, a.depth_inventory, COALESCE(p.total_prepacks, 0) AS total_prepacks, COALESCE(p.prepacks_left, 0) AS prepacks_left, ROUND( 100.0 * COALESCE(p.prepacks_left, 0) / NULLIF(p.total_prepacks, 0), 2 ) AS prepack_fill_rate FROM sku_group_agg a LEFT JOIN prepack_count p ON a.sku_group = p.sku_group AND a.facility = p.facility;",
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
            