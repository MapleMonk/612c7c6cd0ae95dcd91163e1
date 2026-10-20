{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.prepack_so_tracker_uc_sku AS WITH live_inventory AS ( SELECT UPPER(TRIM(u.\"Item SkuCode\")) AS pp_code, SPLIT_PART( UPPER(TRIM(u.\"Item SkuCode\")), \'-\', 2 ) || \'-\' || SPLIT_PART( UPPER(TRIM(u.\"Item SkuCode\")), \'-\', 3 ) AS sku_group, CASE WHEN ( SPLIT_PART(u.\"Item SkuCode\", \'-\', 2) || \'-\' || SPLIT_PART(u.\"Item SkuCode\", \'-\', 3) ) ILIKE \'%MSQ%\' THEN NULL WHEN u.\"Item SkuCode\" ILIKE \'PP-%\' THEN TRY_TO_NUMBER( SPLIT_PART(u.\"Item SkuCode\", \'-\', 4) ) WHEN REGEXP_LIKE( SPLIT_PART(u.\"Item SkuCode\", \'-\', 1), \'^PP[0-9]+$\', \'i\' ) THEN TRY_TO_NUMBER( REGEXP_SUBSTR( SPLIT_PART(u.\"Item SkuCode\", \'-\', 1), \'[0-9]+\' ) ) ELSE NULL END AS depth, u.updated, UPPER(TRIM(u.facility)) AS facility, TRY_TO_NUMBER(u.inventory) AS inventory FROM snitch_db.maplemonk.unicommerce_live_inventory u WHERE u.\"Item SkuCode\" ILIKE \'PP%\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY UPPER(TRIM(u.\"Item SkuCode\")), UPPER(TRIM(u.facility)) ORDER BY u.updated DESC NULLS LAST, TRY_TO_NUMBER(u.inventory) DESC NULLS LAST ) = 1 ), sku_group_agg AS ( SELECT b.sku_group, b.facility, MAX(b.depth) AS depth, MAX(b.updated) AS updated, COUNT( DISTINCT CASE WHEN b.inventory IN (0, 1) THEN b.pp_code END ) AS inventory, COUNT( DISTINCT CASE WHEN b.inventory = 1 THEN b.pp_code END ) AS prepacks_left, ROUND( SUM( COALESCE(b.inventory, 0) * COALESCE(b.depth, 0) ) ) AS depth_inventory FROM live_inventory b GROUP BY b.sku_group, b.facility ), category_mapping AS ( SELECT UPPER(TRIM(sku_group)) AS sku_group, MAX(UPPER(category)) AS category FROM snitch_db.maplemonk.meta_mapping_cogs_sku GROUP BY 1 ) SELECT a.sku_group, m.category, a.depth, a.updated, a.facility, a.inventory AS inventory, a.depth_inventory, a.prepacks_left AS prepacks_left, ROUND( 100.0 * a.prepacks_left / NULLIF(a.inventory, 0), 2 ) AS prepack_fill_rate FROM sku_group_agg a LEFT JOIN category_mapping m ON a.sku_group = m.sku_group;",
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
            