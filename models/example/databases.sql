{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.capsule_clicks_dashboard AS WITH sku_mapping AS ( SELECT DISTINCT UPPER(TRIM(SKU)) AS SKU, TRIM(COLLECTION) AS COLLECTION FROM snitch_db.maplemonk.collection_sku_mapped_sheet1 WHERE SKU IS NOT NULL AND COLLECTION IS NOT NULL ), sku_clicks AS ( SELECT UPPER(TRIM(SKU_GROUP)) AS SKU, MAX(CATEGORY) AS CATEGORY, MAX(CLICKS7) AS CLICKS7, MAX(CLICKS30) AS CLICKS30 FROM snitch_db.maplemonk.category_journey WHERE SKU_GROUP IS NOT NULL GROUP BY UPPER(TRIM(SKU_GROUP)) ), capsule_sku_clicks AS ( SELECT s.SKU, CASE WHEN LOWER(TRIM(m.COLLECTION)) = \'try less denim\' THEN \'Jeans\' ELSE s.CATEGORY END AS CATEGORY, m.COLLECTION AS CAPSULE, s.CLICKS7, s.CLICKS30 FROM sku_clicks s INNER JOIN sku_mapping m ON s.SKU = m.SKU ), category_clicks AS ( SELECT CATEGORY, SUM(COALESCE(CLICKS7, 0)) AS CATEGORY_CLICKS_7D, SUM(COALESCE(CLICKS30, 0)) AS CATEGORY_CLICKS_30D, COUNT(DISTINCT SKU) AS CATEGORY_SKU_COUNT FROM sku_clicks GROUP BY CATEGORY ), capsule_clicks AS ( SELECT CAPSULE, CATEGORY, SUM(COALESCE(CLICKS7, 0)) AS CAPSULE_CLICKS_7D, SUM(COALESCE(CLICKS30, 0)) AS CAPSULE_CLICKS_30D, COUNT(DISTINCT SKU) AS CAPSULE_SKU_COUNT FROM capsule_sku_clicks GROUP BY CAPSULE, CATEGORY ) SELECT c.CAPSULE, c.CATEGORY, c.CAPSULE_CLICKS_7D, c.CAPSULE_CLICKS_30D, c.CAPSULE_SKU_COUNT, cat.CATEGORY_CLICKS_7D AS BASE_CATEGORY_CLICKS_7D, cat.CATEGORY_CLICKS_30D AS BASE_CATEGORY_CLICKS_30D, cat.CATEGORY_SKU_COUNT AS BASE_CATEGORY_SKU_COUNT, ROUND( c.CAPSULE_CLICKS_7D / NULLIF( cat.CATEGORY_CLICKS_7D, 0 ) * 100, 2 ) AS CAPSULE_CLICK_SHARE_7D_PCT, ROUND( c.CAPSULE_CLICKS_30D / NULLIF( cat.CATEGORY_CLICKS_30D, 0 ) * 100, 2 ) AS CAPSULE_CLICK_SHARE_30D_PCT, ROUND( ( c.CAPSULE_CLICKS_7D / NULLIF( cat.CATEGORY_CLICKS_7D, 0 ) ) / ( c.CAPSULE_SKU_COUNT / NULLIF( cat.CATEGORY_SKU_COUNT, 0 ) ), 2 ) AS CLICK_INDEX_7D, ROUND( ( c.CAPSULE_CLICKS_30D / NULLIF( cat.CATEGORY_CLICKS_30D, 0 ) ) / ( c.CAPSULE_SKU_COUNT / NULLIF( cat.CATEGORY_SKU_COUNT, 0 ) ), 2 ) AS CLICK_INDEX_30D, CASE WHEN LOWER(TRIM(c.CAPSULE)) IN ( \'embroided shirts\', \'botanical theme\', \'street fc\', \'street fc plus\', \'golf polo theme\', \'photo print\', \'try less denim\', \'printed shirts\' ) THEN \'YES\' ELSE \'NO\' END AS FOCUS_CAPSULE FROM capsule_clicks c LEFT JOIN category_clicks cat ON LOWER(TRIM(c.CATEGORY)) = LOWER(TRIM(cat.CATEGORY)) ORDER BY c.CAPSULE_CLICKS_30D DESC;",
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
            