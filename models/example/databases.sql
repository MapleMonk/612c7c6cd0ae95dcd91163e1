{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.plus_inwards_master AS WITH product_images_raw AS ( SELECT LEFT( variant.value:sku::STRING, LENGTH(variant.value:sku::STRING) - POSITION( \'-\' IN REVERSE(variant.value:sku::STRING) ) ) AS SKU_GROUP, image.value:\"preview\":\"image\":\"url\"::STRING AS IMAGE_URL, image.index AS IMAGE_INDEX FROM snitch_db.maplemonk.new_meafields_product_products_graph_ql t, LATERAL FLATTEN(input => PARSE_JSON(t.media)) AS image, LATERAL FLATTEN(input => PARSE_JSON(t.variants)) AS variant WHERE image.value:\"mediaContentType\"::STRING = \'IMAGE\' ), product_images AS ( SELECT SKU_GROUP, IMAGE_URL FROM product_images_raw QUALIFY ROW_NUMBER() OVER ( PARTITION BY SKU_GROUP ORDER BY IMAGE_INDEX ) = 1 ), actual_inwards AS ( SELECT \'ACTUAL_INWARD\' AS INWARD_TYPE, INWARD_DATE AS INWARD_DATE, CATEGORY, SKU_GROUP, SUM(INWARD_QUANT) AS QTY FROM snitch_db.maplemonk.inwards_l1_mapped WHERE ( UPPER(SKU_GROUP) LIKE \'4MBG%\' OR UPPER(SKU_GROUP) LIKE \'4B%\' ) GROUP BY INWARD_DATE, CATEGORY, SKU_GROUP ), expected_inwards AS ( SELECT \'EXPECTED_INWARD\' AS INWARD_TYPE, COALESCE( ESTIMATED_DELIVERY_DATE, PLANNED_DATE ) AS INWARD_DATE, CATEGORY, SKU_GROUP, SUM(TOTAL_QTY) AS QTY FROM snitch_db.maplemonk.rts_tat WHERE ( UPPER(SKU_GROUP) LIKE \'4MBG%\' OR UPPER(SKU_GROUP) LIKE \'4B%\' ) AND INWARD_DATE IS NULL AND COALESCE( ESTIMATED_DELIVERY_DATE, PLANNED_DATE ) >= CURRENT_DATE() GROUP BY COALESCE( ESTIMATED_DELIVERY_DATE, PLANNED_DATE ), CATEGORY, SKU_GROUP ), combined_inwards AS ( SELECT INWARD_TYPE, INWARD_DATE, CATEGORY, SKU_GROUP, QTY FROM actual_inwards UNION ALL SELECT INWARD_TYPE, INWARD_DATE, CATEGORY, SKU_GROUP, QTY FROM expected_inwards ) SELECT c.INWARD_TYPE, c.INWARD_DATE, CASE WHEN UPPER(c.SKU_GROUP) LIKE \'4MBG%\' THEN \'4MBG\' WHEN UPPER(c.SKU_GROUP) LIKE \'4B%\' THEN \'4B\' END AS PLUS_SKU_TYPE, c.CATEGORY, c.SKU_GROUP, c.QTY, p.IMAGE_URL, CONCAT( \'<img src=\"\', p.IMAGE_URL, \'\" width=\"80\" height=\"80\" style=\"object-fit:contain;\">\' ) AS IMAGE FROM combined_inwards c LEFT JOIN product_images p ON c.SKU_GROUP = p.SKU_GROUP ORDER BY c.INWARD_DATE ASC, c.INWARD_TYPE, c.CATEGORY, c.QTY DESC;",
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
            