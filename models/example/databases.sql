{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.plus_pipeline_master AS WITH production_base AS ( SELECT CATEGORY, SKU_GROUP, QTY, PRODUCTION_STATUS, STATUS, FACTORY_NAME, INCHARGE_NAME, FIT, COLOR, IMAGE_LINK, DATE_ISSUED, REVISED_DELIVERY_DATE, EXPECTED_DELIVERY_DATE, COALESCE( REVISED_DELIVERY_DATE, EXPECTED_DELIVERY_DATE ) AS PIPELINE_DATE FROM snitch_db.maplemonk.snitch_2_production_tracking_bharat WHERE ( UPPER(SKU_GROUP) LIKE \'4MBG%\' OR UPPER(SKU_GROUP) LIKE \'4B%\' ) ), clean_dates AS ( SELECT *, DATE_TRUNC( \'MONTH\', TRY_TO_DATE(PIPELINE_DATE, \'DD-MM-YYYY\') ) AS PIPELINE_MONTH FROM production_base ), sku_level AS ( SELECT PIPELINE_MONTH, PIPELINE_DATE, CATEGORY, SKU_GROUP, SUM(QTY) AS PIPELINE_QTY, PRODUCTION_STATUS, STATUS, FACTORY_NAME, INCHARGE_NAME, FIT, COLOR, IMAGE_LINK, DATE_ISSUED FROM clean_dates WHERE PIPELINE_MONTH IS NOT NULL AND UPPER(COALESCE(PRODUCTION_STATUS, \'\')) <> \'DELIVERED\' AND UPPER(COALESCE(STATUS, \'\')) <> \'DELIVERED\' AND UPPER(COALESCE(STATUS, \'\')) <> \'CANCEL\' GROUP BY PIPELINE_MONTH, PIPELINE_DATE, CATEGORY, SKU_GROUP, PRODUCTION_STATUS, STATUS, FACTORY_NAME, INCHARGE_NAME, FIT, COLOR, IMAGE_LINK, DATE_ISSUED ) SELECT PIPELINE_MONTH, PIPELINE_DATE, CATEGORY, SKU_GROUP, PIPELINE_QTY, PRODUCTION_STATUS, STATUS, FACTORY_NAME, INCHARGE_NAME, FIT, COLOR, IMAGE_LINK, CONCAT( \'<img src=\"\', IMAGE_LINK, \'\" width=\"80\" height=\"80\" style=\"object-fit:contain;\">\' ) AS IMAGE, DATE_ISSUED, CASE WHEN UPPER(SKU_GROUP) LIKE \'4MBG%\' THEN \'4MBG\' WHEN UPPER(SKU_GROUP) LIKE \'4B%\' THEN \'4B\' END AS PLUS_SKU_TYPE FROM sku_level ORDER BY PIPELINE_MONTH, CATEGORY, PIPELINE_DATE, SKU_GROUP;",
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
            