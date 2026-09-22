{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.daily_sales_summary_dashboard AS WITH sku_mapping AS ( SELECT DISTINCT SKU, COLLECTION FROM snitch_db.maplemonk.collection_sku_mapped_sheet1 ), daily_sales_summary AS ( SELECT h.DATE, h.SKU_GROUP, c.COLLECTION AS CAPSULE, ANY_VALUE(h.CATEGORY) AS CATEGORY, CASE WHEN h.CHANNEL LIKE \'SNITCH - COCO%\' THEN \'Offline Store\' WHEN h.CHANNEL LIKE \'SNITCH - FOCO%\' THEN \'Offline Store\' WHEN h.CHANNEL LIKE \'SNITCH - COFO%\' THEN \'Offline Store\' WHEN UPPER(h.CHANNEL) IN ( \'MYNTRA\', \'AJIO\', \'AMAZON\', \'FLIPKART\', \'NYKAA\', \'TATACLIQ\', \'TATA CLIQ\' ) THEN \'Marketplace\' WHEN LOWER(h.CHANNEL) IN (\'web\',\'web2\') THEN \'Website\' WHEN LOWER(h.CHANNEL) LIKE \'app%\' THEN \'App\' ELSE \'Others\' END AS CHANNEL_GROUP, h.CHANNEL, SUM(h.GROSS_QUANTITY) AS TOTAL_QTY, SUM(h.GROSS_SALES) AS GROSS_SALES, SUM(h.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT, ROUND( SUM(h.DISCOUNT_AMOUNT) * 100.0 / NULLIF( SUM(h.GROSS_SALES + h.DISCOUNT_AMOUNT), 0 ), 2) AS DISCOUNT_PCT, ROUND( SUM(h.GROSS_SALES) / NULLIF(SUM(h.GROSS_QUANTITY),0), 2) AS ASP_GROSS, COUNT( DISTINCT CASE WHEN CHANNEL_GROUP = \'Offline Store\' THEN CHANNEL END ) AS ACTIVE_STORES FROM snitch_db.maplemonk.horizontal_sales_categories h JOIN sku_mapping c ON h.SKU_GROUP = c.SKU GROUP BY h.DATE, h.SKU_GROUP, c.COLLECTION, CASE WHEN h.CHANNEL LIKE \'SNITCH - COCO%\' THEN \'Offline Store\' WHEN h.CHANNEL LIKE \'SNITCH - FOCO%\' THEN \'Offline Store\' WHEN h.CHANNEL LIKE \'SNITCH - COFO%\' THEN \'Offline Store\' WHEN UPPER(h.CHANNEL) IN ( \'MYNTRA\', \'AJIO\', \'AMAZON\', \'FLIPKART\', \'NYKAA\', \'TATACLIQ\', \'TATA CLIQ\' ) THEN \'Marketplace\' WHEN LOWER(h.CHANNEL) IN (\'web\',\'web2\') THEN \'Website\' WHEN LOWER(h.CHANNEL) LIKE \'app%\' THEN \'App\' ELSE \'Others\' END, h.CHANNEL ) SELECT dss.*, CASE WHEN dss.CAPSULE IN ( \'Embroided Shirts\', \'botanical THEME\', \'Street FC\', \'Street FC Plus\', \'GOLF POLO THEME\', \'PHOTO PRINT\', \'Try Less Denim\', \'Printed Shirts\' ) THEN \'YES\' ELSE \'NO\' END AS FOCUS_CAPSULE FROM daily_sales_summary dss ORDER BY DATE DESC, GROSS_SALES DESC;",
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
            