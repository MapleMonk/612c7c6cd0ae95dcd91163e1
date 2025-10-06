{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE mp_cataloged_static_and_pslj as (WITH transformed_barcodes AS ( SELECT CASE WHEN REGEXP_COUNT(UPPER(TRIM(barcode)), \'-\') >= 2 THEN UPPER(TRIM(REGEXP_REPLACE(barcode, \'-[^-]+$\', \'\'))) ELSE UPPER(TRIM(barcode)) END AS sku_group, catalog_completed_date FROM snitch_db.maplemonk.pslj_final_pslj_table ), base AS ( SELECT sku_group, MAX(TO_TIMESTAMP_NTZ(FLOOR(TRY_TO_NUMBER(TRIM(NULLIF(catalog_completed_date, \'\'))) / 1000))) AS marketplace_cataloged_ts FROM transformed_barcodes GROUP BY sku_group ), mp_realtime AS ( SELECT sku_group FROM base WHERE sku_group LIKE \'MP%\' AND marketplace_cataloged_ts IS NOT NULL ), finale as(SELECT sku_group FROM mp_realtime UNION ALL SELECT mp_sku AS sku_group FROM snitch_db.maplemonk.mp_incorrect_catalog_mapping_sku_sheet1) select distinct sku_group from finale group by sku_group) ;",
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
            