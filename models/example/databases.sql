{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.SFUCCHECK AS SELECT sf.*, uc.order_name as uc_statusas FROM snitch_db.maplemonk.fact_items_snitch sf LEFT JOIN snitch_db.maplemonk.warehouse_sla_performance uc ON sf.order_name = uc.order_name WHERE LOWER(COALESCE(tags, \'\')) NOT LIKE \'%eco%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%rms%\' AND LOWER(COALESCE(tags, \'\')) NOT LIKE \'%exch%\' AND sf.order_timestamp >= \'2026-01-01\' and city_mapped != \'TEST\' AND uc.order_name is null and sf.order_status not in (\'CANCELLED\') order by order_timestamp asc ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.UCLOGICCHECK AS (SELECT uc.awb, uc.order_name, uc.dispatched_timestamp, uc.marketplace_mapped, uc.sku, le.order_no, uc.warehouse_name, uc.invoice_date FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate_2024 uc LEFT JOIN snitch_db.maplemonk.logicerpnew_get_sale_invoice le ON uc.order_name = le.order_no WHERE uc.marketplace_mapped NOT LIKE \'%STORE%\' and uc.marketplace_mapped not in (\'NOON\',\'SAPL_WH_TRANSFERS\',\'FLIPKART_SOR\',\'MYNTRA_SJIT\',\'M-NOW\',\'SLIKK\',\'M_NOW_WESTBENGAL\',\'KNOT\',\'B2B CORPORATE CHANNEL\',\'ZILO\',\'INSTAMART\',\'SNITCH HQ\') AND uc.marketplace_mapped NOT IN (\'CUSTOM\',\'FLIPKART\') AND uc.dispatched_timestamp IS NOT NULL AND uc.invoice_date::DATE >= \'2026-01-01\' AND sku not in (\'DUMMY-BOX-01\')) UNION ALL (SELECT uc.awb, uc.order_name, uc.dispatched_timestamp, uc.marketplace_mapped, uc.sku, le.order_no, uc.warehouse_name, uc.invoice_date FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate_2024 uc LEFT JOIN snitch_db.maplemonk.logicerpnew_get_sale_invoice le ON uc.awb = le.gr_number WHERE uc.marketplace_mapped NOT LIKE \'%STORE%\' AND uc.marketplace_mapped = \'FLIPKART\' AND uc.dispatched_timestamp IS NOT NULL AND uc.invoice_date::DATE >= \'2026-01-01\' AND sku not in (\'DUMMY-BOX-01\') ) ;",
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
            