{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.switch_uc_api as WITH transfer AS ( SELECT order_id, saleorderitemcode, warehouse_name, item_status, order_date, sku FROM snitch_db.maplemonk.warehouse_sla_performance WHERE marketplace_mapped = \'SHOPIFY\' AND item_status IN (\'UNFULFILLABLE\') AND warehouse_name ILIKE \'%SAPL%\' ), inv AS ( SELECT \"Item SkuCode\" AS sku2, SUM(CASE WHEN facility = \'SAPL-WH2\' THEN inventory ELSE 0 END) AS \"SAPL-WH2\", SUM(CASE WHEN facility = \'SAPL-WH1\' THEN inventory ELSE 0 END) AS \"SAPL-WH1\", SUM(CASE WHEN facility = \'SAPL_EMIZA\' THEN inventory ELSE 0 END) AS \"SAPL_EMIZA\", SUM(CASE WHEN facility = \'SAPL-NORTH-TAURU\' THEN inventory ELSE 0 END) AS \"SAPL-NORTH-TAURU\", SUM(CASE WHEN facility IN (\'SAPL-NORTH-TAURU\',\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL_EMIZA\') THEN inventory ELSE 0 END) AS total_qty FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = CURRENT_DATE GROUP BY 1 HAVING SUM(CASE WHEN facility IN (\'SAPL-NORTH-TAURU\',\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL_EMIZA\') THEN inventory ELSE 0 END) > 0 ), final as ( SELECT t.order_id, t.saleorderitemcode, t.warehouse_name, CASE WHEN i.\"SAPL-WH2\" > 0 AND t.warehouse_name <> \'SAPL-WH2\' THEN \'SAPL-WH2\' WHEN i.\"SAPL-WH1\" > 0 AND t.warehouse_name <> \'SAPL-WH1\' THEN \'SAPL-WH1\' WHEN i.\"SAPL_EMIZA\" > 0 AND t.warehouse_name <> \'SAPL_EMIZA\' THEN \'SAPL_EMIZA\' WHEN i.\"SAPL-NORTH-TAURU\" > 0 AND t.warehouse_name <> \'SAPL-NORTH-TAURU\' THEN \'SAPL-NORTH-TAURU\' ELSE t.warehouse_name END AS suggested_facility, i.\"SAPL-WH2\", i.\"SAPL-WH1\", i.\"SAPL_EMIZA\", i.\"SAPL-NORTH-TAURU\", i.total_qty FROM transfer t LEFT JOIN inv i ON t.sku = i.sku2 ) select order_id, saleorderitemcode, suggested_facility from final where warehouse_name != suggested_facility ;",
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
            