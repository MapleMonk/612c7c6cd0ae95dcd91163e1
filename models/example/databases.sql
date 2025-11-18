{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LIVE_INV_WAREHOUSE_offline_replen AS WITH data AS ( SELECT \"Item SkuCode\" AS sku, FACILITY, COALESCE(SUM(inventory), 0) AS qty FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date AND FACILITY in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL-NORTH-TAURU\') GROUP BY sku, FACILITY ) SELECT sku, facility, SUM(qty) as units_on_hand FROM data GROUP BY sku, facility ;",
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
            