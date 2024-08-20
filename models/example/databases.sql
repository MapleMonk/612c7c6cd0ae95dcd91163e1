{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LIVE_INV_WAREHOUSE_offline_replen AS SELECT \"Item SkuCode\" AS sku, FACILITY, COALESCE(SUM(inventory), 0) AS total_units_on_hand FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date GROUP BY sku, FACILITY;",
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
            