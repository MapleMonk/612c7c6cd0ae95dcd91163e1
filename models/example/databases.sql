{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LIVE_INV_WAREHOUSE_offline_replen AS SELECT sku, facility, SUM(units_on_hand) AS units_on_hand FROM ( SELECT \"Item Type skuCode\" AS sku, facility, COUNT(DISTINCT \"Item Code\") AS units_on_hand FROM SNITCH_DB.MAPLEMONK.unicommerce_inventory_aging WHERE CONVERT_TIMEZONE(\'UTC\', \'Asia/Kolkata\', _airbyte_emitted_at::DATETIME)::date = current_date() -1 GROUP BY 1, 2 ) AS subquery GROUP BY sku, facility UNION ALL SELECT \"Item SkuCode\" AS sku, FACILITY, COALESCE(SUM(inventory), 0) AS total_units_on_hand FROM snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE date = current_date GROUP BY sku, FACILITY;",
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
                        