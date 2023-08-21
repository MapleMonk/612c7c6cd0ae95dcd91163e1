{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.old_180_inventory_deep_dive as WITH InventoryDetails AS ( SELECT sku_group, product_name, days_in_warehouse, SUM(units_on_hand) as total_units_on_hand FROM snitch_db.MAPLEMONK.INVENTORY_AGING_BUCKETS_SNITCH GROUP BY sku_group, product_name, days_in_warehouse ) SELECT id.sku_group, id.days_in_warehouse, id.total_units_on_hand, eoq.product_name, eoq.FINAL_ROS, eoq.DAYS_SINCE_FIRST_ORDER, eoq.FIRST_ORDER_DATE, eoq.SALES_FIRST_15_DAYS, eoq.SALES_LAST_15_DAYS, eoq.TOTAL_SALES, eoq.TOTAL_RETURNS, eoq.SALES_FIRST_30_DAYS, eoq.SALES_LAST_30_DAYS FROM InventoryDetails id LEFT JOIN snitch_db.MAPLEMONK.EOQ eoq ON id.sku_group = eoq.sku_group ORDER BY id.total_units_on_hand DESC",
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
                        