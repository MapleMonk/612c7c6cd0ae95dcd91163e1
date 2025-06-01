{{ config(
            materialized='table',
                post_hook={
                    "sql": "INSERT INTO snitch_db.maplemonk.availability_snapshot ( SKU_GROUP, ID, PRICE, CURRENT_SLASHED_PRICE, PRODUCT_NAME, CATEGORY, FINAL_ROS, TOTAL_EOQ, AVAILABLE_UNITS, ORDER_POINT, NATURAL_ROS, SALES_LAST_7_DAYS, SALES_LAST_15_DAYS, SALES_LAST_30_DAYS, SKU_CLASS, SELLABLE_INVENTORY, XS_UNITS, S_UNITS, M_UNITS, L_UNITS, XL_UNITS, XXL_UNITS, XL3_UNITS, XL4_UNITS, XL5_UNITS, XL6_UNITS, XL7_UNITS, XL8_UNITS, NA_UNITS, NUM_SIZE_AVAILABLE, STATUS, SNAPSHOT_TIMESTAMP ) SELECT SKU_GROUP, ID, PRICE, CURRENT_SLASHED_PRICE, PRODUCT_NAME, CATEGORY, FINAL_ROS, TOTAL_EOQ, AVAILABLE_UNITS, ORDER_POINT, NATURAL_ROS, SALES_LAST_7_DAYS, SALES_LAST_15_DAYS, SALES_LAST_30_DAYS, SKU_CLASS, SELLABLE_INVENTORY, XS_UNITS, S_UNITS, M_UNITS, L_UNITS, XL_UNITS, XXL_UNITS, XL3_UNITS, XL4_UNITS, XL5_UNITS, XL6_UNITS, XL7_UNITS, XL8_UNITS, NA_UNITS, NUM_SIZE_AVAILABLE, STATUS, CURRENT_TIMESTAMP() FROM snitch_db.maplemonk.availability_master_v2; create or replace table snitch_db.maplemonk.availability_snapshot as WITH latest_snapshots AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY DATE(snapshot_timestamp) ORDER BY snapshot_timestamp DESC) AS rn FROM snitch_db.maplemonk.availability_snapshot ) SELECT * FROM latest_snapshots WHERE rn = 1 ;",
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
            