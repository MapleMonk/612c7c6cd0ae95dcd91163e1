{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; create or replace table snitch_db.maplemonk.order_point_test as WITH products AS ( WITH latest_orders AS ( SELECT sku_group, product_name, category, ROW_NUMBER() OVER(PARTITION BY sku_group ORDER BY order_timestamp DESC) as rn FROM snitch_db.maplemonk.fact_items_snitch ) SELECT sku_group, product_name, category FROM latest_orders WHERE rn = 1 ), merged_data AS ( SELECT ros.*, COALESCE(factory.lead_time, 60) AS lead_time, COALESCE(factory.safety, 5) AS safety, factory.min_days_to, factory.factory, CASE WHEN factory.factory in (\'Exclude\') THEN 0 WHEN ros.final_ros * (1-ros.average_return_since_first_order/100) * (COALESCE(factory.lead_time, 60) + COALESCE(factory.safety, 5)) < 0 THEN 0 ELSE ros.final_ros * (1-ros.average_return_since_first_order/100) * (COALESCE(factory.lead_time, 60) + COALESCE(factory.safety, 5)) END AS OP FROM snitch_db.MAPLEMONK.ROS_SNITCH as ros LEFT JOIN snitch_db.MAPLEMONK.FACTORY_INPUTS as factory ON ros.sku_group = factory.sku_group ), inventory_on_hand AS ( Select sku_group , sum(total_units_on_hand) as total_units_on_hand from ( Select REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"),CHARINDEX(\'-\',REVERSE(\"Item SkuCode\")) + 1))AS sku_group, COALESCE(sum(inventory),0) as total_units_on_hand from snitch_db.maplemonk.snitch_final_inventory_wh2 where date = current_date group by 1 ) group by 1 ) SELECT merged_data.*, products.product_name, products.category, inventory_on_hand.total_units_on_hand FROM merged_data LEFT JOIN products ON merged_data.sku_group = products.sku_group LEFT JOIN inventory_on_hand ON merged_data.sku_group = inventory_on_hand.sku_group ORDER BY merged_data.final_ros DESC;",
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
            