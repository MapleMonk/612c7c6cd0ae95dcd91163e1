{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.order_point_inventory as select *, CASE WHEN OP < total_units_on_hand THEN 0 ELSE 1 END as inventory_below_order_point, CASE WHEN OP < total_units_on_hand THEN 0 ELSE ROUND(OP - total_units_on_hand) END as deficit_inventory from ( select * from snitch_db.MAPLEMONK.ORDER_POINT LEFT JOIN ( select REVERSE(SUBSTRING(REVERSE(sku), CHARINDEX(\'-\', REVERSE(sku)) + 1)) AS sku_group, sum(UNITS_ON_HAND) as total_units_on_hand from snitch_db.MAPLEMONK.INVENTORY_AGING_BUCKETS_SNITCH GROUP by sku_group order by total_units_on_hand desc ) as inventory_on_hand ON ORDER_POINT.sku_group = inventory_on_hand.sku_group WHERE OP is not null order by OP desc ) WHERE deficit_inventory > 100 order by OP desc, deficit_inventory desc",
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
                        