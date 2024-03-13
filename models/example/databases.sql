{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.store_wise_daily_orders as WITH UnitsSold AS ( SELECT SKU, SKU_GROUP, category, marketplace_mapped, COALESCE(SUM(suborder_quantity), 0) as total_units_sold, COALESCE(SUM(selling_price), 0) as total_sales, COALESCE(SUM(return_quantity), 0) as total_return_quantity, COALESCE(SUM(discount), 0) as total_discount, order_date, EXTRACT (MONTH FROM order_date) as month, SUBSTRING(sku, LEN(sku) - CHARINDEX(\'-\', REVERSE(sku)) + 2, LEN(sku)) AS size FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE source IN (\'POS1\',\'POS-2\',\'POS-3\') GROUP BY sku, sku_group, order_date, month, category, size, marketplace_mapped ), UnitsOnHandInStore AS ( SELECT LOGICUSERCODE, SUM(STOCK_QTY) as units_in_hand_store FROM snitch_db.MAPLEMONK.LOGICERP_GET_STOCK_IN_HAND GROUP BY LOGICUSERCODE ), UnitsInWarehouse AS ( SELECT SKU, SUM(UNITS_ON_HAND) as units_on_hand_warehouse FROM snitch_db.MAPLEMONK.INVENTORY_AGING_BUCKETS_SNITCH GROUP BY SKU ), FirstOrderDate AS ( SELECT SKU, MIN(order_date) AS first_order_date FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE source IN (\'POS1\',\'POS-2\',\'POS-3\') GROUP BY SKU ), RateOfSale AS ( SELECT us.SKU, COALESCE(SUM(us.suborder_quantity), 0) / NULLIF(CAST(DATEDIFF(DAY, fod.first_order_date, CURRENT_DATE) AS FLOAT), 0) AS rate_of_sale FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH us JOIN FirstOrderDate fod ON us.SKU = fod.SKU WHERE source IN (\'POS1\',\'POS-2\',\'POS-3\') GROUP BY us.sku, fod.first_order_date ) SELECT us.*, COALESCE(ros.rate_of_sale, 0) AS rate_of_sale, COALESCE(uh.units_in_hand_store, 0) AS units_in_hand_store, COALESCE(uw.units_on_hand_warehouse, 0) AS units_on_hand_warehouse, CASE WHEN round(COALESCE(ros.rate_of_sale, 0)*4,0) - COALESCE(uh.units_in_hand_store, 0) > 0 THEN CASE WHEN (round(COALESCE(ros.rate_of_sale, 0)*4,0) - COALESCE(uh.units_in_hand_store, 0)) > COALESCE(uw.units_on_hand_warehouse, 0) THEN COALESCE(uw.units_on_hand_warehouse, 0) ELSE (round(COALESCE(ros.rate_of_sale, 0)*4,0) - COALESCE(uh.units_in_hand_store, 0)) END ELSE 0 END AS order_today FROM UnitsSold us LEFT JOIN RateOfSale ros ON us.SKU = ros.SKU LEFT JOIN UnitsOnHandInStore uh ON us.SKU = uh.LOGICUSERCODE LEFT JOIN UnitsInWarehouse uw ON us.SKU = uw.SKU ORDER BY units_in_hand_store DESC, units_on_hand_warehouse DESC",
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
                        