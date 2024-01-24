{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.liquidation_list AS with sales as( SELECT ia.SKU, ia.SKU_GROUP, ia.PRODUCT_NAME, ia.DAYS_IN_WAREHOUSE, SUM(ia.UNITS_ON_HAND) AS TOTAL_UNITS_ON_HAND, ia.CATEGORY, ol.Last_7_days_sales, r.FINAL_ROS FROM ( SELECT SKU, SKU_GROUP, PRODUCT_NAME, DAYS_IN_WAREHOUSE, SUM(UNITS_ON_HAND) AS UNITS_ON_HAND, CATEGORY FROM Snitch_db.maplemonk.inventory_aging_buckets_snitch WHERE DAYS_IN_WAREHOUSE NOT IN (\'61-90\', \'91-120\', \'31-60\', \'0-30\') GROUP BY SKU, SKU_GROUP, PRODUCT_NAME, DAYS_IN_WAREHOUSE, CATEGORY ) AS ia LEFT JOIN ( SELECT SKU, SKU_GROUP, AVG(SHIPPING_QUANTITY) AS Last_7_days_sales FROM snitch_db.snitch.order_lineitems_fact WHERE order_date::date > (SELECT MAX(order_date::date) FROM snitch_db.snitch.order_lineitems_fact) - 7 GROUP BY SKU, SKU_GROUP ) AS ol ON ia.SKU = ol.SKU AND ia.SKU_GROUP = ol.SKU_GROUP LEFT JOIN ( SELECT SKU_GROUP, FINAL_ROS FROM SNITCH_DB.MAPLEMONK.ros_snitch ) AS r ON ia.SKU_GROUP = r.SKU_GROUP GROUP BY ia.SKU, ia.SKU_GROUP, ia.PRODUCT_NAME, ia.DAYS_IN_WAREHOUSE, ia.CATEGORY, ol.Last_7_days_sales, r.FINAL_ROS ) select doi.*,w.facility as warehouse from sales doi left join (select sku,facility from ( select \"Item Type skuCode\" as sku,facility,\"Item Created On\" ,row_number() over(partition by \"Item Type skuCode\" order by \"Item Created On\" ::timestamp desc)rw from snitch_db.maplemonk.unicommerce_inventory_aging ) where rw=1) w on lower(doi.sku) = lower(w.sku)",
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
                        