{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.order_point as SELECT factory.MOQ, factory.safety, factory.factory, factory.category, factory.lead_time, factory.min_days_to, final.sku_group, final.product_name, final.category final_category, final.Final_ROS, average_returns_since_first_order, CASE WHEN final.Final_ROS * (1 - final.average_returns_since_first_order / 100) * (factory.lead_time + factory.safety) < 0 THEN 0 ELSE final.Final_ROS * (1 - final.average_returns_since_first_order / 100) * (factory.lead_time + factory.safety) END as OP FROM ( SELECT sales.sku_group, sales.product_name, sales.category, sales.Final_ROS, returns.average_returns_since_first_order FROM ( SELECT sku_group, product_name, category, AVG(units_sold_l180/180) AS avg_rate_of_sale_180, AVG(units_sold_l90/90) AS avg_rate_of_sale_90, AVG(units_sold_l30/30) AS avg_rate_of_sale_30, CASE WHEN DATEDIFF(day, first_order_date, GETDATE()) >= 180 THEN avg_rate_of_sale_180 WHEN DATEDIFF(day, first_order_date, GETDATE()) BETWEEN 90 AND 179 THEN avg_rate_of_sale_90 ELSE avg_rate_of_sale_30 END as Final_ROS, first_order_date FROM ( SELECT sku_group, product_name, category, units_sold_l180, units_sold_l90, units_sold_l30, first_order_date FROM snitch_db.maplemonk.Inventory_summary_marketplace_snitch WHERE sku_group is not NULL ) sales GROUP BY sku_group, product_name, category, first_order_date ) as sales LEFT JOIN ( SELECT subquery.sku_group, CASE WHEN total_sales_quantity = 0 THEN 0 ELSE total_returns/total_sales_quantity * 100 END AS average_returns_since_first_order FROM ( SELECT sku_group, sum(return_quantity) as total_returns, sum(suborder_quantity) as total_sales_quantity FROM snitch_db.maplemonk.unicommerce_fact_items_snitch GROUP BY sku_group ) as subquery ) as returns ON sales.sku_group = returns.sku_group ORDER BY sales.Final_ROS DESC ) as final LEFT JOIN snitch_db.MAPLEMONK.FACTORY_INPUTS AS factory ON final.sku_group = factory.sku_group order by final_ros desc",
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
                        