{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.EOQ_Test AS SELECT m.*, p.sku_group_pricing, p.mrp, p.sku_url, p.shopify_asp, p.myntra_asp, p.discount_per FROM ( select m.*, PERCENT_RANK() OVER (PARTITION BY 1 ORDER BY ifnull(sku_inventory_greater_than_10_flag,0)) * PERCENT_RANK() OVER (PARTITION BY 1 ORDER BY ifnull(final_ros,0)) availability_rank from (SELECT *, COALESCE(TOTAL_UNITS_ON_HAND, 0) - ((LEAD_TIME) * (final_ros * (1 - AVERAGE_RETURN_SINCE_FIRST_ORDER/100))) + aggregated.qup as inv_level_lead_time_end, OP - (COALESCE(TOTAL_UNITS_ON_HAND, 0) - ((LEAD_TIME + SAFETY) * (final_ros * (1 - AVERAGE_RETURN_SINCE_FIRST_ORDER/100))) + aggregated.qup) as EOQ, CASE WHEN (COALESCE(TOTAL_UNITS_ON_HAND, 0) - ((LEAD_TIME + SAFETY) * (final_ros * (1 - AVERAGE_RETURN_SINCE_FIRST_ORDER/100))) + aggregated.qup) < 0 THEN OP ELSE CASE WHEN (OP - (COALESCE(TOTAL_UNITS_ON_HAND, 0) - ((LEAD_TIME + SAFETY) * (final_ros * (1 - AVERAGE_RETURN_SINCE_FIRST_ORDER/100))) + aggregated.qup))<0 THEN 0 ELSE (OP - (COALESCE(TOTAL_UNITS_ON_HAND, 0) - ((LEAD_TIME + SAFETY) * (final_ros * (1 - AVERAGE_RETURN_SINCE_FIRST_ORDER/100))) + aggregated.qup)) END END as EOQ_NEW from ( SELECT order_point_test.*, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.quantity ELSE 0 END), 0) as qup, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.L ELSE 0 END), 0) as qup_L, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.M ELSE 0 END), 0) as qup_M, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.S ELSE 0 END), 0) as qup_S, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.XL ELSE 0 END), 0) as qup_XL, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.XXL ELSE 0 END), 0) as qup_XXL, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.XS ELSE 0 END), 0) as qup_XS, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.\"3XL\" ELSE 0 END), 0) as qup_3XL, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.\"4XL\" ELSE 0 END), 0) as qup_4XL, COALESCE(SUM(CASE WHEN SubQuery.consideration_flag = 1 THEN SubQuery.\"5XL\" ELSE 0 END), 0) as qup_5XL FROM snitch_db.maplemonk.order_point_test LEFT JOIN ( SELECT FACTORY_PRODUCTION_INVENTORY.sku_group, FACTORY_PRODUCTION_INVENTORY.quantity, FACTORY_PRODUCTION_INVENTORY.L, FACTORY_PRODUCTION_INVENTORY.M, FACTORY_PRODUCTION_INVENTORY.S, FACTORY_PRODUCTION_INVENTORY.XL, FACTORY_PRODUCTION_INVENTORY.XXL, FACTORY_PRODUCTION_INVENTORY.XS, FACTORY_PRODUCTION_INVENTORY.\"3XL\", FACTORY_PRODUCTION_INVENTORY.\"4XL\", FACTORY_PRODUCTION_INVENTORY.\"5XL\", FACTORY_PRODUCTION_INVENTORY.expected_delivery_date, FACTORY_INPUTS.lead_time, FACTORY_INPUTS.safety, CASE WHEN FACTORY_PRODUCTION_INVENTORY.expected_delivery_date < (CURRENT_DATE + CAST(COALESCE(FACTORY_INPUTS.lead_time,45) AS INTEGER) + CAST(COALESCE(FACTORY_INPUTS.safety,10) AS INTEGER)) THEN 1 ELSE 0 END AS consideration_flag FROM snitch_db.MAPLEMONK.FACTORY_PRODUCTION_INVENTORY LEFT JOIN snitch_db.MAPLEMONK.FACTORY_INPUTS ON FACTORY_PRODUCTION_INVENTORY.sku_group = FACTORY_INPUTS.sku_group ) AS SubQuery ON order_point_test.sku_group = SubQuery.sku_group GROUP BY order_point_test.sku_group, order_point_test.product_name, order_point_test.category, order_point_test.final_ros, order_point_test.AVERAGE_RETURN_SINCE_FIRST_ORDER, order_point_test.first_order_date, order_point_test.days_since_first_order, order_point_test.total_days_sold, order_point_test.total_returns, order_point_test.total_sales, order_point_test.sales_first_30_days, order_point_test.sales_first_15_days, order_point_test.sales_first_7_days, order_point_test.sales_first_60_days, order_point_test.sales_first_90_days, order_point_test.sales_first_180_days, order_point_test.sales_last_30_days, order_point_test.sales_last_15_days, order_point_test.sales_last_7_days, order_point_test.sales_last_60_days, order_point_test.sales_last_90_days, order_point_test.sales_last_180_days, order_point_test.natural_ros, MAX_FIRST_30_LAST_30_NATURAL_ROS, MAX_FIRST_30_LAST_30_NATURAL_ROS_FIRST_7_DAYS, order_point_test.OP, order_point_test.SAFETY, order_point_test.FACTORY, order_point_test.CATEGORY, order_point_test.LEAD_TIME, order_point_test.MIN_DAYS_TO, order_point_test.TOTAL_UNITS_ON_HAND ) as aggregated order by eoq_new DESC ) m left join ( select distinct sku_group, sku_inventory_greater_than_10_flag from snitch_db.maplemonk.Inventory_planning_summary_snitch ) n on lower(m.sku_group) = lower(n.sku_group) order by ifnull(sku_inventory_greater_than_10_flag,0) desc ) m LEFT JOIN ( SELECT sku_group_pricing, mrp, sku_url, shopify_asp, myntra_asp, discount_per FROM ( SELECT product_dim.sku_group as sku_group_pricing, price as mrp, concat(\'https://www.snitch.co.in/products/\',product_dim.handle) as sku_url, round(shopify_asp,0) as shopify_asp, round(myntra_asp,0) as myntra_asp, CASE WHEN shopify_asp is NULL THEN 0 WHEN price = 0 THEN 0 ELSE coalesce(round(1-shopify_asp/price,3),0)*100 END as discount_per, ROW_NUMBER() OVER (PARTITION BY product_dim.sku_group ORDER BY price ASC) as rn FROM snitch_db.snitch.product_dim LEFT JOIN snitch_db.maplemonk.pricing_recommendation ON product_dim.sku_group = pricing_recommendation.sku_group GROUP BY 1, 2, 3, 4, 5, 6 ) as subquery WHERE rn = 1 ORDER BY sku_group_pricing ) p ON m.sku_group = p.sku_group_pricing ORDER BY m.availability_rank DESC, p.mrp DESC;",
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
                        