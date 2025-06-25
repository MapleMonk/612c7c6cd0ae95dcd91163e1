{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.discount_percentage_marketplace_split_eoq as with time_filter as ( select sku_group, inward_date, ros_hit_date from snitch_db.maplemonk.original_ros_str_eoq ), discount as ( select sku_group, date, sum(case when type = \'Marketplace\' then gross_quantity end) as marketplace_quant, sum(gross_quantity) as total_quant, sum(discount_amount) as discount_amount, sum(discount_amount+gross_sales) as sales_before_discount from snitch_db.maplemonk.horizontal_sales_categories group by 1,2 ) SELECT d.sku_group, round(SUM(d.marketplace_quant)/SUM(d.total_quant),2) as marketplace_split, round(SUM(d.discount_amount)/SUM(d.sales_before_discount),2) as discount_percentage FROM discount d JOIN time_filter tf ON d.sku_group = tf.sku_group WHERE d.date BETWEEN tf.inward_date AND tf.ros_hit_date GROUP BY d.sku_group,tf.inward_date ;",
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
            