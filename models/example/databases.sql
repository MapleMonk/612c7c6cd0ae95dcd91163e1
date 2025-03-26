{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ras_inventory_sales as with sales_data as ( select product_name , sku , sum(quantity) as Total_Ordered_L6M , sum(quantity)/180 as ROS , count (distinct order_date) as Days_of_Sale_L6M , sum(return_flag * ifnull(returned_quantity,0)) as Total_Returned_Quantity_L6M from `MAPLEMONK.ras_sales_consolidated` where DATE_DIFF(CURRENT_DATE(), order_timestamp, DAY) <= 180 and product_name is not null and sku is not null group by product_name,SKU ) select i.* , ifnull(s.Total_Ordered_L6M,0) as Total_Ordered_L6M , ifnull(s.ROS,0) as ROS_L6M , ifnull(s.Days_of_Sale_L6M,0) as Days_of_Sale_L6M , ifnull(s.Total_Returned_Quantity_L6M,0) as Total_Returned_Quantity_L6M from `MAPLEMONK.ras_inventory_fact_items` i left join sales_data s on i.sku = s.sku and i.product_name = s.product_name;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            