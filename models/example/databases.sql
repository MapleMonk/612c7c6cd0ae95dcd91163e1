{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.ras_inventory_sales as with sales_data_l6m as ( select product_name , sku , avg(GROSS_SALES_BEFORE_TAX) as gross_sales , avg(DISCOUNT) as discount , avg(SELLING_PRICE) as selling_price , sum(quantity) as Total_Ordered_L6M , sum(quantity)/180 as ROS , count (distinct order_date) as Days_of_Sale_L6M , sum(return_flag * ifnull(returned_quantity,0)) as Total_Returned_Quantity_L6M from `MAPLEMONK.ras_sales_consolidated` where DATE_DIFF(CURRENT_DATE(), order_timestamp, DAY) <= 180 and product_name is not null and sku is not null group by product_name,SKU ), sales_data_l3m as ( select product_name , sku , sum(quantity) as Total_Ordered_L3M , sum(quantity)/180 as ROS_L3M , count (distinct order_date) as Days_of_Sale_L3M , sum(return_flag * ifnull(returned_quantity,0)) as Total_Returned_Quantity_L3M from `MAPLEMONK.ras_sales_consolidated` where DATE_DIFF(CURRENT_DATE(), order_timestamp, DAY) <= 90 and product_name is not null and sku is not null group by product_name,SKU ) select i.* , s6.gross_sales , s6.discount , s6.selling_price , ifnull(s6.Total_Ordered_L6M,0) as Total_Ordered_L6M , ifnull(s6.ROS,0) as ROS_L6M , ifnull(s6.Days_of_Sale_L6M,0) as Days_of_Sale_L6M , ifnull(s6.Total_Returned_Quantity_L6M,0) as Total_Returned_Quantity_L6M , ifnull(s3.Total_Ordered_L3M,0) as Total_Ordered_L3M , ifnull(s3.ROS_L3M,0) as ROS_L3M , ifnull(s3.Days_of_Sale_L3M,0) as Days_of_Sale_L3M , ifnull(s3.Total_Returned_Quantity_L3M,0) as Total_Returned_Quantity_L3M from `MAPLEMONK.ras_inventory_fact_items` i left join sales_data_l6m s6 on i.sku = s6.sku and i.product_name = s6.product_name left join sales_data_l3m s3 on i.sku = s3.sku and i.product_name = s3.product_name;",
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
            