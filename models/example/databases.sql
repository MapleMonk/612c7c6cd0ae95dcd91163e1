{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.city_wise_orders as with orders_fact as ( select * from ( select order_name, pincode, district, ROW_NUMBER() OVER (PARTITION BY order_name ORDER BY pincode DESC, RANDOM()) AS rn from snitch_db.snitch.orders_fact ) where rn=1 ), line_items as (select order_name, sku, sku_group from snitch_db.snitch.order_lineitems_fact ), product_details as ( select * from ( select sku_group, handle, product_title, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY handle DESC, RANDOM()) AS rn from snitch_db.snitch.product_dim ) where rn=1 ) select district, line_items.sku_group, handle, product_title, trim(RIGHT(product_title, CHARINDEX(\' \', REVERSE(product_title)) - 1)) AS category_extract, count(line_items.sku_group) as number_orders from line_items left join orders_fact on orders_fact.order_name=line_items.order_name left join product_details on line_items.sku_group=product_details.sku_group group by district,line_items.sku_group,handle,product_title order by district,number_orders desc",
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
                        