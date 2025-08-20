{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.perfumes_eoq as with inv as ( select sku_group, product_name as product_name, sum(online_inventory+offline_inventory+offline_jit_inventory) as total_inv from snitch_db.maplemonk.category_journey where sku_group like \'4MSFR%\' group by 1, 2 ), sales as ( select sku_group, (sum(inventory_on_inward_date*ros)/sum(inventory_on_inward_date))::int as ros from snitch_db.maplemonk.original_ros_str_eoq where sku_group like \'4MSFR%\' group by 1 ), ros as ( select sku_group, (sum(gross_quantity)/30)::int as ros from snitch_db.maplemonk.horizontal_sales_categories where date >= current_date-30 group by 1 ) select a.*, coalesce(b.ros,c.ros) as ros from inv a left join sales b on a.sku_group = b.sku_group left join ros c on a.sku_group = c.sku_group where PRODUCT_NAME != \'null\'",
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
            