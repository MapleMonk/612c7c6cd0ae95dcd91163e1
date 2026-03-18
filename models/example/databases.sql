{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.atovio_INVENTORY_FACT_ITEMS_blinkit as with blinkit_sales_7d as ( select item_id, sum(abs(cast(quantity as int))) as sale_l7 from Maplemonk.blinkit_atovio_sales_Fact_Items where ordeR_Date between (select max(cast(order_date as date)) -6 from Maplemonk.blinkit_atovio_sales_Fact_Items) and (select max(cast(order_date as date)) from Maplemonk.blinkit_atovio_sales_Fact_Items) group by 1 ), blinkit_sales_30d as ( select item_id, sum(abs(cast(quantity as int))) as sale_l30 from Maplemonk.blinkit_atovio_sales_Fact_Items where ordeR_Date between (select max(cast(order_date as date)) -29 from Maplemonk.blinkit_atovio_sales_Fact_Items) and (select max(cast(order_date as date)) from Maplemonk.blinkit_atovio_sales_Fact_Items) group by 1 ), blinkit_inventory as ( select cast(created_date as date) date, item_id, sum(cast(Total_sellable as int)) as inventory from Maplemonk.atovio_blinkit_seller_inventory group by 1,2 ) select a.date, a.item_id, a.inventory, b.sale_l7, c.sale_l30 from blinkit_inventory a left join blinkit_sales_7d b on a.item_id = b.item_id left join blinkit_sales_30d c on a.item_id = c.item_id ;",
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
            