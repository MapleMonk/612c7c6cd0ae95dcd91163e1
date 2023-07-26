{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Inventory_summary_snitch as with last180_days as ( select a.sku_group ,c.product_name ,category ,collection ,b.first_order_date::date first_order_date ,dateadd(month,1,b.first_order_date::date) First_order_date_plus_1month ,sum(quantity) as quantity ,max(case when order_timestamp::date < dateadd(month,1,b.first_order_date::date) then order_timestamp::date end ) max_sale_date ,datediff(day,b.first_order_date::date, max_sale_Date) days ,sum(case when order_timestamp::date < dateadd(month,1,b.first_order_date::date) then ifnull(quantity,0) end) quantity_in_first_month from snitch_db.maplemonk.fact_items_snitch a left join (select sku_group, min(order_timestamp) first_order_date from snitch_db.maplemonk.fact_items_snitch group by 1) b on a.sku_group = b.sku_group left join ( select sku_group, product_name from ( select sku_group, product_name, row_number() over (partition by sku_group order by order_timestamp desc ) rw from snitch_db.maplemonk.fact_items_snitch ) where rw = 1 ) c on a.sku_group = c.sku_group where order_timestamp::date <= current_timestamp()::date and order_timestamp::date >= dateadd(\'day\',-180,current_timestamp()::date) and lower(order_status) <> \'cancelled\' group by 1,2,3,4,5 ) ,last90_days as ( select sku_group ,sum(quantity) as quantity from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date <= current_timestamp()::date and order_timestamp::date >= dateadd(\'day\',-90,current_timestamp()::date) and lower(order_status) <> \'cancelled\' group by 1 ) ,last30_days as ( select sku_group ,sum(quantity) as quantity from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date <= current_timestamp()::date and order_timestamp::date >= dateadd(\'day\',-30,current_timestamp()::date) and lower(order_status) <> \'cancelled\' group by 1 ), last7_days as( select sku_group ,sum(quantity) as quantity from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date <= current_timestamp()::date and order_timestamp::date >= dateadd(\'day\',-7,current_timestamp()::date) and lower(order_status) <> \'cancelled\' group by 1 ), last3_days as( select sku_group ,sum(quantity) as quantity from snitch_db.maplemonk.fact_items_snitch where order_timestamp::date <= current_timestamp()::date and order_timestamp::date >= dateadd(\'day\',-3,current_timestamp()::date) and lower(order_status) <> \'cancelled\' group by 1 ), inventory as( select case when right(\"Item Type skuCode\",2) = \'-S\' then left(\"Item Type skuCode\",len(\"Item Type skuCode\")-2) else replace(\"Item Type skuCode\",concat(\'-\',split_part(\"Item Type skuCode\",\'-\',-1)),\'\') end sku_group, count(distinct \"Item Code\") inventory_quantity from snitch_db.maplemonk.unicommerce_inventory_aging group by 1 ), sales_from_beginning_by_days as( select a.sku_group, sum(case when datediff(day, first_order_date, order_timestamp::date) <= 2 then quantity end) sales_first_3_days, sum(case when datediff(day, first_order_date, order_timestamp::date) <= 6 then quantity end) sales_first_7_days, sum(case when datediff(day, first_order_date, order_timestamp::date) <= 29 then quantity end) sales_first_30_days, sum(case when datediff(day, first_order_date, order_timestamp::date) <= 89 then quantity end) sales_first_90_days, sum(case when datediff(day, first_order_date, order_timestamp::date) <= 179 then quantity end) sales_first_180_days from snitch_db.maplemonk.fact_items_snitch a left join (select sku_group, min(order_timestamp::date) first_order_date from snitch_db.maplemonk.fact_items_snitch group by 1) b on a.sku_group = b.sku_group where lower(a.order_status) <> \'cancelled\' group by 1 ) select null as Inventory_last_updated_at, min(first_order_Date) first_order_date ,z.SKU_group ,z.product_name product_name ,category ,collection ,max(ifnull(z.days,0)) days ,sum(ifnull(z.quantity_in_first_month,0)) quantity_in_firsT_month ,sum(ifnull(z.quantity,0)) as units_sold_L180 ,sum(ifnull(a.quantity,0)) as units_sold_L90 ,sum(ifnull(b.quantity,0)) as units_sold_L30 ,sum(ifnull(c.quantity,0)) as units_sold_L7 ,sum(ifnull(d.quantity,0)) as units_sold_L3 ,sum(inventory_quantity) as inventory_available ,case when sum(ifnull(a.quantity,0))=0 then 0 else round(sum(inventory_quantity)/((sum(a.quantity) /90)*7),2) end as Weeks_of_Supply_L90 ,case when sum(ifnull(b.quantity,0))=0 then 0 else round(sum(inventory_quantity)/((sum(b.quantity) /30)*7),2) end as Weeks_of_Supply_L30 ,case when sum(ifnull(c.quantity,0))=0 then 0 else round(sum(inventory_quantity)/((sum(c.quantity) /7)*7),2) end as Weeks_of_Supply_L7 ,sum(sales_first_3_days) sales_first_3_days ,sum(sales_first_7_days) sales_first_7_days ,sum(sales_first_30_days) sales_first_30_days ,sum(sales_first_90_days) sales_first_90_days ,sum(sales_first_180_days) sales_first_180_days from last180_days z left join last90_days a on z.sku_group = a.sku_group left join inventory i on z.sku_group = i.sku_group left join last30_days b on z.sku_group = b.sku_group left join last7_days c on z.sku_group = c.sku_group left join last3_days d on z.sku_group = d.sku_group left join sales_from_beginning_by_days e on z.sku_group = e.sku_group group by 3,4,5,6 order by 1 desc;",
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
                        