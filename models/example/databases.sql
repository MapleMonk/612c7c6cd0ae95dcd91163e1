{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table pomme_db.maplemonk.Inventory_summary_pomme as with last90_days as ( select sku ,max(color) color ,max(size) size ,max(grip) grip ,max(product_short_name) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-90,current_timestamp()::date) group by 1 ) ,last30_days as ( select sku ,max(color) color ,max(size) size ,max(grip) grip ,max(product_short_name) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-30,current_timestamp()::date) group by 1 ),last7_days as( select sku ,max(color) color ,max(size) size ,max(grip) grip ,max(product_short_name) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-7,current_timestamp()::date) group by 1 ) select p.date Inventory_last_updated_at ,a.sku as SKU ,p.product_name PRODUCT_NAME ,a.color ,a.size ,a.grip ,a.product_short_name ,a.print ,a.category1 ,a.category2 ,ifnull(sum(a.sub_quantity),0) as units_sold_L90 ,ifnull(sum(b.sub_quantity),0) as units_sold_L30 ,ifnull(sum(c.sub_quantity),0) as units_sold_L7 ,sum(p.inventory) as inventory_available ,sum(p.cogs*p.inventory) stock_value ,sum(d.daily_average) daily_average ,case when sum(a.sub_quantity) =0 then 0 else round(inventory_available/((sum(a.sub_quantity) /90)*7),2) end as Weeks_of_Supply_L90 ,case when sum(b.sub_quantity)=0 then 0 else round(inventory_available/((sum(b.sub_quantity) /30)*7),2) end as Weeks_of_Supply_L30 ,case when sum(c.sub_quantity)=0 then 0 else round(inventory_available/((sum(c.sub_quantity) /7)*7),2) end as Weeks_of_Supply_L7 from last90_days a left join (select replace(inventoryinfo:lastInDate,\'\"\',\'\')::date date, articlename product_name, productcode, articlenumber, articlesystemid, inventoryinfo:numberOfItems inventory, stockvaluationprice cogs from pomme_db.maplemonk.ongoing_articles) p on a.sku = p.articlenumber left join last30_days b on a.sku = b.sku left join last7_days c on a.sku = c.sku left join (select sku ,sum(suborder_quantity)/ count(distinct order_date::date) daily_average from pomme_db.maplemonk.sales_consolidated_pomme where sku in (select sku from pomme_db.maplemonk.Inventory_summary_pomme where inventory_available =0) group by 1 ) d on a.sku = d.sku group by 1,2,3,4,5,6,7,8,9,10 order by 1 desc;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Pomme_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        