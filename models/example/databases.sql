{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table pomme_db.maplemonk.Inventory_summary_pomme as with last90_days as ( select sku ,sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-90,current_timestamp()::date) group by 1 ) ,last30_days as ( select sku ,sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-30,current_timestamp()::date) group by 1 ),last7_days as( select sku, sum(suborder_quantity) as sub_quantity from pomme_db.maplemonk.sales_consolidated_pomme where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-7,current_timestamp()::date) group by 1 ) select p.date Inventory_last_updated_at ,a.sku as SKU ,p.product_name PRODUCT_NAME ,ifnull(sum(a.sub_quantity),0) as units_sold_L90 ,ifnull(sum(b.sub_quantity),0) as units_sold_L30 ,ifnull(sum(c.sub_quantity),0) as units_sold_L7 ,sum(p.inventory) as inventory_available ,sum(p.cogs*p.inventory) stock_value ,case when sum(a.sub_quantity) =0 then 0 else round(inventory_available/((sum(a.sub_quantity) /90)*7),2) end as Weeks_of_Supply_L90 ,case when sum(b.sub_quantity)=0 then 0 else round(inventory_available/((sum(b.sub_quantity) /30)*7),2) end as Weeks_of_Supply_L30 ,case when sum(c.sub_quantity)=0 then 0 else round(inventory_available/((sum(c.sub_quantity) /7)*7),2) end as Weeks_of_Supply_L7 from last90_days a left join (select replace(inventoryinfo:lastInDate,\'\"\',\'\')::date date, articlename product_name, productcode, articlenumber, articlesystemid, inventoryinfo:numberOfItems inventory, stockvaluationprice cogs from pomme_db.maplemonk.ongoing_articles) p on a.sku = p.articlenumber left join last30_days b on a.sku = b.sku left join last7_days c on a.sku = c.sku group by 1,2,3 order by 1 desc;",
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
                        