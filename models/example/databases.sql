{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table pomme_db.maplemonk.Inventory_summary_pomme as with last90_days as ( select sku ,max(color_general) color_general ,max(color_specific) color_specific ,max(product_brand) product_brand ,max(attribute_1) attribute_1 ,max(attribute_2) attribute_2 ,max(attribute_3) attribute_3 ,max(supplier) supplier ,max(size) size ,max(gender) gender ,max(type) type ,max(grip) grip ,max(productname) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,max(model) model ,sum(quantity_sold) as sub_quantity from (select * from pomme_db.maplemonk.ongoing_fact_items where ordeR_Date < \'2024-08-28\' union all select * from pomme_db.maplemonk.mintsoft_fact_items where order_date >= \'2024-08-28\' ) a left join ( select distinct inventorydate, articlenumber, numberofitems inventory from pomme_db.maplemonk.ongoing_historical_inventory_with_date where numberofitems <=1 union all select distinct stockdate inventorydate, sku articlenumber, totalstocklevel inventory from pomme_db.maplemonk.mintsoft_pomme_prod_stock_levels where totalstocklevel <=1 ) b on a.sku = b.articlenumber and a.ordeR_date::DAte = b.inventorydate::Date where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-90,current_timestamp()::date) and b.inventorydate is null group by 1 ), last30_days as ( select sku ,max(color_general) color_general ,max(color_specific) color_specific ,max(product_brand) product_brand ,max(attribute_1) attribute_1 ,max(attribute_2) attribute_2 ,max(attribute_3) attribute_3 ,max(supplier) supplier ,max(size) size ,max(gender) gender ,max(type) type ,max(grip) grip ,max(productname) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,max(model) model ,sum(quantity_sold) as sub_quantity from (select * from pomme_db.maplemonk.ongoing_fact_items where ordeR_Date < \'2024-08-28\' union all select * from pomme_db.maplemonk.mintsoft_fact_items where order_date >= \'2024-08-28\' )a left join ( select distinct inventorydate, articlenumber, numberofitems inventory from pomme_db.maplemonk.ongoing_historical_inventory_with_date where numberofitems <=1 union all select distinct stockdate inventorydate, sku articlenumber, totalstocklevel inventory from pomme_db.maplemonk.mintsoft_pomme_prod_stock_levels where totalstocklevel <=1 ) b on a.sku = b.articlenumber and a.ordeR_date::DAte = b.inventorydate::Date where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-30,current_timestamp()::date) and b.inventorydate is null group by 1 ), last7_days as( select sku ,max(color_general) color_general ,max(color_specific) color_specific ,max(product_brand) product_brand ,max(attribute_1) attribute_1 ,max(attribute_2) attribute_2 ,max(attribute_3) attribute_3 ,max(supplier) supplier ,max(size) size ,max(gender) gender ,max(type) type ,max(grip) grip ,max(productname) product_short_name ,max(print) print ,max(category1) category1 ,max(category2) category2 ,max(model) model ,sum(quantity_sold) as sub_quantity from ( select * from pomme_db.maplemonk.ongoing_fact_items where ordeR_Date < \'2024-08-28\' union all select * from pomme_db.maplemonk.mintsoft_fact_items where order_date >= \'2024-08-28\' )a left join ( select distinct inventorydate, articlenumber, numberofitems inventory from pomme_db.maplemonk.ongoing_historical_inventory_with_date where numberofitems <=1 union all select distinct stockdate inventorydate, sku articlenumber, totalstocklevel inventory from pomme_db.maplemonk.mintsoft_pomme_prod_stock_levels where totalstocklevel <=1 ) b on a.sku = b.articlenumber and a.ordeR_date::DAte = b.inventorydate::Date where order_date::date <= current_timestamp()::date and order_date::date >= dateadd(\'day\',-7,current_timestamp()::date) and b.inventorydate is null group by 1 ) select p.date Inventory_last_updated_at ,p.articlenumber as SKU ,Q.name PRODUCT_NAME ,a.color_general ,a.color_specific ,a.product_brand ,a.attribute_1 ,a.attribute_2 ,a.attribute_3 ,a.supplier ,a.size ,a.gender ,a.type ,a.grip ,a.product_short_name ,a.print ,a.category1 ,a.category2 ,a.model ,ifnull(sum(a.sub_quantity),0) as units_sold_L90 ,ifnull(sum(b.sub_quantity),0) as units_sold_L30 ,ifnull(sum(c.sub_quantity),0) as units_sold_L7 ,sum(p.inventory) as inventory_available ,sum(Q.COSTPRICE*p.inventory) stock_value ,sum(d.daily_average) daily_average ,div0(sum(a.sub_quantity),90) last_90_day_average ,div0(sum(b.sub_quantity),30) last_30_day_average ,div0(sum(c.sub_quantity),7) last_7_day_average ,case when sum(a.sub_quantity) =0 then 0 else round(inventory_available/((sum(a.sub_quantity) /90)*7),2) end as Weeks_of_Supply_L90 ,case when sum(b.sub_quantity)=0 then 0 else round(inventory_available/((sum(b.sub_quantity) /30)*7),2) end as Weeks_of_Supply_L30 ,case when sum(c.sub_quantity)=0 then 0 else round(inventory_available/((sum(c.sub_quantity) /7)*7),2) end as Weeks_of_Supply_L7 from ( select m.* from (select stockdate::date date, null as product_name, productid::string as productcode, sku articlenumber, null as articlesystemid, totalstocklevel inventory, null as cogs from pomme_db.maplemonk.mintsoft_pomme_prod_stock_levels) m left join (select max(stockdate) date from pomme_db.maplemonk.mintsoft_pomme_prod_stock_levels) n on m.date = n.date where n.date is not null ) p left join ( select sku, name, COSTPRICE from ( select sku, name, COSTPRICE, row_number() over (partition by sku order by 1) rw from pomme_db.maplemonk.mintsoft_pomme_prod_products )where rw=1 )q on p.articlenumber = q.sku left join pomme_db.maplemonk.product_attributes_amiko pa on p.articlenumber = pa.sku left join (select distinct sku, grip from pomme_db.maplemonk.woocommerce_all_orders_products_attributes where grip is not null) grip on grip.sku = p.articlenumber left join last90_days a on a.sku = p.articlenumber left join last30_days b on a.sku = b.sku left join last7_days c on a.sku = c.sku left join (select sku ,div0(sum(suborder_quantity), getdate()::date - min(order_date::Date)) daily_average from pomme_db.maplemonk.sales_consolidated_pomme group by 1 ) d on a.sku = d.sku group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 order by 1 desc;",
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
            