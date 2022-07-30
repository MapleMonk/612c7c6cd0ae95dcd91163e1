{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.ub_log as select x.*,avg(d.\"EGGS PER RATE\") as avg_cp_mtd, sum(eggs_sold) over (partition by area_classification, delivery_Date) as eggs_sold_area_level from ( SELECT oo.name, ot.egg_type ,cast(timestampadd(minute,660,oo.delivery_date ) as date) as deliveryDate , rr.area_classification , ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id as order_id , ot.single_sku_rate*ot.quantity as amount , oo.delivery_date , oo.generation_date , concat(pp.sku_count,pp.name) as SKU , case when rr.area_classification = \'Bangalore-UB\' then \'Banglore\' when rr.area_classification = \'NCR-UB\' then \'NCR\' when rr.area_classification = \'MP-UB\' then \'M.P\' when rr.area_classification = \'UP-UB\' then \'U.P\' when rr.area_classification = \'East-UB\' then \'Bihar\' end as Regions , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'Nutra\' when lower(pp.name) like \'%liquid%\' then \'Melted\' end as Category , pp.SKU_Count , oo.order_brand_type , oo.secondary_status , case when egg_type = \'Chatki\' then \'2\' else avg(\"EGGS PER RATE\") end as avg_cp , case when ot.egg_type = \'Melted\' then (ot.quantity*1000)/40 else ot.quantity * pp.SKU_Count end as eggs_sold FROM eggozdb.maplemonk.my_sql_order_orderline ot, eggozdb.maplemonk.my_sql_order_order oo, eggozdb.maplemonk.my_sql_product_product pp , eggozdb.maplemonk.my_sql_retailer_retailer rr, eggozdb.maplemonk.region_wise_procurement_masterdata r where ot.order_id = oo.id and ot.product_id = pp.id and rr.id = oo.retailer_id and secondary_status <> \'cancel_approved\' and rr.area_classification like \'%UB%\' and status <> \'cancelled\' AND deliveryDate = r.GRN_Date and category = r.Type and Regions = r.Region group by oo.name, ot.egg_type ,deliveryDate , rr.area_classification , ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id , ot.single_sku_rate*ot.quantity , oo.delivery_date , oo.generation_date , pp.SKU_Count , oo.order_brand_type , oo.secondary_status , pp.name ) x join eggozdb.maplemonk.region_wise_procurement_masterdata d ON Regions = d.region and category = d.type group by eggs_sold, area_classification, delivery_date, name,egg_type, deliveryDate, quantity, single_sku_rate, single_sku_mrp, order_id, x.amount, x.generation_date, sku,regions, category, sku_count, order_brand_type, secondary_status, avg_cp ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        