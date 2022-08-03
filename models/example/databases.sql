{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.ub_log as select *, sum(eggs_sold) over (partition by area_classification, delivery_Date) as eggs_sold_area_level from ( SELECT oo.name ,cast(timestampadd(minute,660,oo.delivery_date ) as date) as deliveryDate ,ot.egg_type , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'Nutra\' when lower(pp.name) like \'%liquid%\' then \'White\' end as Category , case when rr.area_classification = \'Bangalore-UB\' then \'Banglore\' when rr.area_classification = \'NCR-UB\' then \'NCR\' when rr.area_classification = \'MP-UB\' then \'M.P\' when rr.area_classification = \'UP-UB\' then \'U.P\' when rr.area_classification = \'East-UB\' then \'Bihar\' end as Regions , rr.area_classification , case when egg_type = \'Chatki\' then \'2\' else avg(r.\"EGGS PER RATE\") end as avg_cp , case when egg_type = \'Chatki\' then \'2\' else avg(m.\"EGGS PER RATE\") end as avg_cp_mtd , case when egg_type = \'Chatki\' then \'2\' else avg(v.\"EGGS PER RATE\") end as avg_cp_mtd_region , oo.id as order_id , concat(pp.sku_count,pp.name) as SKU , ot.quantity , case when ot.egg_type = \'Melted\' then (ot.quantity*1000)/40 else ot.quantity * pp.SKU_Count end as eggs_sold , ot.single_sku_rate , ot.single_sku_mrp , ot.single_sku_rate*ot.quantity as amount , oo.delivery_date , oo.generation_date , pp.SKU_Count , oo.order_brand_type , oo.secondary_status FROM eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ot on ot.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on ot.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.region_wise_procurement_masterdata r on deliveryDate = r.GRN_date and category = r.Type and Regions = r.region left join eggozdb.maplemonk.region_wise_procurement_masterdata m on category = m.Type and Regions = m.region left join eggozdb.maplemonk.region_wise_procurement_masterdata v on Regions = v.region where secondary_status <> \'cancel_approved\' and rr.area_classification like \'%UB%\' and lower(status) in (\'delivered\',\'completed\') group by oo.name, ot.egg_type ,deliveryDate , rr.area_classification , ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id , ot.single_sku_rate*ot.quantity , oo.delivery_date , oo.generation_date , pp.SKU_Count , oo.order_brand_type , oo.secondary_status , pp.name )",
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
                        