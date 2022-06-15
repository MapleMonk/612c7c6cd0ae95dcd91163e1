{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.ub_log as select *,sum(eggs_sold) over (partition by area_classification, delivery_Date) as eggs_sold_area_level from ( SELECT ot.egg_type ,cast(timestampadd(minute,660,oo.delivery_date ) as date) as deliveryDate , rr.area_classification , ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id as order_id , oo.order_price_amount , oo.delivery_date , oo.generation_date , concat(pp.sku_count,pp.name) as SKU , case when lower(pp.name) like \'%white%\' then \'White\' when lower(pp.name) like \'%brown%\' then \'Brown\' when lower(pp.name) like \'%nutra%\' then \'Nutra\' when lower(pp.name) like \'%1liquid%\' then \'1Liquid\' end as Category , pp.SKU_Count , oo.order_brand_type , oo.secondary_status ,ot.quantity * pp.SKU_Count as eggs_sold FROM eggozdb.maplemonk.my_sql_order_orderline ot, eggozdb.maplemonk.my_sql_order_order oo, eggozdb.maplemonk.my_sql_product_product pp , eggozdb.maplemonk.my_sql_retailer_retailer rr where ot.order_id = oo.id and ot.product_id = pp.id and rr.id = oo.retailer_id and secondary_status <> \'cancel_approved\' and rr.area_classification like \'%UB%\' );",
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
                        