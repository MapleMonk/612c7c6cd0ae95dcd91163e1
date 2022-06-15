{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.ub_log as SELECT ot.egg_type ,cast(timestampadd(minute,330,oo.delivery_date ) as date) as deliveryDate, rr.area_classification, ot.quantity , ot.single_sku_rate , ot.single_sku_mrp , oo.id as order_id , oo.order_price_amount , oo.delivery_date, oo.generation_date, concat(pp.sku_count,pp.name) as SKU , pp.SKU_Count , oo.order_brand_type , oo.secondary_status FROM eggozdb.maplemonk.my_sql_order_orderline ot, eggozdb.maplemonk.my_sql_order_order oo, eggozdb.maplemonk.my_sql_product_product pp , eggozdb.maplemonk.my_sql_retailer_retailer rr where ot.order_id = oo.id and ot.product_id = pp.id and rr.id = oo.retailer_id and secondary_status <> \'cancel_approved\';",
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
                        