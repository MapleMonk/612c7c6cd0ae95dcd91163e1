{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.NPD_sales AS select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, rr.area_classification, rr.code retailer_name, rp.name as Parent_name, rr.beat_number, rr.category_id, case when pp.productSubDivision_id = 42 then \'Frozen\' else \'EAZY EGGS\' end as EGG_TYPE,srd.beatAssignment_id as operational_beat, oo.id order_id, oo.name invoice, orl.line_type,orl.quantity as return_quantity,srd.product_damage_quantity,srd.product_id as salesdemandsku_product_id, ol.product_id, concat(pp.sku_count, pp.short_name) SKU, pp.short_name, pp.description, pp.name as product_name, pp.slug, pp.SKU_Count, ol.quantity, ol.single_sku_rate, ol.single_sku_discount, ol.single_sku_mrp,ol.single_sku_tax, ol.quantity*ol.single_sku_rate+ol.quantity*ol.single_sku_tax as sku_order_price_amount, oo.order_price_amount, max(cast(timestampadd(minute, 660, oo.delivery_date) as date)) over (partition by rr.code) as last_order_date from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ol on oo.id=ol.order_id left join eggozdb.maplemonk.my_sql_product_product pp on ol.product_id=pp.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id=rr.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rp on rr.parent_id=rp.id left join eggozdb.maplemonk.my_sql_saleschain_salesdemandsku srd on oo.beat_assignment_id=srd.beatassignment_id left join eggozdb.maplemonk.my_sql_order_orderreturnline orl on ol.id=orl.orderline_id where pp.productSubDivision_id in (42,45) and oo.status in (\'completed\',\'delivered\') ;",
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
                        