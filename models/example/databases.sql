{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.NPD_sales AS select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, rr.area_classification, rr.code retailer_name, rr.beat_number, rr.category_id, oo.id order_id, oo.name invoice, ol.product_id, concat(pp.sku_count, pp.short_name) SKU, pp.short_name, pp.description, pp.name as product_name, pp.slug, pp.SKU_Count, ol.quantity, ol.single_sku_rate, ol.single_sku_discount, ol.single_sku_mrp, ol.quantity*ol.single_sku_rate as sku_order_price_amount, oo.order_price_amount from order_order oo left join order_orderline ol on oo.id=ol.order_id left join product_product pp on ol.product_id=pp.id left join retailer_retailer rr on oo.retailer_id=rr.id left join distributionchain_beatassignment db on oo.beat_assignment_id=db.id where pp.productSubDivision_id in (42,45) and oo.status in (\'completed\',\'delivered\');",
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
                        