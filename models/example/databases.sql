{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.order_taking_adherence AS select soh.id,rr.code,rr.id as retailer_id,sst.id as secondarytrip_id,cast(timestampadd(minute, 660, soh.created_at) as date) as date,soh.activity_status, dso.id as order_id,dso.name,dso.status,cast(timestampadd(minute, 660, dso.delivery_date) as date) as order_date,dso.order_price_amount, dso.scheme_discount_amount,dso.invoice_due,dso.deviated_amount, dsol.quantity,dsol.single_sku_rate,dsol.single_sku_discount, dsol.quantity*dsol.single_sku_rate-dsol.quantity*dsol.single_sku_discount as Single_SKU_price,concat(pp.sku_count,pp.short_name) SKU,dso.trip_id, sku.product_sold_quantity,sku.product_demand_sold_quantity from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on soh.secondaryTrip_id=sst.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemand dsrd on sst.id=dsrd.trip_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemandsku sku on dsrd.id=sku.retailerDemand_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder dso on sst.id=dso.trip_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline dsol on dso.id=dsol.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on dso.retailer_id=rr.id left join eggozdb.maplemonk.my_sql_product_product pp on dsol.product_id=pp.id where sku.product_id=dsol.product_id group by dsol.id,dso.id,dsol.product_id, soh.id,rr.code,rr.id,sst.id,cast(timestampadd(minute, 660, soh.created_at) as date),soh.activity_status,soh.secondaryTrip_id, dso.id,dso.name,dso.status,cast(timestampadd(minute, 660, dso.delivery_date) as date),dso.order_price_amount, dso.scheme_discount_amount,dso.invoice_due,dso.deviated_amount,dsol.single_sku_rate,dsol.single_sku_discount, dsol.single_sku_rate,dsol.single_sku_discount,dso.trip_id,dsol.quantity,concat(pp.sku_count,pp.short_name),sku.product_sold_quantity,sku.product_demand_sold_quantity ;",
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
                        