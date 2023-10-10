{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select soh.id,rr.code,cast(timestampadd(minute, 660, soh.created_at) as date) as date,soh.activity_status,soh.secondaryTrip_id, dso.id as order_id,dso.name,dso.status,cast(timestampadd(minute, 660, dso.delivery_date) as date) as order_date,dso.order_price_amount, dso.scheme_discount_amount,dso.invoice_due,dso.deviated_amount, dsol.quantity,dsol.single_sku_rate,dsol.single_sku_discount, dsol.quantity*dsol.single_sku_rate-dsol.quantity*dsol.single_sku_discount as SKU_price,concat(pp.sku_count,pp.short_name) SKU_Price,dso.trip_id from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder dso on soh.secondaryTrip_id=dso.trip_id and soh.retailer_id=dso.retailer_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline dsol on dso.id=dsol.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on soh.retailer_id=rr.id left join eggozdb.maplemonk.my_sql_product_product pp on dsol.product_id=pp.id where rr.id is not null group by soh.id,dso.id,dsol.product_id, soh.id,rr.code,cast(timestampadd(minute, 660, soh.created_at) as date),soh.activity_status,soh.secondaryTrip_id, dso.id,dso.name,dso.status,cast(timestampadd(minute, 660, dso.delivery_date) as date),dso.order_price_amount, dso.scheme_discount_amount,dso.invoice_due,dso.deviated_amount, dsol.quantity,dsol.single_sku_rate,dsol.single_sku_discount, dsol.quantity*dsol.single_sku_rate-dsol.quantity*dsol.single_sku_discount,concat(pp.sku_count,pp.short_name),dso.trip_id;",
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
                        