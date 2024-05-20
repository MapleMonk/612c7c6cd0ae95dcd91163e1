{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.tertiary_sales as select dist.code as distributor,rrr.code as dealer, tt.* from ( select rr.code as retailer_name,oo.name bill_name,bcc.city_name as city, bz.zone_name as zone, oo.status, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, sum(oo.order_price_amount) order_price_amount, ol.quantity * (ol.single_sku_rate + ol.single_sku_discount+ol.single_sku_tax) sku_sale,sp.id as salesperson_id , cu.name,cu.email, ol.single_sku_rate, ol.single_sku_discount,ol.single_sku_tax, ol.quantity*pp.sku_count AS eggs_sold, concat(pp.sku_count,pp.short_name) sku, pp.slug, ol.quantity, pp.product_type, oo.retailer_id, rr.dealer_id,rr.distributor_id, rr.area_classification, rr.beat_number beat_number_original,dst.beat_number as beat_number_operational, bc.cluster_name, rr.onboarding_status, rr.city_id, rcc.name as retailer_category from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip dst on oo.trip_id = dst.id left join eggozdb.maplemonk.my_sql_tertiary_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ol on ol.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ol.product_id left join eggozdb.maplemonk.my_sql_base_cluster bc on rr.cluster_id = bc.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.SALESPERSON_ID=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id = cu.id left join eggozdb.maplemonk.my_sql_base_city bcc on rr.city_id = bcc.id left join eggozdb.maplemonk.my_sql_base_zone bz on bcc.zone_id = bz.id where lower(oo.status) in(\'created\',\'draft\') and rr.dealer_id is not null group by cast(timestampadd(minute, 660, oo.delivery_date) as date), oo.retailer_id, oo.name, pp.name, pp.short_name, pp.slug, oo.status, ol.quantity, pp.sku_count, pp.product_type,dst.beat_number, rr.area_classification, pp.slug, bcc.city_name , bz.zone_name, rr.code,rr.dealer_id,rr.distributor_id, rr.beat_number, rr.onboarding_status, rr.city_id, ol.single_sku_rate, ol.single_sku_discount,ol.single_sku_tax,sp.id,cu.name,cu.email, bc.cluster_name, rcc.name ) tt left join eggozdb.maplemonk.my_sql_retailer_retailer rrr on tt.dealer_id = rrr.id left join eggozdb.maplemonk.my_sql_retailer_retailer dist on rrr.distributor_id = dist.id",
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
                        