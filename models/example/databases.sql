{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.order_taking_adherence AS select tt.*, dd.delivery_date, dd.order_price_amount, dd.sold_quantity from ( select cast(timestampadd(minute, 660, soh.date) as date) soh_date, soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.secondaryTrip_id, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status, dsrd.status as retailer_demand_status, dsrd.action_status as retailer_demand_action_status, dsrds.product_demand_replacement_quantity, dsrds.product_replacement_quantity, dsrds.product_demand_return_quantity, dsrds.product_return_quantity, dsrds.product_demand_sold_quantity, dsrds.product_sold_quantity, dsrds.product_id, rr.code as retailer_name, rr.area_classification, concat(pp.sku_count,pp.short_name) sku, pp.SKU_Count from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on soh.secondaryTrip_id=sst.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemand dsrd on dsrd.trip_id = sst.id and dsrd.retailer_id = soh.retailer_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemandsku dsrds on dsrd.id=dsrds.retailerDemand_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = dsrds.product_id where soh.activity_status in (\'Sale\',\'Payment\') and soh.secondaryTrip_id is not null ) tt left join ( select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.trip_id, ool.quantity sold_quantity, ool.product_id, concat(pp.sku_count,pp.short_name) sku, pp.sku_count, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ool on oo.id=ool.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id where lower(oo.status) = \'created\' )dd on tt.retailer_id=dd.retailer_id and tt.secondaryTrip_id=dd.trip_id and tt.product_id=dd.product_id;",
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
                        