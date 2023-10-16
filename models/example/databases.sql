{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.order_taking_adherence AS select tt.*, dd.delivery_date,dd.beat_number as opertaions_beat, dd.order_price_amount, dd.sold_quantity,dd.management_status,dd.name as Sales_person_name from ( select cast(timestampadd(minute, 660, soh.date) as date) soh_date, soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.secondaryTrip_id, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number as beat_number_operations_demand, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status, dsrd.status as retailer_demand_status,dsrd.retailer_status as retailer_demand_action_status, dsrds.product_demand_replacement_quantity, dsrds.product_replacement_quantity, dsrds.product_demand_return_quantity, dsrds.product_return_quantity, dsrds.product_demand_sold_quantity, dsrds.product_sold_quantity, dsrds.product_id, rr.code as retailer_name, rr.area_classification, concat(pp.sku_count,pp.short_name) sku, pp.SKU_Count from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on soh.secondaryTrip_id=sst.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemand dsrd on dsrd.trip_id = sst.id and dsrd.retailer_id = soh.retailer_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemandsku dsrds on dsrd.id=dsrds.retailerDemand_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = dsrds.product_id where soh.activity_status in (\'Sale\',\'Payment\') and soh.secondaryTrip_id is not null ) tt left join ( select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.trip_id,sp.management_status,cu.name,sst2.beat_number as beat_number_operations, ool.quantity sold_quantity, ool.product_id, concat(pp.sku_count,pp.short_name) sku, pp.sku_count, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst2 on oo.trip_id=sst2.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ool on oo.id=ool.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id where lower(oo.status) = \'created\' )dd on tt.retailer_id=dd.retailer_id and tt.secondaryTrip_id=dd.trip_id and tt.product_id=dd.product_id ; CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_order_taking AS select tt.*, dd.delivery_date,dd.beat_number as opertaions_beat, dd.order_id, dd.order_price_amount, dd.sold_quantity,dd.management_status,dd.status, dd.name as Sales_person_name from ( select cast(timestampadd(minute, 660, soh.date) as date) soh_date,soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.secondaryTrip_id,sst.id, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number as beat_number_operations_demand, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status, dsrd.status as retailer_demand_status,dsrd.retailer_status as retailer_demand_action_status, dsrds.product_demand_replacement_quantity, dsrds.product_replacement_quantity, dsrds.product_demand_return_quantity, dsrds.product_return_quantity, dsrds.product_demand_sold_quantity, dsrds.product_sold_quantity, dsrds.product_id, rr.code as retailer_name, rr.area_classification, concat(pp.sku_count,pp.short_name) sku, pp.SKU_Count from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment sst on soh.beatassignment_id=sst.id left join eggozdb.maplemonk.my_sql_saleschain_retailerdemand dsrd on dsrd.beatassignment_id = sst.id and soh.retailer_id=dsrd.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_retailerdemandsku dsrds on dsrd.id=dsrds.retailerDemand_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = dsrds.product_id where lower(soh.activity_status) in (\'sale\',\'payment\') ) tt left join ( select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.beat_assignment_id,sp.management_status,cu.name,sst2.beat_number as beat_number_operations, ool.quantity sold_quantity, ool.product_id, concat(pp.sku_count,pp.short_name) sku, pp.sku_count, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment sst2 on sst2.id=oo.beat_assignment_id left join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id=ool.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id )dd on tt.retailer_id=dd.retailer_id and tt.beatassignment_id=dd.beat_assignment_id and tt.product_id=dd.product_id where lower(dd.status) in (\'completed\',\'delivered\',\'draft\') ;",
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
                        