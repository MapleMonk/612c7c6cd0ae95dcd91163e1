{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.order_taking_adherence_full as select ttdd.*, rr.beat_retailer_count, rr.area_retailer_count from ( select tt.soh_date, tt.retailer_id soh_retailer_id , tt.activity_status soh_activity_status, tt.beatAssignment_id soh_beatAssignment_id, tt.secondaryTrip_id soh_secondaryTrip_id, tt.soh_sales_person, tt.trip_date soh_trip_date, tt.soh_beat_number_operations, tt.beat_status soh_beat_status, tt.demand_classification soh_demand_classification, tt.beat_made_type soh_beat_made_type, tt.retailer_status as soh_activity_status_flow, tt.status as soh_demand_status, tt.code as soh_retailer_name, tt.area_classification soh_area_classification, tt.beat_number as soh_beat_number_original, dd.delivery_date order_delivery_date, dd.order_id, dd.status order_status, dd.order_price_amount, dd.trip_id order_trip_id, dd.management_status order_management_status, dd.order_beat_made_type, dd.order_sales_person, dd.order_beat_number_operations, dd.retailer_id order_retailer_id, dd.order_retailer_name, dd.area_classification order_area_classification, dd.beat_number as order_beat_number_original, coalesce(tt.soh_beat_number_operations,dd.order_beat_number_operations) beat_number_operations, coalesce(tt.beat_number,dd.beat_number) beat_number, coalesce(tt.area_classification,dd.area_classification) area_classification from ( select cast(timestampadd(minute, 660, soh.date) as date) soh_date, soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.secondaryTrip_id, cuu.name as soh_sales_person, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number as soh_beat_number_operations, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status , dsrd.status, rr.code, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile spp on soh.sales_person_profile_id=spp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cuu on spp.user_id=cuu.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on soh.secondaryTrip_id=sst.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemand dsrd on dsrd.trip_id = sst.id and dsrd.retailer_id = soh.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id where lower(soh.activity_status) in (\'sale\',\'payment\',\'replacement\',\'return\') and rr.onboarding_status = \'Active\' and cast(timestampadd(minute, 660, soh.date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) and lower(soh.type) in (\'visit\',\'closing\') and rr.area_classification LIKE ANY (\'%GT\', \'%OF-MT\') ) tt full outer join ( select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.trip_id,sp.management_status,cu.name as order_sales_person ,sst2.beat_number as order_beat_number_operations, ifnull(sst2.beat_made_type,\'without_trip\') as order_beat_made_type, oo.retailer_id, rr.code as order_retailer_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst2 on oo.trip_id=sst2.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id where lower(oo.status) in (\'created\') and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) and rr.onboarding_status = \'Active\' and rr.area_classification LIKE ANY (\'%GT\', \'%OF-MT\') union all select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.beat_assignment_id as trip_id,sp.management_status,cu.name as order_sales_person ,sst2.beat_number as order_beat_number_operations, ifnull(sst2.beat_made_type,\'without_trip\') as order_beat_made_type, oo.retailer_id, rr.code as order_retailer_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment sst2 on oo.beat_assignment_id=sst2.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id where lower(oo.status) in (\'completed\',\'delivered\') and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) and rr.onboarding_status = \'Active\' and rr.area_classification LIKE ANY (\'%GT\', \'%OF-MT\') ) dd on tt.retailer_id=dd.retailer_id and tt.secondaryTrip_id=dd.trip_id ) ttdd left join ( select distinct beat_number, area_classification, count(code) over (partition by area_classification, beat_number) beat_retailer_count, count(code) over (partition by area_classification) area_retailer_count from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' and (area_classification like \'%GT\' or area_classification like \'%OF-MT\') and category_id <> 3 and category_id <> 10 ) rr on ttdd.area_classification = rr.area_classification and coalesce(ttdd.beat_number_operations,ttdd.beat_number) = rr.beat_number ; CREATE OR REPLACE TABLE eggozdb.maplemonk.order_taking_adherence AS select tt.soh_date, coalesce(tt.retailer_id,dd.retailer_id) retailer_id, tt.activity_status, tt.soh_sales_person, tt.soh_beat_number_operations, coalesce(tt.demand_classification,dd.area_classification) area_classification, tt.product_demand_sold_quantity, tt.product_sold_quantity, tt.product_demand_replacement_quantity, tt.product_replacement_quantity, tt.product_demand_return_quantity, tt.product_return_quantity, coalesce(tt.retailer_name,dd.retailer_name) retailer_name, coalesce(tt.beat_number_original,dd.beat_number_original) beat_number_original, coalesce(tt.sku,dd.sku) sku, coalesce(tt.SKU_Count,dd.sku_count) SKU_Count, coalesce(tt.product_id,dd.product_id) product_id, dd.status order_status, dd.delivery_date, dd.order_price_amount, dd.sold_quantity,dd.management_status,dd.delivery_sales_person, dd.delivery_beat_number_operations from ( select cast(timestampadd(minute, 660, soh.date) as date) soh_date, soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.secondaryTrip_id trip_id,cuu.name as soh_sales_person, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number as soh_beat_number_operations, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status, dsrd.status as retailer_demand_status,dsrd.retailer_status as retailer_demand_action_status, dsrds.product_demand_replacement_quantity, dsrds.product_replacement_quantity, dsrds.product_demand_return_quantity, dsrds.product_return_quantity, dsrds.product_demand_sold_quantity, dsrds.product_sold_quantity, dsrds.product_id, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,pp.short_name) sku, pp.SKU_Count from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile spp on soh.sales_person_profile_id=spp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cuu on spp.user_id=cuu.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on soh.secondaryTrip_id=sst.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemand dsrd on dsrd.trip_id = sst.id and dsrd.retailer_id = soh.retailer_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryretailerdemandsku dsrds on dsrd.id=dsrds.retailerDemand_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = dsrds.product_id where lower(soh.activity_status) in (\'sale\',\'payment\',\'replacement\',\'return\') and soh.secondaryTrip_id is not null and cast(timestampadd(minute, 660, soh.date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) union all select cast(timestampadd(minute, 660, soh.date) as date) soh_date, soh.retailer_id, soh.activity_status, soh.beatAssignment_id, soh.beatassignment_id trip_id,cuu.name as soh_sales_person, cast(timestampadd(minute, 660, sst.beat_date) as date) trip_date, sst.beat_number as soh_beat_number_operations, sst.beat_status, sst.demand_classification, sst.beat_made_type, dsrd.retailer_status, dsrd.status as retailer_demand_status,dsrd.retailer_status as retailer_demand_action_status, dsrds.product_demand_replacement_quantity, dsrds.product_replacement_quantity, dsrds.product_demand_return_quantity, dsrds.product_return_quantity, dsrds.product_demand_sold_quantity, dsrds.product_sold_quantity, dsrds.product_id, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,pp.short_name) sku, pp.SKU_Count from eggozdb.maplemonk.my_sql_order_sohmodel soh left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile spp on soh.sales_person_profile_id=spp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cuu on spp.user_id=cuu.id left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment sst on soh.beatassignment_id=sst.id left join eggozdb.maplemonk.my_sql_saleschain_retailerdemand dsrd on dsrd.beatassignment_id = sst.id and dsrd.retailer_id = soh.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_retailerdemandsku dsrds on dsrd.id=dsrds.retailerDemand_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = soh.retailer_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = dsrds.product_id where lower(soh.activity_status) in (\'sale\',\'payment\',\'replacement\',\'return\') and soh.beatassignment_id is not null and cast(timestampadd(minute, 660, soh.date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) ) tt full outer join ( select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.trip_id,sp.management_status,cu.name as delivery_sales_person ,sst2.beat_number as delivery_beat_number_operations, ool.quantity sold_quantity, ool.product_id, concat(pp.sku_count,pp.short_name) sku, pp.sku_count, ool.quantity*(ool.single_sku_rate+ool.single_sku_tax+ool.single_sku_discount) sku_order_price_amount, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst2 on oo.trip_id=sst2.id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ool on oo.id=ool.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id where lower(oo.status) = \'created\' and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) and rr.area_classification LIKE ANY (\'%GT\', \'%OF-MT\') union all select cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.id as order_id, oo.status, oo.order_price_amount, oo.beat_assignment_id trip_id,sp.management_status,cu.name as delivery_sales_person ,sst2.beat_number as delivery_beat_number_operations, ool.quantity sold_quantity, ool.product_id, concat(pp.sku_count,pp.short_name) sku, pp.sku_count, ool.quantity*(ool.single_sku_rate+ool.single_sku_tax+ool.single_sku_discount) sku_order_price_amount, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_distributionchain_beatassignment sst2 on oo.beat_assignment_id=sst2.id left join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id=ool.order_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile sp on oo.salesPerson_id=sp.id left join eggozdb.maplemonk.my_sql_custom_auth_user cu on sp.user_id=cu.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id where lower(oo.status) in (\'completed\',\'delivered\') and cast(timestampadd(minute, 660, oo.delivery_date) as date) >= DATE_TRUNC(\'month\', dateadd(\'month\', -2, current_date())) and rr.area_classification LIKE ANY (\'%GT\', \'%OF-MT\') )dd on tt.retailer_id=dd.retailer_id and tt.trip_id=dd.trip_id and tt.product_id=dd.product_id ; create or replace table eggozdb.maplemonk.sales_in_draft as select cast(timestampadd(minute, 330, oo.delivery_date) as date) delivery_date, oo.name invoice, oo.order_price_amount, rr.code retailer_name, rr.area_classification, rr.beat_number beat_number_original, dss.beat_number beat_number_operational, cau.name as OT_SR, cau2.name as delivery_SR from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip dss on dss.id = oo.trip_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp on ssp.id = oo.salesPerson_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ssp.user_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp2 on ssp2.id = dss.salesRepresentative_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau2 on cau2.id = ssp2.user_id where oo.status = \'draft\' and cast(timestampadd(minute, 660, oo.delivery_date) as date) <= cast(timestampadd(minute, 660, current_date()) as date) ;",
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
                        