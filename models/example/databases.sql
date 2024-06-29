{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.first_mile as SELECT pp.id as procurement_id, cast(timestampadd(minute, 660, po_date) as date) as po_date, ff.farm_name, wd.driver_name, wd.driver_no as driver_phone_no, wv.vehicle_no, sum(pb.expected_egg_tray) expected_tray_quantity, sum(pb.actual_egg_tray) actual_tray_quantity, sum(pb.expected_egg_tray*30) expected_egg_quantity, sum(pb.actual_egg_tray*30) actual_egg_quantity, ww.name warehouse, wv.per_day_charge, wv.per_day_distance, wv.per_day_duration, wv.vehicle_identifier_type, pp.procurement_bill_url, pp.extra_egg_trays_charge, pp.additional_charge, pp.loading_charge, pp.packaging_charge FROM eggozdb.maplemonk.my_sql_procurement_procurement pp left join eggozdb.maplemonk.my_sql_farmer_farm ff on ff.id = pp.farm_id left join eggozdb.maplemonk.my_sql_warehouse_driver wd on wd.id = pp.driver_id left join eggozdb.maplemonk.my_sql_warehouse_vehicle wv on wv.id = pp.vehicle_id left join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pb.procurement_id = pp.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = pp.warehouse_id group by pp.id, cast(timestampadd(minute, 660, po_date) as date), ff.farm_name, wd.driver_name, wd.driver_no, wv.vehicle_no, ww.name, wv.per_day_charge, wv.per_day_distance, wv.per_day_duration, pp.procurement_bill_url, pp.extra_egg_trays_charge, pp.additional_charge, pp.loading_charge, pp.packaging_charge, wv.vehicle_identifier_type ; create or replace table eggozdb.maplemonk.mid_mile as SELECT dt.id, cast(timestampadd(minute,660,dt.beat_date) as date) beat_date, cast(timestampadd(minute,660,dt.transferred_at) as time) transferred_at, cast(timestampadd(minute,660,dt.vehicle_assigned_at) as datetime) vehicle_assigned_at, cast(timestampadd(minute,660,dt.received_at) as datetime) received_at, ww1.name transfered_from, ww2.name transfered_to, sum(dts.quantity*pp.SKU_Count) as sent_eggs_ims, sum(dts.receive_quantity*pp.SKU_Count) as received_eggs_ims, wd.driver_name, wv.vehicle_no, wv.per_day_charge, wv.per_day_distance, wv.per_day_duration, wv.vehicle_identifier_type FROM eggozdb.maplemonk.my_sql_distributionchain_tripskutransfer dt right join eggozdb.maplemonk.my_sql_distributionchain_transfersku dts on dt.id = dts.tripSKUTransfer_id left join eggozdb.maplemonk.my_sql_product_product pp on dts.product_id = pp.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww1 on ww1.id = dt.from_warehouse_id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww2 on ww2.id = dt.to_warehouse_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rrb on dt.to_beat_id = rrb.id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rrb2 on dt.from_beat_id = rrb2.id left join eggozdb.maplemonk.my_sql_warehouse_driver wd on wd.id = dt.driver_id left join eggozdb.maplemonk.my_sql_warehouse_vehicle wv on wv.id = dt.vehicle_id where dt.material_type = \'Fresh\' and lower(transfer_status) = \'confirmed\' and dt.transfer_type = \'satellite\' group by dt.id, cast(timestampadd(minute,660,dt.beat_date) as date), cast(timestampadd(minute,660,dt.transferred_at) as time), cast(timestampadd(minute,660,dt.vehicle_assigned_at) as datetime), cast(timestampadd(minute,660,dt.received_at) as datetime), ww1.name, ww2.name, wd.driver_name, wv.vehicle_no, wv.per_day_charge, wv.per_day_distance, wv.per_day_duration, wv.vehicle_identifier_type ; create or replace table eggozdb.maplemonk.last_mile as select cast(timestampadd(minute, 660, db.beat_date) as date) beat_date, db.beat_number, db.beat_status, cast(timestampadd(minute, 660, db.beat_time) as time) beat_time, wd.driver_name, wv.vehicle_no, wv.per_day_charge, wv.per_day_distance, wv.per_day_duration, wv.vehicle_identifier_type, cast(timestampadd(minute, 660, db.beat_expected_time) as time) beat_expected_time, db.beat_material_status, db.priority, ww.name warehouse, db.ODO_in, db.ODO_return, cast(timestampadd(minute, 660, db.in_time) as time) in_time, cast(timestampadd(minute, 660, db.out_time) as time) out_time, cast(timestampadd(minute, 660, db.return_time) as time) return_time, cast(timestampadd(minute, 660, db.sc_in_time) as time) sc_in_time, db.beat_type, db.finance_trip_status, db.warehouse_trip_status, db.demand_classification, db.supply_approval_status, cast(timestampadd(minute, 660, db.cancelled_at) as datetime) cancelled_at, cast(timestampadd(minute, 660, db.vehicle_assigned_at) as datetime) vehicle_assigned_at, cast(timestampadd(minute, 660, db.created_at) as datetime) created_at, cast(timestampadd(minute, 660, db.modified_at) as datetime) modified_at, cast(timestampadd(minute, 660, db.out_date) as date) out_date, cast(timestampadd(minute, 660, db.return_date) as date) return_date, db.beat_brand_type from eggozdb.maplemonk.my_sql_distributionchain_beatassignment db left join eggozdb.maplemonk.my_sql_warehouse_driver wd on wd.id = db.driver_id left join eggozdb.maplemonk.my_sql_warehouse_vehicle wv on wv.id = db.vehicle_id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = db.warehouse_id ; create or replace table eggozdb.maplemonk.mojro_trip_event AS select id, created_at, modified_at, shipments_businesscode, name, route, driver, tripid, orderid, vehicleid, driverphone, vehiclecategoryname, sku, sum(quantity) over (partition by id, orderid, shipments_businesscode, created_at, sku) quantity, get_sku_count_for_sku(sku) eggs, traveltime, traveldistance, actualtraveltime, actualtraveldistance, noofpickups, noofdeliveries, starttime, endtime, actualstarttime, actualendtime, shipments_starttime, shipments_endtime, shipments_visitstarttime, shipments_visitendtime, shipments_actualstarttime, shipments_actualendtime, shipments_location, shipments_doclocation, shipments_actuallocation, status, shipments_type, shipments_status, shipments_dockey, shipments_doctime, shipments_returning_to_pickup, shipments_reason, shipments_reasonCode, shipments_orderReferenceNumber, eventtype, isMarketVehicle, transporteCode, transporterName from (select id, cast(timestampadd(minute, 810, created_at) as datetime) created_at, cast(timestampadd(minute, 810, modified_at) as datetime) modified_at, name, parse_json(data):data:endTz::string AS endTz, parse_json(data):data:route::string AS route, parse_json(data):data:driver::string AS driver, parse_json(data):data:eventtype::string AS eventtype, parse_json(data):data:status::string AS status, parse_json(data):data:tripId::string AS tripId, cast(timestampadd(minute, 0, parse_json(data):data:endTime::string::datetime) as datetime) AS endTime, parse_json(data):data:orderId::string AS orderId, parse_json(data):data:startTz::string AS startTz, cast(timestampadd(minute, 0, parse_json(data):data:startTime::string::datetime) as datetime) AS startTime, parse_json(data):data:vehicleId::string AS vehicleId, parse_json(data):data:travelTime::integer AS travelTime, parse_json(data):data:driverPhone::string AS driverPhone, parse_json(data):data:noOfPickups::integer AS noOfPickups, cast(timestampadd(minute, 0, parse_json(data):data:actualEndTime::string::datetime) as datetime) AS actualEndTime, parse_json(data):data:noOfDeliveries::integer AS noOfDeliveries, parse_json(data):data:travelDistance::integer AS travelDistance, parse_json(data):data:actualStartTime::string::datetime AS actualStartTime, parse_json(data):data:actualTravelTime::integer AS actualTravelTime, parse_json(data):data:vehicleCategoryName::string AS vehicleCategoryName, parse_json(data):data:actualTravelDistance::integer AS actualTravelDistance, parse_json(data):data:isMarketVehicle::string AS isMarketVehicle, parse_json(data):data:transporteCode::string AS transporteCode, parse_json(data):data:transporterName::string AS transporterName, parse_json(b.value):tz::string AS shipments_tz, parse_json(b.value):type::string AS shipments_type, parse_json(b.value):docKey::string AS shipments_docKey, parse_json(b.value):status::string AS shipments_status, parse_json(b.value):returningToPickup::string AS shipments_returning_to_pickup, parse_json(b.value):reason::string AS shipments_reason, parse_json(b.value):reasonCode::string AS shipments_reasonCode, parse_json(b.value):orderReferenceNumber::string AS shipments_orderReferenceNumber, cast(timestampadd(minute, 0, parse_json(b.value):docTime::string::datetime) as datetime) AS shipments_docTime, cast(timestampadd(minute, 0, parse_json(b.value):endTime::string::datetime) as datetime) AS shipments_endTime, parse_json(b.value):location::string AS shipments_location, cast(timestampadd(minute, 0, parse_json(b.value):startTime::string::datetime) as datetime) AS shipments_startTime, parse_json(b.value):docLocation::string AS shipments_docLocation, parse_json(b.value):businessCode::string AS shipments_businessCode, cast(timestampadd(minute, 0, parse_json(b.value):visitEndTime::string::datetime) as datetime) AS shipments_visitEndTime, cast(timestampadd(minute, 0, parse_json(b.value):actualEndTime::string::datetime) as datetime) AS shipments_actualEndTime, parse_json(b.value):actualLocation::string AS shipments_actualLocation, cast(timestampadd(minute, 0, parse_json(b.value):visitStartTime::string::datetime) as datetime) AS shipments_visitStartTime, cast(timestampadd(minute, 0, parse_json(b.value):actualStartTime::string::datetime) as datetime) AS shipments_actualStartTime, case when parse_json(d.value):code::string like \'%_%\' or parse_json(d.value):code::string like \'%:%\' then split_part(split_part(parse_json(d.value):code::string, \'_\', 0), \':\', 0) else parse_json(d.value):code::string end AS sku , parse_json(d.value):qty::integer AS quantity from my_sql_integration_mojrodata, lateral flatten (input => parse_json(data):data:shipments) b, lateral flatten (input => b.value:packageList) c, lateral flatten (input => c.value:packages) d where name = \'Trip event\' qualify dense_rank() over (partition by tripId order by created_at desc)=1 order by created_at desc ) te ; create or replace table eggozdb.maplemonk.mojro_distributionchain_combined_raw as select db_id, beat_type, demand_classification, beat_material_status, beat_date, beat_number, beat_name, beat_brand_type, id as mojro_id, created_at, modified_at, case when shipments_businesscode = \'NOI DC\' then \'Noida DC\' when shipments_businesscode = \'DEL DC\' then \'Delhi DC\' else shipments_businesscode end as shipments_businesscode, name, route, driver, tripid, orderid, vehicleid, driverphone, vehiclecategoryname, sum(get_sku_count_for_sku(sku)*quantity) eggs, traveltime, traveldistance, actualtraveltime, actualtraveldistance, noofpickups, noofdeliveries, shipments_returning_to_pickup, shipments_reason, shipments_reasonCode, shipments_orderReferenceNumber, eventtype, isMarketVehicle, transporteCode, transporterName, starttime::datetime starttime, endtime::datetime endtime, actualstarttime::datetime actualstarttime, actualendtime::datetime actualendtime, shipments_starttime::datetime shipments_starttime, shipments_endtime::datetime shipments_endtime, shipments_visitstarttime::datetime shipments_visitstarttime, shipments_visitendtime::datetime shipments_visitendtime, shipments_actualstarttime::datetime shipments_actualstarttime, shipments_actualendtime::datetime shipments_actualendtime, shipments_location, shipments_doclocation, shipments_actuallocation, status, shipments_type, shipments_status, shipments_dockey, shipments_doctime, timediff(\'minute\',shipments_starttime,coalesce(shipments_visitstarttime,shipments_actualstarttime))/60 in_delay_hours, timediff(\'minute\',shipments_endtime,coalesce(shipments_actualendtime,shipments_visitendtime))/60 out_delay_hours, get_loading_capacity_for_vehicle(vehiclecategoryname) capacity from ( select distinct db.id as db_id, db.beat_type, db.demand_classification, db.beat_material_status, db.beat_date::date beat_date, db.beat_number, db.beat_name, db.beat_brand_type, te.* from my_sql_distributionchain_beatassignment db full outer join mojro_trip_event te on te.orderid = db.mojro_order_id ) tt where year(beat_date)>=2024 or year(shipments_starttime)>=2024 group by db_id, beat_type, demand_classification, beat_material_status, beat_date, beat_number, beat_name, beat_brand_type, id, created_at, modified_at, shipments_businesscode, name, route, driver, tripid, orderid, vehicleid, driverphone, vehiclecategoryname, traveltime, traveldistance, actualtraveltime, actualtraveldistance, noofpickups, noofdeliveries, starttime, endtime, actualstarttime, actualendtime, shipments_starttime, shipments_endtime, shipments_visitstarttime, shipments_visitendtime, shipments_actualstarttime, shipments_actualendtime, shipments_location, shipments_doclocation, shipments_actuallocation, status, shipments_type, shipments_status, shipments_dockey, shipments_doctime, shipments_returning_to_pickup, shipments_reason, shipments_reasonCode, shipments_orderReferenceNumber, eventtype, isMarketVehicle, transporteCode, transporterName ; create or replace table eggozdb.maplemonk.mojro_distributionchain_combined as select case when pp.shipments_businesscode like \'P%\' then \'FM\' when pp.shipments_businesscode like \'%DC\' or pp.shipments_businesscode like \'%IN\' then \'LM\' when pp.shipments_businesscode like \'%EPC\' and dd.shipments_businesscode like \'%DC\' then \'MM\' when pp.shipments_businesscode like \'%EPC\' and LENGTH(REGEXP_REPLACE(dd.shipments_businesscode, \'[^0-9]\', \'\')) = 4 then \'LM\' when dd.shipments_businesscode in (\'ZTG\',\'MBGGN\') then \'LM\' when dd.shipments_businesscode in (\'NDLS\',\'HY111\') then \'MM\' else \'Other\' end as mile, pp.shipments_businesscode plant_code, pp.eggs eggs_pickup, pp.shipments_starttime as pickup_shipments_starttime, pp.shipments_endtime as pickup_shipments_endtime, pp.shipments_visitstarttime pickup_shipments_visitstarttime, pp.shipments_actualstarttime pickup_shipments_actualstarttime, pp.shipments_visitendtime pickup_shipments_visitendtime, pp.shipments_actualendtime pickup_shipments_actualendtime, pp.in_delay_hours pickup_in_delay_hours, pp.out_delay_hours pickup_out_delay_hours, dd.*, concat(date(cast(TIMESTAMPadd(minute, 0,dd.SHIPMENTS_ENDTIME) as datetime)),\' \',get_to_time(dd.BEAT_NAME)) adjusted_drop_endtime, timediff(\'minute\',concat(date(cast(TIMESTAMPadd(minute, 0,dd.SHIPMENTS_ENDTIME) as datetime)),\' \',get_to_time(dd.BEAT_NAME)),coalesce(dd.shipments_actualendtime,dd.shipments_visitendtime))/60 adjusted_out_delay, rr.code as retailer_name, rr.area_classification, rrp.name as parent_name from (select * from mojro_distributionchain_combined_raw where shipments_type = 0) pp left join (select * from mojro_distributionchain_combined_raw where shipments_type = 1) dd on pp.mojro_id = dd.mojro_id left join my_sql_retailer_retailer rr on split_part(rr.code,\'*\',0) = dd.shipments_businesscode left join my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id ;",
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
                        