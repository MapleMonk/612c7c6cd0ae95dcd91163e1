{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.epm_data as select batch.batch_id, batch.zone_name as procurement_region, batch.warehouse, batch.farm_name, batch.bill_date, eggsin.grn_date, cleaning.processing_date, batch.egg_type, eggsin.procured_eggs, cleaning.processed_eggs, batch.procured_price, cleaning.processed_tray, eggsin.procured_tray, (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) as loss, (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub) as ub, (eggsin.procured_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) - (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub)) as branded_eggs, (cleaning.processed_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss)-(eggsin.eggsin_ub + cleaning.cleaning_ub)) as cleaned_eggs, (eggsin.eggsin_egg_chatki + cleaning.cleaning_egg_chatki + package.package_egg_chatki) as egg_chatki, eggsin.eggsin_egg_chatki as procurement_chatki, cleaning.cleaning_egg_chatki as cleaning_chatki, package.package_egg_chatki as package_chatki, (eggsin.eggsin_egg_hairline + cleaning.cleaning_egg_hairline + package.package_egg_hairline) as egg_hairline, (eggsin.eggsin_egg_dirty + cleaning.cleaning_egg_dirty + package.package_egg_dirty) as egg_dirty, (eggsin.eggsin_egg_small + cleaning.cleaning_egg_small + package.package_egg_small) as egg_small, (eggsin.eggsin_egg_air_gap + cleaning.cleaning_egg_air_gap + package.package_egg_air_gap) as air_gap, (eggsin.eggsin_egg_very_dirty + cleaning.cleaning_egg_very_dirty + package.package_egg_very_dirty) as very_dirty, (eggsin.eggsin_egg_good + cleaning.cleaning_egg_good + package.package_egg_good) as good, (eggsin.eggsin_egg_melted + cleaning.cleaning_egg_melted + package.package_egg_melted) as melted, (eggsin.eggsin_egg_blood_spot + cleaning.cleaning_egg_blood_spot + package.package_egg_blood_spot) as blood_spot, (eggsin.eggsin_egg_loss + eggsin.eggsin_egg_missing + cleaning.cleaning_egg_loss + package.package_egg_loss) as general_loss, eggsin_loss, eggsin.transit_loss, cleaning.cleaning_loss, eggsin.eggsin_ub, cleaning.cleaning_ub, package.package_ub, cleaning.cleaning_damaged_loss as damaged_loss from ( select pb.id as batch_id, bz.zone_name, ff.farm_name, ww.name as warehouse, cast(timestampadd(minute,660,pp.po_date) as date) bill_date, pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price as procured_price from eggozdb.maplemonk.my_sql_procurement_procurement pp left join eggozdb.maplemonk.my_sql_farmer_farm ff on pp.farm_id = ff.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = pp.warehouse_id left join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pp.id = pb.procurement_id join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id group by pb.id, bz.zone_name, ff.farm_name, cast(timestampadd(minute,660,pp.po_date) as date), pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price, ww.name ) batch Left join ( select batch_id as cleaning_batch_id, cast(timestampadd(minute, 660, start_time) as date) processing_date, (sum(egg_loss) + sum(blood_spot_loss) + sum(short_loss) + sum(black_spot_loss) + sum(color_spot_loss)) as cleaning_loss, (sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(damaged_loss) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot)) as cleaning_ub, sum(egg_count * 30) AS processed_eggs, sum(egg_count) as processed_tray, sum(egg_chatki) as cleaning_egg_chatki, sum(egg_loss) as cleaning_egg_loss, sum(black_spot_loss) as cleaning_black_spot_loss, sum(blood_spot_loss) as cleaning_blood_spot_loss, sum(damaged_loss) as cleaning_damaged_loss, sum(color_spot_loss) as cleaning_color_spot_loss, sum(egg_air_gap) as cleaning_egg_air_gap, sum(egg_dirty) as cleaning_egg_dirty, sum(egg_hairline) as cleaning_egg_hairline, sum(short_loss) as cleaning_short_loss, sum(egg_blood_spot) as cleaning_egg_blood_spot, sum(egg_good) as cleaning_egg_good, sum(egg_melted) as cleaning_egg_melted, sum(egg_small) as cleaning_egg_small, sum(egg_very_dirty) as cleaning_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggcleaning group by batch_id, cast(timestampadd(minute, 660, start_time) as date) ) cleaning on batch.batch_id = cleaning.cleaning_batch_id join ( select batch_id eggsin_batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) AS grn_date, sum(egg_tray*30)-sum(egg_missing)-Sum(egg_chatki) as procured_eggs, sum(egg_tray) as procured_tray, sum(egg_loss) + sum(egg_missing) as eggsin_loss, sum(egg_loss) + sum(egg_missing) + sum(egg_chatki) as transit_loss, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as eggsin_ub, Sum(egg_chatki) as eggsin_egg_chatki, sum(egg_loss) as eggsin_egg_loss, sum(egg_missing) as eggsin_egg_missing, sum(egg_air_gap) as eggsin_egg_air_gap, sum(egg_blood_spot) as eggsin_egg_blood_spot, sum(egg_dirty) as eggsin_egg_dirty, sum(egg_good) as eggsin_egg_good, sum(egg_hairline) as eggsin_egg_hairline, sum(egg_melted) as eggsin_egg_melted, sum(egg_small) as eggsin_egg_small, sum(egg_very_dirty) as eggsin_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggsin group by batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) ) eggsin on batch.batch_id = eggsin.eggsin_batch_id Left join ( select batch_id package_batch_id, sum(package_count) as package_count, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as package_ub, sum(egg_chatki) as package_egg_chatki, sum(egg_loss) as package_loss, sum(egg_loss) as package_egg_loss, sum(egg_air_gap) as package_egg_air_gap, sum(egg_blood_spot) as package_egg_blood_spot, sum(egg_dirty) as package_egg_dirty, sum(egg_good) as package_egg_good, sum(egg_hairline) as package_egg_hairline, sum(egg_melted) as package_egg_melted, sum(egg_small) as package_egg_small, sum(egg_very_dirty) as package_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_package pp group by batch_id ) package on batch.batch_id = package.package_batch_id ; create or replace table eggozdb.maplemonk.epm_data_bangalore as select batch.batch_id, batch.zone_name as procurement_region, batch.farm_name, eggsin.grn_date, batch.bill_date, cleaning.processing_date, batch.egg_type, eggsin.procured_eggs, cleaning.processed_eggs, batch.procured_price, (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) as loss, (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub) as ub, (eggsin.procured_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) - (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub)) as branded_eggs, (cleaning.processed_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss)-(eggsin.eggsin_ub + cleaning.cleaning_ub)) as cleaned_eggs, (eggsin.eggsin_egg_chatki + cleaning.cleaning_egg_chatki + package.package_egg_chatki) as egg_chatki, cleaning.cleaning_egg_chatki as cleaning_chatki, package.package_egg_chatki as package_chatki, (eggsin.eggsin_egg_hairline + cleaning.cleaning_egg_hairline + package.package_egg_hairline) as egg_hairline, (eggsin.eggsin_egg_dirty + cleaning.cleaning_egg_dirty + package.package_egg_dirty) as egg_dirty, (eggsin.eggsin_egg_small + cleaning.cleaning_egg_small + package.package_egg_small) as egg_small, (eggsin.eggsin_egg_air_gap + cleaning.cleaning_egg_air_gap + package.package_egg_air_gap) as air_gap, (eggsin.eggsin_egg_very_dirty + cleaning.cleaning_egg_very_dirty + package.package_egg_very_dirty) as very_dirty, (eggsin.eggsin_egg_good + cleaning.cleaning_egg_good + package.package_egg_good) as good, (eggsin.eggsin_egg_melted + cleaning.cleaning_egg_melted + package.package_egg_melted) as melted, (eggsin.eggsin_egg_blood_spot + cleaning.cleaning_egg_blood_spot + package.package_egg_blood_spot) as blood_spot, eggsin.transit_loss, cleaning.cleaning_loss, (eggsin.eggsin_egg_loss + eggsin.eggsin_egg_missing + cleaning.cleaning_egg_loss + package.package_egg_loss) as general_loss, eggsin_loss, cleaning.cleaning_ub, cleaning.cleaning_damaged_loss as damaged_loss, package.package_ub, package.package_loss from ( select pb.id as batch_id, bz.zone_name, ff.farm_name, ww.name as warehouse, cast(timestampadd(minute,330,pp.po_date) as date) bill_date, pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price as procured_price from eggozdb.maplemonk.my_sql_procurement_procurement pp left join eggozdb.maplemonk.my_sql_farmer_farm ff on pp.farm_id = ff.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = pp.warehouse_id left join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pp.id = pb.procurement_id join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id group by pb.id, bz.zone_name, ff.farm_name, cast(timestampadd(minute,330,pp.po_date) as date), pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price, ww.name ) batch Left join ( select batch_id as cleaning_batch_id, cast(timestampadd(minute, 330, start_time) as date) processing_date, (sum(egg_loss) + sum(blood_spot_loss) + sum(short_loss) + sum(black_spot_loss) + sum(color_spot_loss)) as cleaning_loss, (sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(damaged_loss) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot)) as cleaning_ub, sum(egg_count * 30) AS processed_eggs, sum(egg_count) as processed_tray, sum(egg_chatki) as cleaning_egg_chatki, sum(egg_loss) as cleaning_egg_loss, sum(black_spot_loss) as cleaning_black_spot_loss, sum(blood_spot_loss) as cleaning_blood_spot_loss, sum(damaged_loss) as cleaning_damaged_loss, sum(color_spot_loss) as cleaning_color_spot_loss, sum(egg_air_gap) as cleaning_egg_air_gap, sum(egg_dirty) as cleaning_egg_dirty, sum(egg_hairline) as cleaning_egg_hairline, sum(short_loss) as cleaning_short_loss, sum(egg_blood_spot) as cleaning_egg_blood_spot, sum(egg_good) as cleaning_egg_good, sum(egg_melted) as cleaning_egg_melted, sum(egg_small) as cleaning_egg_small, sum(egg_very_dirty) as cleaning_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggcleaning group by batch_id , start_time ) cleaning on batch.batch_id = cleaning.cleaning_batch_id join ( select batch_id eggsin_batch_id, CAST(TIMESTAMPADD(MINUTE, 330, date) AS DATE) AS grn_date, sum(egg_tray*30)-sum(egg_missing)-Sum(egg_chatki) as procured_eggs, sum(egg_tray) as procured_tray, sum(egg_loss) + sum(egg_missing) as eggsin_loss, sum(egg_loss) + sum(egg_missing) + sum(egg_chatki) as transit_loss, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as eggsin_ub, Sum(egg_chatki) as eggsin_egg_chatki, sum(egg_loss) as eggsin_egg_loss, sum(egg_missing) as eggsin_egg_missing, sum(egg_air_gap) as eggsin_egg_air_gap, sum(egg_blood_spot) as eggsin_egg_blood_spot, sum(egg_dirty) as eggsin_egg_dirty, sum(egg_good) as eggsin_egg_good, sum(egg_hairline) as eggsin_egg_hairline, sum(egg_melted) as eggsin_egg_melted, sum(egg_small) as eggsin_egg_small, sum(egg_very_dirty) as eggsin_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggsin group by batch_id, CAST(TIMESTAMPADD(MINUTE, 330, date) AS DATE) ) eggsin on batch.batch_id = eggsin.eggsin_batch_id Left join ( select batch_id package_batch_id, sum(package_count) as package_count, sum(egg_loss) as package_loss, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as package_ub, sum(egg_chatki) as package_egg_chatki, sum(egg_loss) as package_egg_loss, sum(egg_air_gap) as package_egg_air_gap, sum(egg_blood_spot) as package_egg_blood_spot, sum(egg_dirty) as package_egg_dirty, sum(egg_good) as package_egg_good, sum(egg_hairline) as package_egg_hairline, sum(egg_melted) as package_egg_melted, sum(egg_small) as package_egg_small, sum(egg_very_dirty) as package_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_package pp group by batch_id ) package on batch.batch_id = package.package_batch_id where batch.zone_name = \'Bangalore\' ; create or replace table eggozdb.maplemonk.batch_data as select pb.id as batch_id, bz.zone_name, ff.farm_name, ww.name as warehouse, cast(timestampadd(minute,660,pp.po_date) as date) bill_date, pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price as procured_price from eggozdb.maplemonk.my_sql_procurement_procurement pp left join eggozdb.maplemonk.my_sql_farmer_farm ff on pp.farm_id = ff.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = pp.warehouse_id left join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pp.id = pb.procurement_id join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id group by pb.id, bz.zone_name, ff.farm_name, cast(timestampadd(minute,660,pp.po_date) as date), pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price, ww.name ; create or replace table eggozdb.maplemonk.processing_data as select bd.*, tt.* from eggozdb.maplemonk.batch_data bd left join ( select batch_id as processing_batch_id, cast(timestampadd(minute, 660, start_time) as date) processing_date, sum(egg_chatki) egg_chatki, sum(egg_loss) eggs_loss, sum(egg_count) egg_count, sum(black_spot_loss) black_spot_loss, sum(blood_spot_loss) blood_spot_loss, sum(damaged_loss) damaged_loss, sum(color_spot_loss) color_spot_loss, sum(egg_air_gap) egg_air_gap, sum(egg_dirty) egg_dirty, sum(egg_hairline) egg_hairline, sum(short_loss) short_loss, sum(egg_blood_spot) egg_blood_spot, sum(egg_good) egg_good, sum(egg_melted) egg_melted, sum(egg_small) egg_small, sum(egg_very_dirty) egg_very_dirty, sum(egg_shape_size) egg_shape_size, sum(no_of_eggs) no_of_eggs, sum(processing_loss) processing_loss, sum(egg_other) egg_other, sum(egg_other_ub) egg_other_ub, sum(egg_pid) egg_pid, sum(egg_rtv) egg_rtv from eggozdb.maplemonk.my_sql_procurement_eggcleaning group by batch_id, cast(timestampadd(minute, 660, start_time) as date) ) tt on tt.processing_batch_id = bd.batch_id where tt.processing_batch_id is not null ; create or replace table eggozdb.maplemonk.procurement_data as select bd.*, tt.* from eggozdb.maplemonk.batch_data bd left join ( select batch_id procurement_batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) AS grn_date, sum(egg_chatki) egg_chatki, sum(egg_tray) egg_tray, sum(egg_loss) egg_loss, sum(egg_missing) egg_missing, sum(egg_air_gap) egg_air_gap, sum(egg_blood_spot) egg_blood_spot, sum(egg_dirty) egg_dirty, sum(egg_good) egg_good, sum(egg_hairline) egg_hairline, sum(egg_melted) egg_melted, sum(egg_small) egg_small, sum(egg_very_dirty) egg_very_dirty, sum(egg_pid) egg_pid, sum(egg_handling_loss) egg_handling_loss from eggozdb.maplemonk.my_sql_procurement_eggsin group by batch_id, cast(timestampadd(minute, 660, date) as date) ) tt on tt.procurement_batch_id = bd.batch_id where tt.procurement_batch_id is not null ; create or replace table eggozdb.maplemonk.packaging_data as select bd.*, tt.* from eggozdb.maplemonk.batch_data bd left join ( select t1.batch_id package_batch_id, cast(timestampadd(minute, 660, t1.start_time) as date) packaging_date, sum(t1.egg_chatki) egg_chatki, sum(t1.egg_loss) egg_loss, sum(t1.package_count*pp.sku_count) packaged_eggs, sum(t1.egg_air_gap) egg_air_gap, sum(t1.egg_blood_spot) egg_blood_spot, sum(t1.egg_dirty) egg_dirty, sum(t1.egg_good) egg_good, sum(t1.egg_hairline) egg_hairline, sum(t1.egg_melted) egg_melted, sum(t1.egg_small) egg_small, sum(t1.egg_very_dirty) egg_very_dirty, sum(t1.egg_pid) egg_pid, sum(t1.egg_shape_size) egg_shape_size, sum(t1.eggs_used) eggs_used from eggozdb.maplemonk.my_sql_procurement_package t1 left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t1.product_id group by t1.batch_id, cast(timestampadd(minute, 660, t1.start_time) as date) ) tt on tt.package_batch_id = bd.batch_id where tt.package_batch_id is not null ; create or replace table eggozdb.maplemonk.batch_procurement_processing_packaging as select bd.*, tt1.*, tt2.*, tt3.* from eggozdb.maplemonk.batch_data bd left join ( select batch_id procurement_batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) AS grn_date, sum(egg_chatki) procurement_egg_chatki, sum(egg_tray) procurement_egg_tray, sum(egg_loss) procurement_egg_loss, sum(egg_missing) procurement_egg_missing, sum(egg_air_gap) procurement_egg_air_gap, sum(egg_blood_spot) procurement_egg_blood_spot, sum(egg_dirty) procurement_egg_dirty, sum(egg_good) procurement_egg_good, sum(egg_hairline) procurement_egg_hairline, sum(egg_melted) procurement_egg_melted, sum(egg_small) procurement_egg_small, sum(egg_very_dirty) procurement_egg_very_dirty, sum(egg_pid) procurement_egg_pid, sum(egg_handling_loss) procurement_egg_handling_loss, sum(egg_loss+egg_missing+egg_handling_loss) total_procurement_loss, sum(egg_dirty+egg_chatki+egg_air_gap+egg_blood_spot+egg_hairline+egg_melted+egg_small+egg_very_dirty+egg_pid) total_procurement_ub from eggozdb.maplemonk.my_sql_procurement_eggsin group by batch_id, cast(timestampadd(minute, 660, date) as date) ) tt1 on tt1.procurement_batch_id = bd.batch_id left join ( select batch_id as processing_batch_id, min(cast(timestampadd(minute, 660, start_time) as date)) processing_date, sum(egg_chatki) processing_egg_chatki, sum(egg_loss) processing_egg_loss, sum(egg_count) processing_egg_count, sum(black_spot_loss) processing_black_spot_loss, sum(blood_spot_loss) processing_blood_spot_loss, sum(damaged_loss) processing_damaged_loss, sum(color_spot_loss) processing_color_spot_loss, sum(egg_air_gap) processing_egg_air_gap, sum(egg_dirty) processing_egg_dirty, sum(egg_hairline) processing_egg_hairline, sum(short_loss) processing_short_loss, sum(egg_blood_spot) processing_egg_blood_spot, sum(egg_good) processing_egg_good, sum(egg_melted) processing_egg_melted, sum(egg_small) processing_egg_small, sum(egg_very_dirty) processing_egg_very_dirty, sum(egg_shape_size) processing_egg_shape_size, sum(no_of_eggs) processing_no_of_eggs, sum(processing_loss) processing_processing_loss, sum(egg_other) processing_egg_other, sum(egg_other_ub) processing_egg_other_ub, sum(egg_pid) processing_egg_pid, sum(egg_rtv) processing_egg_rtv, sum(egg_hairline+egg_chatki+egg_dirty+egg_small+egg_pid+egg_air_gap+egg_blood_spot+egg_melted+egg_very_dirty+egg_shape_size+egg_other+egg_other_ub) total_processing_ub, sum(blood_spot_loss+damaged_loss+processing_loss+short_loss+color_spot_loss+egg_loss+black_spot_loss) total_processing_loss from eggozdb.maplemonk.my_sql_procurement_eggcleaning group by batch_id ) tt2 on tt2.processing_batch_id = bd.batch_id left join ( select t1.batch_id package_batch_id, min(cast(timestampadd(minute, 660, t1.start_time) as date)) packaging_date, sum(t1.egg_chatki) packaging_egg_chatki, sum(t1.egg_loss) packaging_egg_loss, sum(t1.package_count*ifnull(pp.sku_count,0)) packaging_packaged_eggs, sum(t1.egg_air_gap) packaging_egg_air_gap, sum(t1.egg_blood_spot) packaging_egg_blood_spot, sum(t1.egg_dirty) packaging_egg_dirty, sum(t1.egg_good) packaging_egg_good, sum(t1.egg_hairline) packaging_egg_hairline, sum(t1.egg_melted) packaging_egg_melted, sum(t1.egg_small) packaging_egg_small, sum(t1.egg_very_dirty) packaging_egg_very_dirty, sum(t1.egg_pid) packaging_egg_pid, sum(t1.egg_shape_size) packaging_egg_shape_size, sum(t1.eggs_used) packaging_eggs_used, sum(t1.egg_loss) total_packaging_loss, sum(t1.egg_chatki+t1.egg_dirty+t1.egg_air_gap+t1.egg_blood_spot+t1.egg_hairline+t1.egg_melted+t1.egg_small+t1.egg_very_dirty+t1.egg_shape_size+t1.egg_pid) total_packaging_ub from eggozdb.maplemonk.my_sql_procurement_package t1 left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t1.product_id group by t1.batch_id ) tt3 on tt3.package_batch_id = bd.batch_id ;",
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
                        