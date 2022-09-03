{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.epm_data as select batch.batch_id, batch.zone_name as procurement_region, batch.warehouse, batch.farm_name, batch.bill_date, eggsin.grn_date, cleaning.processing_date, batch.egg_type, eggsin.procured_eggs, cleaning.processed_eggs, batch.procured_price, cleaning.processed_tray, eggsin.procured_tray, (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) as loss, (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub) as ub, (eggsin.procured_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss + package.package_loss) - (eggsin.eggsin_ub + cleaning.cleaning_ub + package.package_ub)) as branded_eggs, (cleaning.processed_eggs - (eggsin.eggsin_loss + cleaning.cleaning_loss)-(eggsin.eggsin_ub + cleaning.cleaning_ub)) as cleaned_eggs, (eggsin.eggsin_egg_chatki + cleaning.cleaning_egg_chatki + package.package_egg_chatki) as egg_chatki, eggsin.eggsin_egg_chatki as procurement_chatki, cleaning.cleaning_egg_chatki as cleaning_chatki, package.package_egg_chatki as package_chatki, (eggsin.eggsin_egg_hairline + cleaning.cleaning_egg_hairline + package.package_egg_hairline) as egg_hairline, (eggsin.eggsin_egg_dirty + cleaning.cleaning_egg_dirty + package.package_egg_dirty) as egg_dirty, (eggsin.eggsin_egg_small + cleaning.cleaning_egg_small + package.package_egg_small) as egg_small, (eggsin.eggsin_egg_air_gap + cleaning.cleaning_egg_air_gap + package.package_egg_air_gap) as air_gap, (eggsin.eggsin_egg_very_dirty + cleaning.cleaning_egg_very_dirty + package.package_egg_very_dirty) as very_dirty, (eggsin.eggsin_egg_good + cleaning.cleaning_egg_good + package.package_egg_good) as good, (eggsin.eggsin_egg_melted + cleaning.cleaning_egg_melted + package.package_egg_melted) as melted, (eggsin.eggsin_egg_blood_spot + cleaning.cleaning_egg_blood_spot + package.package_egg_blood_spot) as blood_spot, (eggsin.eggsin_egg_loss + eggsin.eggsin_egg_missing + cleaning.cleaning_egg_loss + package.package_egg_loss) as general_loss, eggsin_loss, eggsin.transit_loss, cleaning.cleaning_loss, package.package_loss, eggsin.eggsin_ub, cleaning.cleaning_ub, package.package_ub, cleaning.cleaning_damaged_loss as damaged_loss from ( select pb.id as batch_id, bz.zone_name, ff.farm_name, ww.name as warehouse, cast(timestampadd(minute,660,pp.po_date) as date) bill_date, pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price as procured_price from eggozdb.maplemonk.my_sql_procurement_procurement pp left join eggozdb.maplemonk.my_sql_farmer_farm ff on pp.farm_id = ff.id left join eggozdb.maplemonk.my_sql_warehouse_warehouse ww on ww.id = pp.warehouse_id left join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pp.id = pb.procurement_id join eggozdb.maplemonk.my_sql_base_zone bz on bz.id = ww.zone_id group by pb.id, bz.zone_name, ff.farm_name, cast(timestampadd(minute,660,pp.po_date) as date), pb.type, pb.batch_status, pb.egg_type, pb.actual_egg_price, ww.name ) batch Left join ( select batch_id as cleaning_batch_id, CAST(TIMESTAMPADD(MINUTE, 660, start_time) AS DATE) AS processing_date, (sum(egg_loss) + sum(blood_spot_loss) + sum(short_loss) + sum(black_spot_loss) + sum(color_spot_loss)) as cleaning_loss, (sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(damaged_loss) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot)) as cleaning_ub, sum(egg_count * 30) AS processed_eggs, sum(egg_count) as processed_tray, sum(egg_chatki) as cleaning_egg_chatki, sum(egg_loss) as cleaning_egg_loss, sum(black_spot_loss) as cleaning_black_spot_loss, sum(blood_spot_loss) as cleaning_blood_spot_loss, sum(damaged_loss) as cleaning_damaged_loss, sum(color_spot_loss) as cleaning_color_spot_loss, sum(egg_air_gap) as cleaning_egg_air_gap, sum(egg_dirty) as cleaning_egg_dirty, sum(egg_hairline) as cleaning_egg_hairline, sum(short_loss) as cleaning_short_loss, sum(egg_blood_spot) as cleaning_egg_blood_spot, sum(egg_good) as cleaning_egg_good, sum(egg_melted) as cleaning_egg_melted, sum(egg_small) as cleaning_egg_small, sum(egg_very_dirty) as cleaning_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggcleaning group by batch_id, CAST(TIMESTAMPADD(MINUTE, 660, start_time) AS DATE) ) cleaning on batch.batch_id = cleaning.cleaning_batch_id join ( select batch_id eggsin_batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) AS grn_date, sum(egg_tray*30)-sum(egg_missing)-Sum(egg_chatki) as procured_eggs, sum(egg_tray) as procured_tray, sum(egg_loss) + sum(egg_missing) as eggsin_loss, sum(egg_loss) + sum(egg_missing) + sum(egg_chatki) as transit_loss, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as eggsin_ub, Sum(egg_chatki) as eggsin_egg_chatki, sum(egg_loss) as eggsin_egg_loss, sum(egg_missing) as eggsin_egg_missing, sum(egg_air_gap) as eggsin_egg_air_gap, sum(egg_blood_spot) as eggsin_egg_blood_spot, sum(egg_dirty) as eggsin_egg_dirty, sum(egg_good) as eggsin_egg_good, sum(egg_hairline) as eggsin_egg_hairline, sum(egg_melted) as eggsin_egg_melted, sum(egg_small) as eggsin_egg_small, sum(egg_very_dirty) as eggsin_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_eggsin group by batch_id, CAST(TIMESTAMPADD(MINUTE, 660, date) AS DATE) ) eggsin on batch.batch_id = eggsin.eggsin_batch_id Left join ( select batch_id package_batch_id, sum(package_count) as package_count, sum(egg_loss) as package_loss, sum(egg_chatki) + sum(egg_hairline) + sum(egg_dirty) + sum(egg_small) + sum(egg_air_gap) + sum(egg_very_dirty) + sum(egg_good) + sum(egg_melted) + sum(egg_blood_spot) as package_ub, sum(egg_chatki) as package_egg_chatki, sum(egg_loss) as package_egg_loss, sum(egg_air_gap) as package_egg_air_gap, sum(egg_blood_spot) as package_egg_blood_spot, sum(egg_dirty) as package_egg_dirty, sum(egg_good) as package_egg_good, sum(egg_hairline) as package_egg_hairline, sum(egg_melted) as package_egg_melted, sum(egg_small) as package_egg_small, sum(egg_very_dirty) as package_egg_very_dirty from eggozdb.maplemonk.my_sql_procurement_package pp group by batch_id ) package on batch.batch_id = package.package_batch_id ;",
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
                        