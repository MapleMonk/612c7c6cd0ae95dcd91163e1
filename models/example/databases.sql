{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.epm_data as select c.state as procurement_region, ffm.id as farmer_id, ffm.necc_zone_id, x.* from ( select ff.farm_name, ff.farmer_id, cast(timestampadd(minute, 660, pp.po_date) as date ) as bill_date, cast(timestampadd(minute, 660, peg.date) as date ) as grn_date, cast(timestampadd(minute, 660, pe.start_time) as date ) as processing_date, peg.batch_id, pb.egg_type, sum(peg.egg_tray*30) as procured_eggs,avg(pb.actual_egg_price) as procured_price, sum(pe.egg_count*30) as processed_eggs, (sum(pe.egg_loss) + sum(pe.blood_spot_loss) + sum(pe.short_loss)) as loss, sum(pe.egg_count*30) - (sum(pe.egg_chatki)+sum(pe.egg_hairline)+sum(pe.egg_dirty)+sum(pe.egg_small)+sum(pe.damaged_loss)+sum(pe.egg_loss)+sum(pe.egg_air_gap)+sum(pe.short_loss)+sum(pe.blood_spot_loss)+sum(color_spot_loss)) as clean_eggs, sum(pe.egg_chatki) as egg_chatki, sum(pe.egg_hairline) as egg_hairline, sum(pe.egg_dirty) as egg_dirty, sum(pe.egg_small) as egg_small, sum(pe.damaged_loss) as damaged_loss, sum(pe.egg_loss) as cleaning_loss, sum(pe.blood_spot_loss) as blood_spot_loss, sum(pe.short_loss) as short_loss, (sum(pe.egg_chatki)+sum(pe.egg_hairline)+sum(pe.egg_small)+sum(pe.egg_dirty)+sum(damaged_loss)) as ub from eggozdb.maplemonk.my_sql_procurement_eggcleaning pe join eggozdb.maplemonk.my_sql_procurement_batchmodel pb on pe.batch_id = pb.id join eggozdb.maplemonk.my_sql_procurement_procurement pp on pp.id = pb.procurement_id join eggozdb.maplemonk.my_sql_farmer_farm ff on ff.id = pp.farm_id join eggozdb.maplemonk.my_sql_procurement_eggsin peg on pb.id = peg.batch_id group by processing_date, ff.farm_name, ff.farmer_id, pb.egg_type, pp.po_date, peg.date,peg.batch_id ) x left join eggozdb.maplemonk.my_sql_farmer_farmer ffm on x.farmer_id = ffm.id left join eggozdb.maplemonk.my_sql_base_city c on ffm.necc_zone_id = c.id ;",
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
                        