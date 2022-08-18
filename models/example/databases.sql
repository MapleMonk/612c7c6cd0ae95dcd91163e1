{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.epm_data as SELECT z.zone_name AS procurement_region, ww.name AS warehouse, ff.farmer_id, ff.farm_name, pb.egg_type, CAST(TIMESTAMPADD(MINUTE, 330, pp.po_date) AS DATE) AS bill_date, CAST(TIMESTAMPADD(MINUTE, 330, pe.date) AS DATE) AS grn_date, CAST(TIMESTAMPADD(MINUTE, 330, pec.start_time) AS DATE) AS processing_date, pec.batch_id, SUM(pe.egg_tray * 30) AS procured_eggs, AVG(pb.actual_egg_price) AS procured_price, SUM(pec.egg_count * 30) AS processed_eggs, SUM(pec.egg_count * 30) - (SUM(pec.egg_chatki) + SUM(pec.egg_hairline) + SUM(pec.egg_dirty) + SUM(pec.egg_small) + SUM(pec.damaged_loss) + SUM(pec.egg_loss) + SUM(pec.egg_air_gap) + SUM(pec.short_loss) + SUM(pec.blood_spot_loss) + SUM(pec.color_spot_loss)) AS clean_eggs, SUM(pec.egg_chatki) AS egg_chatki, SUM(pec.egg_hairline) AS egg_hairline, SUM(pec.egg_dirty) AS egg_dirty, SUM(pec.egg_small) AS egg_small, SUM(pec.damaged_loss) AS damaged_loss, (SUM(pec.egg_chatki) + SUM(pec.egg_hairline) + SUM(pec.egg_small) + SUM(pec.egg_dirty) + SUM(pec.damaged_loss)) AS ub FROM eggozdb.maplemonk.my_sql_procurement_procurement pp LEFT JOIN eggozdb.maplemonk.my_sql_warehouse_warehouse ww ON ww.id = pp.warehouse_id LEFT JOIN eggozdb.maplemonk.my_sql_farmer_farm ff ON ff.id = pp.farm_id LEFT JOIN eggozdb.maplemonk.my_sql_base_zone z ON ww.zone_id = z.id LEFT JOIN eggozdb.maplemonk.my_sql_procurement_batchmodel pb ON pb.procurement_id = pp.id LEFT JOIN eggozdb.maplemonk.my_sql_procurement_eggsin pe ON pe.batch_id = pb.id LEFT JOIN eggozdb.maplemonk.my_sql_procurement_eggcleaning pec ON pb.id = pec.batch_id GROUP BY z.zone_name , ff.farmer_id , grn_date , pp.po_date , ww.name , ff.farm_name , pb.egg_type, pec.batch_id , pec.start_time",
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
                        