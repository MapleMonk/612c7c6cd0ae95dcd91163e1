{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.kpi_tracker as select dd.date::date as date, t1.sold, t1.return, t1.procured, t2.processing_ub, t2.processing_hairline_ub, t2.processing_chatki_ub, t2.processing_dirty_ub, t2.processing_small_ub, t2.processing_damage_ub, t2.processing_loss, t2.processing_short_loss, t2.processing_tray_loss, t2.processing_cleaning_loss, t3.packaging_ub, t3.packaging_chatki_ub, t3.packaging_dirty_ub, t3.packaging_loss, t4.eggs_sold, t4.eggs_replaced, t4.eggs_return, t5.ot_amount, t5.housekeeping, t5.unbranded_labour, t5.ub_manpower_per_egg, t5.in_out_labour_cost, t5.branded_labour_cost, t5.housekeeping_labour, t5.logistics_labour_cost, t5.processing_manpower_per_egg from eggozdb.maplemonk.date_dim dd left join ( select date::date as date, sold, return, procured from eggozdb.maplemonk.demand_supply_sold_procure where region IN (\'NCR\') ) t1 on t1.date = dd.date left join ( select pdate::date as pdate, sum(UB) processing_ub, sum(hairline) as processing_hairline_ub, sum(chatki) processing_chatki_ub, sum(dirty) processing_dirty_ub, sum(\"Small size\") processing_small_ub, sum(\"Damage Qty(in eggs)\") processing_damage_ub, sum(loss) processing_loss, sum(\"Short Qty(in eggs)\") processing_short_loss, sum(\"Tray Loss (Farm Loss)\") processing_tray_loss, sum(\"Cleaning loss\") processing_cleaning_loss from eggozdb.maplemonk.epm_sheet1 group by pdate ) t2 on t2.pdate = dd.date left join ( select pdate::date as pdate, sum(\"Packaging UB\") packaging_ub, sum(chatki) packaging_chatki_ub, sum(dirty) packaging_dirty_ub, sum(loss) packaging_loss from eggozdb.maplemonk.epm_pkg_loss group by pdate ) t3 on t3.pdate = dd.date left join ( select date::date as date,sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return from eggozdb.maplemonk.summary_reporting_table_beat_retailer where area in (\'Delhi-GT\',\'Gurgaon-GT\',\'Noida-GT\',\'NCR-ON-MT\',\'NCR-OF-MT\') and date between \'2022-05-01\' and getdate() group by date ) t4 on t4.date = dd.date left join ( select date::date as date, \"OT Amount\" as ot_amount, housekeeping, \"Unbranded Labour\" as unbranded_labour, \"UB Manpower / Egg\" as ub_manpower_per_egg, \"In/Out Labour cost\" as in_out_labour_cost, \"Branded Labour Cost\" as branded_labour_cost, \"Housekeeping Labour\" as housekeeping_labour, \"Logistics Labour Cost\" as logistics_labour_cost, \"Processing Manpower/Egg\" as processing_manpower_per_egg from eggozdb.maplemonk.epm_kpi_tracker_v2 ) t5 on t5.date::date = dd.date where dd.date between \'2022-05-01\' and getdate() ;",
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
                        