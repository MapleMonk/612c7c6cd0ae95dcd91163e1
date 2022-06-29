{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.demand_supply_sold_procure as select a.*, b.procured from (select date, sum(demand_eggs) as demanded, sum(out_eggs) as supplied, sum(sold_eggs) as sold, sum(replacement_eggs) as replaced from maplemonk.beat_material_kpi group by date) a join (select logdate, sum(\"Total eggs\") as procured from maplemonk.epm_sheet1 group by logdate) b on a.date = b.logdate ;",
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
                        