{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.all_loss as select a.date ,ifnull(a.bloodspot_loss,0) bloodspot_loss ,ifnull(a.cleaning_loss,0) cleaning_loss ,ifnull(a.damageqty_loss,0) damageqty_loss ,ifnull(a.shortqty_loss,0) shortqty_loss ,ifnull(a.procurement_loss,0) procurement_loss ,ifnull(b.ub_loss,0) ub_loss ,ifnull(c.pkg_loss,0) pkg_loss ,ifnull(d.net_eggs_sold,0) net_eggs_sold from ( select logdate date , sum(bloodspot::float) bloodspot_loss , sum(\"Cleaning loss\"::float) cleaning_loss , sum(\"Damage Qty(in eggs)\"::float) damageqty_loss , sum(\"Short Qty(in eggs)\"::float) shortqty_loss , sum(\"Procurement Loss\") procurement_loss from eggozdb.maplemonk.epm_sheet1 group by logdate )a left join ( select cdate date, sum(\"Qty(in eggs)\"::float) ub_loss from eggozdb.maplemonk.ub_loss group by cdate ) b on a.date = b.date left join ( select cdate date, sum(Loss::float) pkg_loss from eggozdb.maplemonk.pkg_loss group by cdate ) c on c.date = a.date left join ( select date, sum(ifnull(eggs_Sold,0))-sum(ifnull(eggs_return,0)) net_eggs_sold from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku group by date ) d on d.date = a.date",
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
                        