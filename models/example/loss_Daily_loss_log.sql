{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.loss_daily_loss_log ADD (dDate Date); UPDATE eggozdb.maplemonk.loss_daily_loss_log SET dDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\'); create or replace table eggozdb.maplemonk.loss_daily_loss_log as select dl.*, sr.revenue, sr.eggs_sold from eggozdb.maplemonk.loss_daily_loss_log dl join (select date, sum(net_sales) as revenue, sum(eggs_sold) as eggs_sold from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date) sr on sr.date = dl.ddate;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.loss_Daily_loss_log
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        