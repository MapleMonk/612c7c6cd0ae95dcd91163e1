{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.loss_daily_loss_log ADD (ddate date); ALTER TABLE eggozdb.maplemonk.loss_daily_loss_log ADD (eggs_sold numeric); ALTER TABLE eggozdb.maplemonk.loss_daily_loss_log ADD (revenue numeric); UPDATE eggozdb.maplemonk.loss_daily_loss_log dl SET ddate = dl.date; UPDATE dl SET dl.eggs_sold = ds.eggs_sold FROM eggozdb.maplemonk.loss_daily_loss_log dl INNER JOIN eggozdb.maplemonk.summary_reporting_table_beat_retailer ds ON dl.ddate = ds.date; UPDATE dl SET dl.revenue = ds.net_sales FROM eggozdb.maplemonk.loss_daily_loss_log dl INNER JOIN eggozdb.maplemonk.summary_reporting_table_beat_retailer ds ON dl.ddate = ds.date;",
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
                        