{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "select dl.date, eggs_sold, net_sales, dl.Total Loss(White),dl.Total Loss(Brown), dl.Total Loss(Nutra),dl.Total PPP Loss, dl.Loss Amt(White), dl.Loss Amt(Brown),dl.Loss Amt(Nutra+), dl.Total PPP Loss Amt,dl.Total UB Loss, dl.Total Loss from eggozdb.maplemonk.summary_reporting_table_beat_retailer ds, loss_Daily_loss_log dl group by date;",
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
                        