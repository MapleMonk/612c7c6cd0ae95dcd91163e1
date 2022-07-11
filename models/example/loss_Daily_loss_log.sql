{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.loss_Daily_loss_log ADD (eggs_sold numeric); ALTER TABLE eggozdb.maplemonk.loss_Daily_loss_log ADD (revenue numeric); UPDATE TABLE eggozdb.maplemonk.loss_Daily_loss_log dl INNER JOIN eggozdb.maplemonk.summary_reporting_table_beat_retailer ds ON dl.date::date = ds.date::date SET dl.eggs_sold = ds.eggs_sold::numeric; UPDATE TABLE eggozdb.maplemonk.loss_Daily_loss_log dl INNER JOIN eggozdb.maplemonk.summary_reporting_table_beat_retailer ds ON dl.date::date = ds.date::date SET dl.revenue = ds.net_sales::numeric;",
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
                        