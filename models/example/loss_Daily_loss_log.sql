{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "SELECT ds.eggs_sold, ds.net_sales FROM eggozdb.maplemonk.summary_reporting_table_beat_retailer ds GROUP BY ds.date;",
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
                        