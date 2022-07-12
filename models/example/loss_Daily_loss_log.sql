{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.loss_daily_loss_log ADD (dDate Date); UPDATE eggozdb.maplemonk.loss_daily_loss_log SET dDate = TRY_TO_DATE(\"DATE\",\'DD/MM/YYYY\');",
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
                        