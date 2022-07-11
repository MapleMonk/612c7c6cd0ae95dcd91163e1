{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.loss_Daily_loss_log ADD (ddate Date); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET ddate = TRY_TO_DATE(\"Date\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET total_loss = TRY_TO_DATE(\"Total Loss); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET total_ub_loss = TRY_TO_FLOAT(\"Total UB Loss\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET total_ppp_loss = TRY_TO_FLOAT(\"Total PPP Loss\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET brown_loss_amt = TRY_TO_FLOAT(\"Loss Amt (Brown)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET white_loss_amt = TRY_TO_FLOAT(\"Loss Amt (White)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET nutra_loss_amt = TRY_TO_FLOAT(\"Loss Amt (Nutra+)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET brown_total_loss = TRY_TO_FLOAT(\"Total Loss(Brown)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET white_total_loss = TRY_TO_FLOAT(\"Total Loss(White)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET nutra_total_loss = TRY_TO_FLOAT(\"Total Loss(Nutra)\"); UPDATE eggozdb.maplemonk.loss_Daily_loss_log SET total_ppp_loss_amt = TRY_TO_FLOAT(\"Total PPP Loss Amt\");",
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
                        