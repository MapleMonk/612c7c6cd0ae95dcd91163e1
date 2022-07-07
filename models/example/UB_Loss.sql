{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER TABLE eggozdb.maplemonk.ub_loss ADD (cdate Date); UPDATE eggozdb.maplemonk.ub_loss SET cdate = TRY_TO_DATE(\"Date \",\'dd/mm/yyyy\');",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.MAPLEMONK.UB_Loss
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        